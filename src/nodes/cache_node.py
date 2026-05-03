"""
Distributed Cache Coherence — MESI Protocol
=============================================
Implements the MESI (Modified-Exclusive-Shared-Invalid) cache coherence protocol
across multiple cache nodes with LRU/LFU replacement policies.

MESI States:
- Modified (M): Cache has the only copy, dirty (differs from main memory)
- Exclusive (E): Cache has the only copy, clean (same as main memory)
- Shared (S): Multiple caches may have this line, all clean
- Invalid (I): Cache line is stale/not present
"""
import asyncio
import logging
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class MESIState(str, Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


@dataclass
class CacheLine:
    key: str
    value: Any
    state: MESIState = MESIState.EXCLUSIVE
    last_access: float = field(default_factory=time.time)
    access_count: int = 1
    dirty: bool = False
    version: int = 1

    def touch(self):
        self.last_access = time.time()
        self.access_count += 1


class LRUCache:
    """LRU eviction using OrderedDict for O(1) operations."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: OrderedDict[str, CacheLine] = OrderedDict()

    def get(self, key: str) -> Optional[CacheLine]:
        if key not in self._cache:
            return None
        self._cache.move_to_end(key)
        line = self._cache[key]
        line.touch()
        return line

    def put(self, key: str, line: CacheLine) -> Optional[str]:
        """Put a cache line. Returns evicted key if capacity exceeded."""
        evicted = None
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self.capacity:
                evicted, _ = self._cache.popitem(last=False)
            self._cache[key] = line
        return evicted

    def invalidate(self, key: str) -> Optional[CacheLine]:
        return self._cache.pop(key, None)

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)

    def keys(self):
        return self._cache.keys()


class LFUCache:
    """LFU eviction using frequency buckets."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: Dict[str, CacheLine] = {}
        self._freq_map: Dict[int, OrderedDict] = {}
        self._min_freq = 0

    def _update_freq(self, key: str, line: CacheLine):
        old_freq = line.access_count - 1
        new_freq = line.access_count
        if old_freq in self._freq_map:
            self._freq_map[old_freq].pop(key, None)
            if not self._freq_map[old_freq] and self._min_freq == old_freq:
                self._min_freq = new_freq
        if new_freq not in self._freq_map:
            self._freq_map[new_freq] = OrderedDict()
        self._freq_map[new_freq][key] = line

    def get(self, key: str) -> Optional[CacheLine]:
        if key not in self._cache:
            return None
        line = self._cache[key]
        self._update_freq(key, line)
        line.touch()
        return line

    def put(self, key: str, line: CacheLine) -> Optional[str]:
        if self.capacity <= 0:
            return None
        evicted = None
        if key in self._cache:
            self._cache[key] = line
            self._update_freq(key, line)
        else:
            if len(self._cache) >= self.capacity:
                # Evict LFU item
                lfu_bucket = self._freq_map.get(self._min_freq, OrderedDict())
                if lfu_bucket:
                    evicted, _ = lfu_bucket.popitem(last=False)
                    del self._cache[evicted]
            self._cache[key] = line
            self._min_freq = 1
            if 1 not in self._freq_map:
                self._freq_map[1] = OrderedDict()
            self._freq_map[1][key] = line
        return evicted

    def invalidate(self, key: str) -> Optional[CacheLine]:
        return self._cache.pop(key, None)

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)

    def keys(self):
        return self._cache.keys()


class CacheNode:
    """
    Distributed cache node implementing the MESI coherence protocol.
    
    Each node maintains a local cache and coordinates with peers
    to maintain coherence. Operations:
    
    READ:
    1. Hit in M/E/S state → serve from local cache
    2. Miss (I state) → fetch from home node or memory
    3. Fetching while another has M state → writeback required first
    
    WRITE:
    1. If in M state → write locally (no coherence action needed)
    2. If in E state → transition to M, write
    3. If in S state → invalidate all other sharers, transition to M
    4. If in I state → fetch, invalidate others, transition to M
    """

    def __init__(self, node_id: str, capacity: int = 10_000,
                 policy: str = "LRU"):
        self.node_id = node_id
        self.capacity = capacity
        self.policy = policy

        if policy.upper() == "LRU":
            self._store = LRUCache(capacity)
        else:
            self._store = LFUCache(capacity)

        # Track which nodes share each key
        self._sharers: Dict[str, Set[str]] = {}

        # Message sender (injected)
        self._send_coherence_msg: Optional[Callable] = None

        # Known peer nodes
        self._peers: List[str] = []

        # Directory-based coherence: which node is home for each key
        # Simple: home = hash(key) % num_nodes (deterministic)
        self._is_home: bool = False  # Set by cluster coordinator

        # Metrics
        self.hits = 0
        self.misses = 0
        self.invalidations_sent = 0
        self.invalidations_received = 0
        self.writebacks = 0
        self.state_transitions: Dict[str, int] = {}

        logger.info(f"[Cache:{self.node_id}] Initialized with {policy} policy, capacity={capacity}")

    def set_peers(self, peers: List[str]):
        self._peers = peers

    def set_message_sender(self, sender: Callable):
        self._send_coherence_msg = sender

    # ─────────────────────────── Read ────────────────────────────────────────

    async def read(self, key: str) -> Optional[Any]:
        """
        Read a value with MESI coherence.
        Returns the value, or None if not found in the cluster.
        """
        line = self._store.get(key)

        if line and line.state != MESIState.INVALID:
            self.hits += 1
            logger.debug(f"[Cache:{self.node_id}] HIT: key={key} state={line.state.value}")
            return line.value

        # Cache miss
        self.misses += 1
        logger.debug(f"[Cache:{self.node_id}] MISS: key={key}")

        # Fetch from peer that has it (or from backing store)
        value, source_node, state = await self._fetch_from_peers(key)
        if value is None:
            return None

        # Determine state for new line
        new_state = MESIState.SHARED if source_node else MESIState.EXCLUSIVE

        new_line = CacheLine(key=key, value=value, state=new_state)
        evicted = self._store.put(key, new_line)
        if evicted:
            await self._handle_eviction(evicted)

        # Track sharers
        if key not in self._sharers:
            self._sharers[key] = set()
        self._sharers[key].add(self.node_id)

        return value

    async def _fetch_from_peers(self, key: str) -> tuple:
        """Ask peers if they have the key. Returns (value, source_node, state)."""
        if not self._send_coherence_msg or not self._peers:
            return None, None, None

        fetch_tasks = [
            asyncio.create_task(
                self._send_coherence_msg(peer, {
                    "type": "cache_fetch",
                    "key": key,
                    "requester": self.node_id,
                })
            )
            for peer in self._peers
        ]

        for task in asyncio.as_completed(fetch_tasks):
            try:
                result = await task
                if result and result.get("found"):
                    # Cancel remaining
                    for t in fetch_tasks:
                        if not t.done():
                            t.cancel()
                    return result["value"], result.get("node_id"), result.get("state")
            except Exception:
                pass

        return None, None, None

    # ─────────────────────────── Write ───────────────────────────────────────

    async def write(self, key: str, value: Any) -> bool:
        """
        Write a value with MESI coherence.
        Invalidates all other copies before writing (write-invalidate protocol).
        """
        line = self._store.get(key)

        if line and line.state == MESIState.MODIFIED:
            # We own it exclusively — write directly
            line.value = value
            line.version += 1
            line.dirty = True
            self._record_transition("M", "M")
            return True

        if line and line.state == MESIState.EXCLUSIVE:
            # We're the only reader — transition to Modified
            line.value = value
            line.state = MESIState.MODIFIED
            line.version += 1
            line.dirty = True
            self._record_transition("E", "M")
            return True

        # Shared or Invalid — must invalidate all other copies first
        await self._invalidate_others(key)

        new_line = CacheLine(
            key=key,
            value=value,
            state=MESIState.MODIFIED,
            dirty=True,
        )
        if line:
            self._record_transition(line.state.value, "M")
        else:
            self._record_transition("I", "M")

        evicted = self._store.put(key, new_line)
        if evicted:
            await self._handle_eviction(evicted)

        self._sharers[key] = {self.node_id}
        return True

    async def _invalidate_others(self, key: str):
        """Send invalidation to all nodes that may share this key."""
        if not self._send_coherence_msg:
            return

        sharers = self._sharers.get(key, set()) - {self.node_id}
        if not sharers:
            sharers = set(self._peers)  # Broadcast if unknown

        inv_tasks = [
            asyncio.create_task(
                self._send_coherence_msg(peer, {
                    "type": "cache_invalidate",
                    "key": key,
                    "sender": self.node_id,
                })
            )
            for peer in sharers
        ]

        if inv_tasks:
            await asyncio.gather(*inv_tasks, return_exceptions=True)
            self.invalidations_sent += len(inv_tasks)
            logger.debug(
                f"[Cache:{self.node_id}] Invalidated {len(inv_tasks)} copies of key={key}"
            )

    # ─────────────────────────── Coherence Handlers ──────────────────────────

    async def handle_invalidation(self, key: str, sender: str):
        """Handle an invalidation request from another node."""
        line = self._store.get(key)
        if line:
            if line.state == MESIState.MODIFIED:
                # Must writeback before invalidating
                await self._writeback(key, line)
            # Transition to Invalid
            self._record_transition(line.state.value, "I")
            line.state = MESIState.INVALID
            self._store.invalidate(key)
            self.invalidations_received += 1
            logger.debug(f"[Cache:{self.node_id}] Invalidated key={key} from {sender}")

    async def handle_fetch_request(self, key: str, requester: str) -> Dict:
        """Handle a fetch request from another node."""
        line = self._store.get(key)
        if not line or line.state == MESIState.INVALID:
            return {"found": False}

        # If we have it Modified, we must supply it (and may need to downgrade)
        if line.state == MESIState.MODIFIED:
            # Writeback and transition to Shared
            await self._writeback(key, line)
            line.state = MESIState.SHARED
            self._record_transition("M", "S")

        elif line.state == MESIState.EXCLUSIVE:
            # Downgrade to Shared since another node now has it too
            line.state = MESIState.SHARED
            self._record_transition("E", "S")

        # Update sharer tracking
        if key not in self._sharers:
            self._sharers[key] = set()
        self._sharers[key].add(requester)

        return {
            "found": True,
            "value": line.value,
            "state": line.state.value,
            "node_id": self.node_id,
        }

    async def handle_update(self, key: str, value: Any, version: int):
        """
        Handle an update broadcast (for write-update protocol variant).
        Updates shared copies instead of invalidating them.
        """
        line = self._store.get(key)
        if line and line.state != MESIState.INVALID:
            line.value = value
            line.version = version
            line.state = MESIState.SHARED

    # ─────────────────────────── Eviction ────────────────────────────────────

    async def _handle_eviction(self, key: str):
        """Handle eviction of a cache line (may need writeback)."""
        # In a full implementation, if the line is Modified, writeback to backing store
        self._sharers.pop(key, None)

    async def _writeback(self, key: str, line: CacheLine):
        """Writeback a modified line to the backing store."""
        if not line.dirty:
            return
        # In production: write to Redis/persistent store
        line.dirty = False
        self.writebacks += 1
        logger.debug(f"[Cache:{self.node_id}] Writeback: key={key} version={line.version}")

    # ─────────────────────────── Metrics ─────────────────────────────────────

    def _record_transition(self, from_state: str, to_state: str):
        key = f"{from_state}->{to_state}"
        self.state_transitions[key] = self.state_transitions.get(key, 0) + 1

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "policy": self.policy,
            "size": len(self._store),
            "capacity": self.capacity,
            "hit_rate": round(self.hit_rate, 4),
            "hits": self.hits,
            "misses": self.misses,
            "invalidations_sent": self.invalidations_sent,
            "invalidations_received": self.invalidations_received,
            "writebacks": self.writebacks,
            "state_transitions": self.state_transitions,
        }