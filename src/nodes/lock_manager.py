"""
Distributed Lock Manager using Raft Consensus
===============================================
Implements shared and exclusive distributed locks with:
- Deadlock detection using cycle detection in wait-for graph
- Lock timeout and automatic release
- Raft-backed persistence for crash safety
- Network partition handling
"""
import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from ..consensus.raft import RaftNode, NodeState

logger = logging.getLogger(__name__)


class LockType(str, Enum):
    SHARED = "shared"       # Multiple readers allowed
    EXCLUSIVE = "exclusive"  # Single writer only


class LockStatus(str, Enum):
    GRANTED = "granted"
    WAITING = "waiting"
    DENIED = "denied"
    RELEASED = "released"
    EXPIRED = "expired"


@dataclass
class LockRequest:
    request_id: str
    client_id: str
    resource_id: str
    lock_type: LockType
    timestamp: float = field(default_factory=time.time)
    timeout: float = 30.0  # Seconds before auto-release
    priority: int = 0


@dataclass
class Lock:
    lock_id: str
    resource_id: str
    lock_type: LockType
    holder_ids: Set[str] = field(default_factory=set)  # Multiple for shared
    waiters: List[LockRequest] = field(default_factory=list)
    acquired_at: float = field(default_factory=time.time)
    timeout: float = 30.0

    def is_expired(self) -> bool:
        return time.time() - self.acquired_at > self.timeout

    def can_acquire_shared(self) -> bool:
        return self.lock_type == LockType.SHARED or not self.holder_ids

    def can_acquire_exclusive(self) -> bool:
        return not self.holder_ids


class WaitForGraph:
    """
    Directed graph for deadlock detection.
    Edge A -> B means "A is waiting for B to release a lock."
    A cycle in this graph indicates a deadlock.
    """

    def __init__(self):
        self._graph: Dict[str, Set[str]] = defaultdict(set)

    def add_wait(self, waiter_id: str, holder_id: str):
        """Record that waiter_id is waiting for holder_id."""
        self._graph[waiter_id].add(holder_id)

    def remove_wait(self, waiter_id: str, holder_id: Optional[str] = None):
        """Remove wait edges for a client."""
        if holder_id:
            self._graph[waiter_id].discard(holder_id)
        else:
            self._graph.pop(waiter_id, None)

    def remove_node(self, node_id: str):
        """Remove all edges for a node (when released)."""
        self._graph.pop(node_id, None)
        for edges in self._graph.values():
            edges.discard(node_id)

    def detect_cycle(self) -> Optional[List[str]]:
        """
        Detect a cycle using DFS.
        Returns the cycle as a list of node IDs, or None if no deadlock.
        """
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in self._graph.get(node, set()):
                if neighbor not in visited:
                    result = dfs(neighbor)
                    if result:
                        return result
                elif neighbor in rec_stack:
                    # Found cycle — extract it
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:]

            path.pop()
            rec_stack.discard(node)
            return None

        for node in list(self._graph.keys()):
            if node not in visited:
                result = dfs(node)
                if result:
                    return result
        return None

    def get_victim(self, cycle: List[str]) -> str:
        """
        Choose a victim to abort from a deadlock cycle.
        Simple strategy: pick the most recently added (youngest transaction).
        """
        return cycle[-1]


class DistributedLockManager:
    """
    Distributed Lock Manager backed by Raft consensus.
    
    All lock operations are proposed through Raft to ensure:
    1. Strong consistency across all nodes
    2. Crash recovery (lock state is in Raft log)
    3. No split-brain scenarios
    """

    def __init__(self, node_id: str, raft_node: RaftNode):
        self.node_id = node_id
        self.raft = raft_node

        # Local lock table (rebuilt from Raft log on recovery)
        self._locks: Dict[str, Lock] = {}  # resource_id -> Lock
        self._client_locks: Dict[str, Set[str]] = defaultdict(set)  # client_id -> resource_ids

        # Deadlock detection
        self._wfg = WaitForGraph()
        self._deadlock_check_interval = 0.5  # seconds
        self._deadlock_task: Optional[asyncio.Task] = None

        # Pending requests waiting for lock
        self._pending_futures: Dict[str, asyncio.Future] = {}

        # Metrics
        self.locks_acquired = 0
        self.locks_released = 0
        self.deadlocks_detected = 0
        self.deadlocks_resolved = 0

        # Register Raft state machine
        self.raft.set_state_machine(self._apply_command)

        logger.info(f"[LockManager:{self.node_id}] Initialized")

    async def start(self):
        await self.raft.start()
        self._deadlock_task = asyncio.create_task(self._deadlock_detector())
        asyncio.create_task(self._timeout_checker())
        logger.info(f"[LockManager:{self.node_id}] Started")

    async def stop(self):
        await self.raft.stop()
        if self._deadlock_task:
            self._deadlock_task.cancel()

    # ─────────────────────────── Lock Operations ─────────────────────────────

    async def acquire(self, client_id: str, resource_id: str,
                      lock_type: LockType = LockType.EXCLUSIVE,
                      timeout: float = 30.0,
                      wait_timeout: float = 10.0) -> LockStatus:
        """
        Acquire a lock on a resource.
        If the lock is held by another, waits up to wait_timeout seconds.
        """
        if self.raft.state != NodeState.LEADER:
            logger.warning(f"[LockManager] Cannot acquire on non-leader node")
            return LockStatus.DENIED

        request_id = str(uuid.uuid4())
        request = LockRequest(
            request_id=request_id,
            client_id=client_id,
            resource_id=resource_id,
            lock_type=lock_type,
            timeout=timeout,
        )

        # Propose the lock request via Raft
        command = {
            "type": "lock_acquire",
            "request": {
                "request_id": request_id,
                "client_id": client_id,
                "resource_id": resource_id,
                "lock_type": lock_type.value,
                "timeout": timeout,
                "timestamp": request.timestamp,
            }
        }

        # Create future for when lock is granted
        future = asyncio.get_event_loop().create_future()
        self._pending_futures[request_id] = future

        committed = await self.raft.propose(command, timeout=5.0)
        if not committed:
            self._pending_futures.pop(request_id, None)
            return LockStatus.DENIED

        # Wait for the lock to be granted or denied
        try:
            status = await asyncio.wait_for(future, timeout=wait_timeout)
            return status
        except asyncio.TimeoutError:
            self._pending_futures.pop(request_id, None)
            # Propose cancellation
            await self.raft.propose({
                "type": "lock_cancel",
                "request_id": request_id,
                "client_id": client_id,
                "resource_id": resource_id,
            })
            return LockStatus.DENIED

    async def release(self, client_id: str, resource_id: str) -> bool:
        """Release a lock held by client_id on resource_id."""
        if self.raft.state != NodeState.LEADER:
            return False

        command = {
            "type": "lock_release",
            "client_id": client_id,
            "resource_id": resource_id,
        }
        return await self.raft.propose(command)

    async def release_all(self, client_id: str):
        """Release all locks held by a client (used on client disconnect)."""
        resources = list(self._client_locks.get(client_id, set()))
        for resource_id in resources:
            await self.release(client_id, resource_id)

    # ─────────────────────────── State Machine ────────────────────────────────

    async def _apply_command(self, entry) -> None:
        """Apply a committed Raft log entry to the lock state machine."""
        cmd = entry.command
        cmd_type = cmd.get("type")

        if cmd_type == "lock_acquire":
            await self._apply_acquire(cmd["request"])
        elif cmd_type == "lock_release":
            await self._apply_release(cmd["client_id"], cmd["resource_id"])
        elif cmd_type == "lock_cancel":
            self._apply_cancel(cmd["request_id"], cmd["resource_id"])
        elif cmd_type == "lock_abort":
            await self._apply_release(cmd["client_id"], cmd["resource_id"])
            logger.warning(
                f"[LockManager] Deadlock victim aborted: client={cmd['client_id']}"
            )
        elif cmd_type == "no_op":
            pass

    async def _apply_acquire(self, req: Dict):
        resource_id = req["resource_id"]
        client_id = req["client_id"]
        request_id = req["request_id"]
        lock_type = LockType(req["lock_type"])
        timeout = req.get("timeout", 30.0)

        if resource_id not in self._locks:
            self._locks[resource_id] = Lock(
                lock_id=str(uuid.uuid4()),
                resource_id=resource_id,
                lock_type=lock_type,
                timeout=timeout,
            )

        lock = self._locks[resource_id]
        can_acquire = (
            lock.can_acquire_shared() if lock_type == LockType.SHARED
            else lock.can_acquire_exclusive()
        )

        if can_acquire:
            lock.holder_ids.add(client_id)
            lock.lock_type = lock_type
            lock.acquired_at = time.time()
            self._client_locks[client_id].add(resource_id)
            self.locks_acquired += 1

            # Update wait-for graph
            self._wfg.remove_wait(client_id)

            # Resolve future
            if request_id in self._pending_futures:
                future = self._pending_futures.pop(request_id)
                if not future.done():
                    future.set_result(LockStatus.GRANTED)

            logger.info(
                f"[LockManager] {lock_type.value} lock GRANTED: "
                f"client={client_id} resource={resource_id}"
            )
        else:
            # Add to wait queue and update WFG
            lock_req = LockRequest(
                request_id=request_id,
                client_id=client_id,
                resource_id=resource_id,
                lock_type=lock_type,
                timeout=timeout,
            )
            lock.waiters.append(lock_req)

            # Add wait edges to graph
            for holder_id in lock.holder_ids:
                self._wfg.add_wait(client_id, holder_id)

            logger.info(
                f"[LockManager] Lock QUEUED: client={client_id} "
                f"resource={resource_id} waiters={len(lock.waiters)}"
            )

    async def _apply_release(self, client_id: str, resource_id: str):
        if resource_id not in self._locks:
            return

        lock = self._locks[resource_id]
        lock.holder_ids.discard(client_id)
        self._client_locks[client_id].discard(resource_id)
        self._wfg.remove_node(client_id)
        self.locks_released += 1

        logger.info(f"[LockManager] Lock RELEASED: client={client_id} resource={resource_id}")

        # If no more holders, try to grant to next waiter(s)
        if not lock.holder_ids and lock.waiters:
            await self._grant_next(lock)
        elif not lock.holder_ids:
            del self._locks[resource_id]

    def _apply_cancel(self, request_id: str, resource_id: str):
        if resource_id in self._locks:
            lock = self._locks[resource_id]
            lock.waiters = [w for w in lock.waiters if w.request_id != request_id]

        if request_id in self._pending_futures:
            future = self._pending_futures.pop(request_id)
            if not future.done():
                future.set_result(LockStatus.DENIED)

    async def _grant_next(self, lock: Lock):
        """Grant lock to the next eligible waiter(s)."""
        if not lock.waiters:
            return

        next_req = lock.waiters[0]

        if next_req.lock_type == LockType.EXCLUSIVE:
            lock.waiters.pop(0)
            await self._apply_acquire({
                "request_id": next_req.request_id,
                "client_id": next_req.client_id,
                "resource_id": next_req.resource_id,
                "lock_type": next_req.lock_type.value,
                "timeout": next_req.timeout,
            })
        else:
            # Grant all shared readers at the front
            i = 0
            while i < len(lock.waiters) and lock.waiters[i].lock_type == LockType.SHARED:
                req = lock.waiters.pop(0)
                await self._apply_acquire({
                    "request_id": req.request_id,
                    "client_id": req.client_id,
                    "resource_id": req.resource_id,
                    "lock_type": req.lock_type.value,
                    "timeout": req.timeout,
                })

    # ─────────────────────────── Deadlock Detection ──────────────────────────

    async def _deadlock_detector(self):
        """Periodically check the wait-for graph for cycles (deadlocks)."""
        while True:
            await asyncio.sleep(self._deadlock_check_interval)
            try:
                cycle = self._wfg.detect_cycle()
                if cycle:
                    self.deadlocks_detected += 1
                    victim_id = self._wfg.get_victim(cycle)
                    logger.warning(
                        f"[LockManager] DEADLOCK detected! Cycle: {' -> '.join(cycle)}. "
                        f"Aborting victim: {victim_id}"
                    )
                    await self._abort_client(victim_id, cycle)
                    self.deadlocks_resolved += 1
            except Exception as e:
                logger.error(f"[LockManager] Deadlock detector error: {e}")

    async def _abort_client(self, client_id: str, cycle: List[str]):
        """Abort a deadlocked client's locks."""
        resources = list(self._client_locks.get(client_id, set()))
        for resource_id in resources:
            if self.raft.state == NodeState.LEADER:
                await self.raft.propose({
                    "type": "lock_abort",
                    "client_id": client_id,
                    "resource_id": resource_id,
                    "reason": "deadlock",
                    "cycle": cycle,
                })

    # ─────────────────────────── Timeout Checker ─────────────────────────────

    async def _timeout_checker(self):
        """Periodically release expired locks."""
        while True:
            await asyncio.sleep(5.0)
            expired = []
            for resource_id, lock in list(self._locks.items()):
                if lock.is_expired() and lock.holder_ids:
                    for client_id in list(lock.holder_ids):
                        expired.append((client_id, resource_id))

            for client_id, resource_id in expired:
                logger.warning(
                    f"[LockManager] Lock EXPIRED: client={client_id} resource={resource_id}"
                )
                if self.raft.state == NodeState.LEADER:
                    await self.release(client_id, resource_id)

    # ─────────────────────────── Status ─────────────────────────────────────

    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "raft": self.raft.get_status(),
            "active_locks": len(self._locks),
            "locks_acquired": self.locks_acquired,
            "locks_released": self.locks_released,
            "deadlocks_detected": self.deadlocks_detected,
            "deadlocks_resolved": self.deadlocks_resolved,
            "resources": [
                {
                    "resource_id": rid,
                    "lock_type": lock.lock_type.value,
                    "holders": list(lock.holder_ids),
                    "waiters": len(lock.waiters),
                }
                for rid, lock in self._locks.items()
            ]
        }