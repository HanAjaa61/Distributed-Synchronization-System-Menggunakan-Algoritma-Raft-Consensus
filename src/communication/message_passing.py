"""
Async message passing layer for inter-node communication.
Handles serialization, retry logic, and connection pooling.
"""
import asyncio
import json
import time
import uuid
import logging
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field, asdict

import aiohttp

logger = logging.getLogger(__name__)


class MessageType(str, Enum):
    # Raft messages
    VOTE_REQUEST = "vote_request"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"
    INSTALL_SNAPSHOT = "install_snapshot"

    # Lock messages
    LOCK_REQUEST = "lock_request"
    LOCK_GRANT = "lock_grant"
    LOCK_DENY = "lock_deny"
    LOCK_RELEASE = "lock_release"
    DEADLOCK_PROBE = "deadlock_probe"

    # Queue messages
    QUEUE_PRODUCE = "queue_produce"
    QUEUE_ACK = "queue_ack"
    QUEUE_FETCH = "queue_fetch"
    QUEUE_REBALANCE = "queue_rebalance"

    # Cache messages
    CACHE_INVALIDATE = "cache_invalidate"
    CACHE_UPDATE = "cache_update"
    CACHE_FETCH = "cache_fetch"
    CACHE_FETCH_RESPONSE = "cache_fetch_response"
    CACHE_UPGRADE = "cache_upgrade"

    # Health
    HEARTBEAT = "heartbeat"
    HEARTBEAT_ACK = "heartbeat_ack"


@dataclass
class Message:
    type: MessageType
    sender_id: str
    payload: Dict[str, Any]
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    term: int = 0  # Used by Raft

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["type"] = self.type.value
        return d

    @classmethod
    def from_dict(cls, d: Dict) -> "Message":
        d = dict(d)
        d["type"] = MessageType(d["type"])
        return cls(**d)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, s: str) -> "Message":
        return cls.from_dict(json.loads(s))


MessageHandler = Callable[[Message], Any]


class MessageBus:
    """
    Async HTTP-based message bus for cluster communication.
    Handles connection pooling, retries, and failure detection.
    """

    def __init__(self, node_id: str, host: str, port: int,
                 timeout: float = 5.0, max_retries: int = 3):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_retries = max_retries

        self._handlers: Dict[MessageType, List[MessageHandler]] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False

    async def start(self):
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=20)
        )
        self._running = True
        logger.info(f"[{self.node_id}] MessageBus started on {self.host}:{self.port}")

    async def stop(self):
        self._running = False
        if self._session:
            await self._session.close()

    def register_handler(self, msg_type: MessageType, handler: MessageHandler):
        """Register a handler for a specific message type."""
        if msg_type not in self._handlers:
            self._handlers[msg_type] = []
        self._handlers[msg_type].append(handler)

    async def handle_incoming(self, message: Message) -> Optional[Any]:
        """Dispatch an incoming message to registered handlers."""
        handlers = self._handlers.get(message.type, [])
        results = []
        for handler in handlers:
            try:
                result = await asyncio.ensure_future(
                    asyncio.coroutine(handler)(message)
                    if not asyncio.iscoroutinefunction(handler)
                    else handler(message)
                )
                results.append(result)
            except Exception as e:
                logger.error(f"Handler error for {message.type}: {e}")
        return results[0] if len(results) == 1 else results or None

    async def send(self, target: str, message: Message,
                   expect_response: bool = False) -> Optional[Dict]:
        """
        Send a message to a target node (host:port format).
        Retries up to max_retries times on failure.
        """
        if not self._session:
            raise RuntimeError("MessageBus not started")

        url = f"http://{target}/messages"
        payload = message.to_dict()

        for attempt in range(self.max_retries):
            try:
                async with self._session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        if expect_response:
                            return await resp.json()
                        return {"status": "ok"}
                    else:
                        logger.warning(
                            f"[{self.node_id}] Message to {target} returned {resp.status}"
                        )
            except asyncio.TimeoutError:
                logger.warning(
                    f"[{self.node_id}] Timeout sending to {target} (attempt {attempt+1})"
                )
            except aiohttp.ClientConnectorError:
                logger.warning(
                    f"[{self.node_id}] Cannot connect to {target} (attempt {attempt+1})"
                )
            except Exception as e:
                logger.error(f"[{self.node_id}] Send error to {target}: {e}")

            if attempt < self.max_retries - 1:
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff

        return None

    async def broadcast(self, targets: List[str], message: Message) -> Dict[str, Optional[Dict]]:
        """Broadcast a message to multiple targets concurrently."""
        tasks = {
            target: asyncio.create_task(self.send(target, message))
            for target in targets
            if target != f"{self.host}:{self.port}"
        }
        results = {}
        for target, task in tasks.items():
            try:
                results[target] = await task
            except Exception as e:
                logger.error(f"Broadcast to {target} failed: {e}")
                results[target] = None
        return results