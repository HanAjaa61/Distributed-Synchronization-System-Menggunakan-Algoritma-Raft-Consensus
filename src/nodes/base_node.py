"""
Base Node — HTTP API server for the distributed system.
Each node exposes REST endpoints for cluster communication and client interaction.
"""
import asyncio
import json
import logging
import os
from typing import Optional

from aiohttp import web

from ..consensus.raft import RaftNode, NodeState
from ..nodes.lock_manager import DistributedLockManager, LockType
from ..nodes.queue_node import DistributedQueue
from ..nodes.cache_node import CacheNode
from ..communication.message_passing import MessageBus, Message, MessageType
from ..communication.failure_detector import FailureDetector
from ..utils.config import SystemConfig, config
from ..utils.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class DistributedNode:
    """
    Full distributed node that combines Lock Manager, Queue, and Cache
    components into a single HTTP-accessible service.
    
    Cluster communication happens over HTTP/JSON.
    Client-facing API also exposed over HTTP with REST semantics.
    """

    def __init__(self, node_cfg: Optional[SystemConfig] = None):
        self.cfg = node_cfg or config
        self.node_id = self.cfg.node.node_id

        # Determine peers (exclude self)
        self.peers = [
            n for n in self.cfg.node.cluster_nodes
            if n != f"{self.cfg.node.host}:{self.cfg.node.port}"
        ]

        # Core components
        self.raft = RaftNode(
            node_id=self.node_id,
            peers=self.peers,
            election_timeout_min=self.cfg.raft.election_timeout_min / 1000,
            election_timeout_max=self.cfg.raft.election_timeout_max / 1000,
            heartbeat_interval=self.cfg.raft.heartbeat_interval / 1000,
        )

        self.lock_manager = DistributedLockManager(self.node_id, self.raft)

        self.queue = DistributedQueue(
            node_id=self.node_id,
            redis_url=self.cfg.redis.url,
            num_partitions=self.cfg.queue.partitions,
        )

        self.cache = CacheNode(
            node_id=self.node_id,
            capacity=self.cfg.cache.max_size,
            policy=self.cfg.cache.replacement_policy,
        )
        self.cache.set_peers(self.peers)

        self.failure_detector = FailureDetector(
            node_id=self.node_id,
            heartbeat_interval=1.0,
        )

        self.metrics = MetricsCollector(self.node_id)

        # HTTP app
        self.app = web.Application()
        self._setup_routes()

    def _setup_routes(self):
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/status", self.handle_status)

        # Raft internal endpoints
        self.app.router.add_post("/raft/vote", self.handle_vote_request)
        self.app.router.add_post("/raft/append", self.handle_append_entries)

        # Lock Manager
        self.app.router.add_post("/locks/acquire", self.handle_lock_acquire)
        self.app.router.add_post("/locks/release", self.handle_lock_release)
        self.app.router.add_get("/locks/status", self.handle_lock_status)

        # Queue
        self.app.router.add_post("/queue/produce", self.handle_queue_produce)
        self.app.router.add_post("/queue/consume", self.handle_queue_consume)
        self.app.router.add_post("/queue/ack", self.handle_queue_ack)
        self.app.router.add_post("/queue/nack", self.handle_queue_nack)

        # Cache
        self.app.router.add_get("/cache/{key}", self.handle_cache_read)
        self.app.router.add_put("/cache/{key}", self.handle_cache_write)
        self.app.router.add_delete("/cache/{key}", self.handle_cache_invalidate)
        self.app.router.add_post("/cache/coherence", self.handle_coherence_message)

        # Heartbeat
        self.app.router.add_post("/heartbeat", self.handle_heartbeat)

    async def start(self):
        # Start subsystems
        await self.queue.start()
        await self.lock_manager.start()

        # Inject cache message sender
        self.cache.set_message_sender(self._send_coherence)

        # Set Raft message sender
        self.raft.set_message_sender(self._send_raft_message)

        # Register failure handlers
        self.failure_detector.on_failure(self._on_node_failure)
        for peer in self.peers:
            self.failure_detector.add_node(peer)

        # Start failure detector
        asyncio.create_task(self.failure_detector.run_check_loop())

        # Start heartbeat sender
        asyncio.create_task(self._heartbeat_loop())

        logger.info(f"[Node:{self.node_id}] All subsystems started")

    async def stop(self):
        await self.lock_manager.stop()
        await self.queue.stop()

    # ─────────────────────────── HTTP Handlers ───────────────────────────────

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "node_id": self.node_id})

    async def handle_status(self, request: web.Request) -> web.Response:
        return web.json_response({
            "node": self.node_id,
            "raft": self.raft.get_status(),
            "locks": self.lock_manager.get_status(),
            "queue": self.queue.get_status(),
            "cache": self.cache.get_status(),
            "metrics": self.metrics.get_summary(),
        })

    async def handle_vote_request(self, request: web.Request) -> web.Response:
        payload = await request.json()
        result = await self.raft.handle_vote_request(payload)
        return web.json_response(result)

    async def handle_append_entries(self, request: web.Request) -> web.Response:
        payload = await request.json()
        result = await self.raft.handle_append_entries(payload)
        return web.json_response(result)

    async def handle_lock_acquire(self, request: web.Request) -> web.Response:
        data = await request.json()
        client_id = data.get("client_id", "anonymous")
        resource_id = data["resource_id"]
        lock_type = LockType(data.get("lock_type", "exclusive"))
        timeout = float(data.get("timeout", 30.0))
        wait_timeout = float(data.get("wait_timeout", 10.0))

        status = await self.lock_manager.acquire(
            client_id, resource_id, lock_type, timeout, wait_timeout
        )
        self.metrics.record_lock_acquired(lock_type.value)
        return web.json_response({"status": status.value, "resource_id": resource_id})

    async def handle_lock_release(self, request: web.Request) -> web.Response:
        data = await request.json()
        success = await self.lock_manager.release(data["client_id"], data["resource_id"])
        if success:
            self.metrics.record_lock_released()
        return web.json_response({"success": success})

    async def handle_lock_status(self, request: web.Request) -> web.Response:
        return web.json_response(self.lock_manager.get_status())

    async def handle_queue_produce(self, request: web.Request) -> web.Response:
        data = await request.json()
        message_id = await self.queue.produce(
            topic=data["topic"],
            payload=data["payload"],
            key=data.get("key"),
            headers=data.get("headers"),
        )
        return web.json_response({"message_id": message_id})

    async def handle_queue_consume(self, request: web.Request) -> web.Response:
        data = await request.json()
        messages = await self.queue.consume(
            topic=data["topic"],
            group_id=data["group_id"],
            consumer_id=data.get("consumer_id", self.node_id),
            batch_size=data.get("batch_size", 1),
        )
        return web.json_response({
            "messages": [m.to_dict() for m in messages]
        })

    async def handle_queue_ack(self, request: web.Request) -> web.Response:
        data = await request.json()
        success = await self.queue.acknowledge(data["message_id"])
        return web.json_response({"success": success})

    async def handle_queue_nack(self, request: web.Request) -> web.Response:
        data = await request.json()
        success = await self.queue.negative_acknowledge(data["message_id"])
        return web.json_response({"success": success})

    async def handle_cache_read(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        value = await self.cache.read(key)
        if value is None:
            return web.json_response({"found": False}, status=404)
        return web.json_response({"found": True, "value": value})

    async def handle_cache_write(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        data = await request.json()
        await self.cache.write(key, data["value"])
        return web.json_response({"success": True})

    async def handle_cache_invalidate(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        await self.cache._invalidate_others(key)
        self.cache._store.invalidate(key)
        return web.json_response({"success": True})

    async def handle_coherence_message(self, request: web.Request) -> web.Response:
        data = await request.json()
        msg_type = data.get("type")

        if msg_type == "cache_invalidate":
            await self.cache.handle_invalidation(data["key"], data["sender"])
            return web.json_response({"ok": True})
        elif msg_type == "cache_fetch":
            result = await self.cache.handle_fetch_request(data["key"], data["requester"])
            return web.json_response(result)
        elif msg_type == "cache_update":
            await self.cache.handle_update(data["key"], data["value"], data.get("version", 1))
            return web.json_response({"ok": True})

        return web.json_response({"error": "unknown coherence message"}, status=400)

    async def handle_heartbeat(self, request: web.Request) -> web.Response:
        data = await request.json()
        sender = data.get("sender_id")
        if sender:
            self.failure_detector.record_heartbeat(sender)
        return web.json_response({"node_id": self.node_id})

    # ─────────────────────────── Internal ────────────────────────────────────

    async def _send_raft_message(self, target: str, msg_type: str, payload: dict) -> Optional[dict]:
        """Send a Raft protocol message to a peer node."""
        import aiohttp
        url_map = {
            "vote_request": f"http://{target}/raft/vote",
            "append_entries": f"http://{target}/raft/append",
        }
        url = url_map.get(msg_type)
        if not url:
            return None

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    return await resp.json()
        except Exception:
            return None

    async def _send_coherence(self, target: str, payload: dict) -> Optional[dict]:
        """Send a cache coherence message to a peer node."""
        import aiohttp
        url = f"http://{target}/cache/coherence"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    return await resp.json()
        except Exception:
            return None

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to all peers."""
        import aiohttp
        while True:
            await asyncio.sleep(1.0)
            for peer in self.peers:
                try:
                    async with aiohttp.ClientSession() as session:
                        await session.post(
                            f"http://{peer}/heartbeat",
                            json={"sender_id": self.node_id},
                            timeout=aiohttp.ClientTimeout(total=1),
                        )
                except Exception:
                    pass

    async def _on_node_failure(self, failed_node: str):
        """Handle detected node failure."""
        logger.warning(f"[Node:{self.node_id}] Node failure detected: {failed_node}")
        # Release all locks held by clients on the failed node
        # In production: trigger rebalancing, leader election, etc.


async def main():
    """Entry point for running a single node."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )

    node = DistributedNode()
    await node.start()

    runner = web.AppRunner(node.app)
    await runner.setup()
    site = web.TCPSite(runner, config.node.host, config.node.port)
    await site.start()

    logger.info(f"Node {config.node.node_id} running on {config.node.host}:{config.node.port}")

    try:
        await asyncio.sleep(float("inf"))
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Node shutting down...")
        await node.stop()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())