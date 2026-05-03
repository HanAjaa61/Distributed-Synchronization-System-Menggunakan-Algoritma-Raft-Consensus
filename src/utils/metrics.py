"""
Metrics collection and monitoring for distributed sync system.
Uses prometheus_client for exposition.
"""
import time
import asyncio
from typing import Dict, Optional
from collections import defaultdict, deque
from dataclasses import dataclass, field
from prometheus_client import (
    Counter, Histogram, Gauge, Summary,
    start_http_server, REGISTRY
)


@dataclass
class MetricSnapshot:
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Centralized metrics collection for all system components."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))

        # Raft / Lock Manager metrics
        self.raft_elections_total = Counter(
            "raft_elections_total",
            "Total number of Raft leader elections",
            ["node_id", "result"]
        )
        self.raft_log_entries = Counter(
            "raft_log_entries_total",
            "Total log entries appended",
            ["node_id"]
        )
        self.raft_commit_latency = Histogram(
            "raft_commit_latency_seconds",
            "Latency for log entry commit",
            ["node_id"],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        self.distributed_locks_acquired = Counter(
            "distributed_locks_acquired_total",
            "Total distributed locks acquired",
            ["node_id", "lock_type"]
        )
        self.distributed_locks_held = Gauge(
            "distributed_locks_held",
            "Current number of held locks",
            ["node_id"]
        )
        self.deadlocks_detected = Counter(
            "deadlocks_detected_total",
            "Total deadlocks detected and resolved",
            ["node_id"]
        )

        # Queue metrics
        self.queue_messages_produced = Counter(
            "queue_messages_produced_total",
            "Total messages produced",
            ["node_id", "partition"]
        )
        self.queue_messages_consumed = Counter(
            "queue_messages_consumed_total",
            "Total messages consumed",
            ["node_id", "partition"]
        )
        self.queue_depth = Gauge(
            "queue_depth",
            "Current queue depth per partition",
            ["node_id", "partition"]
        )
        self.queue_produce_latency = Histogram(
            "queue_produce_latency_seconds",
            "Message production latency",
            ["node_id"],
            buckets=[0.0001, 0.001, 0.01, 0.1, 1.0]
        )
        self.queue_consume_latency = Histogram(
            "queue_consume_latency_seconds",
            "Message consumption latency",
            ["node_id"],
            buckets=[0.0001, 0.001, 0.01, 0.1, 1.0]
        )

        # Cache metrics
        self.cache_hits = Counter(
            "cache_hits_total",
            "Total cache hits",
            ["node_id"]
        )
        self.cache_misses = Counter(
            "cache_misses_total",
            "Total cache misses",
            ["node_id"]
        )
        self.cache_invalidations = Counter(
            "cache_invalidations_total",
            "Total cache invalidations",
            ["node_id", "reason"]
        )
        self.cache_size = Gauge(
            "cache_size_entries",
            "Current cache size in entries",
            ["node_id"]
        )
        self.cache_state_transitions = Counter(
            "cache_state_transitions_total",
            "MESI state transitions",
            ["node_id", "from_state", "to_state"]
        )

        # Network metrics
        self.network_messages_sent = Counter(
            "network_messages_sent_total",
            "Total messages sent",
            ["node_id", "message_type"]
        )
        self.network_messages_received = Counter(
            "network_messages_received_total",
            "Total messages received",
            ["node_id", "message_type"]
        )
        self.network_failures = Counter(
            "network_failures_total",
            "Total network failures",
            ["node_id", "failure_type"]
        )
        self.network_latency = Histogram(
            "network_latency_seconds",
            "Network round-trip latency",
            ["node_id", "target_node"],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
        )

        # Node health
        self.node_uptime = Gauge(
            "node_uptime_seconds",
            "Node uptime in seconds",
            ["node_id"]
        )
        self._start_time = time.time()

    async def start_http_server(self, port: int):
        """Start Prometheus metrics HTTP server."""
        start_http_server(port)

    def record_lock_acquired(self, lock_type: str = "exclusive"):
        self.distributed_locks_acquired.labels(
            node_id=self.node_id, lock_type=lock_type
        ).inc()
        self.distributed_locks_held.labels(node_id=self.node_id).inc()

    def record_lock_released(self):
        self.distributed_locks_held.labels(node_id=self.node_id).dec()

    def record_cache_hit(self):
        self.cache_hits.labels(node_id=self.node_id).inc()

    def record_cache_miss(self):
        self.cache_misses.labels(node_id=self.node_id).inc()

    @property
    def cache_hit_rate(self) -> float:
        hits = self.cache_hits.labels(node_id=self.node_id)._value.get()
        misses = self.cache_misses.labels(node_id=self.node_id)._value.get()
        total = hits + misses
        return hits / total if total > 0 else 0.0

    def get_summary(self) -> Dict:
        """Return a summary of current metrics."""
        uptime = time.time() - self._start_time
        return {
            "node_id": self.node_id,
            "uptime_seconds": round(uptime, 2),
            "cache_hit_rate": round(self.cache_hit_rate, 4),
        }