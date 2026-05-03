"""
Phi Accrual Failure Detector for distributed nodes.
Provides adaptive suspicion levels based on heartbeat history.
"""
import asyncio
import math
import time
import logging
from collections import deque
from typing import Callable, Dict, Optional, Set

logger = logging.getLogger(__name__)


class PhiAccrualDetector:
    """
    Implementation of the Phi Accrual Failure Detector.
    
    Calculates a suspicion level (phi) based on heartbeat arrival history.
    Higher phi = higher confidence that the node has failed.
    Threshold of 8.0 is commonly used in practice (Cassandra uses 8).
    """

    def __init__(self, threshold: float = 8.0,
                window_size: int = 1000,
                min_std_deviation_ms: float = 500.0,
                acceptable_heartbeat_pause_ms: float = 0.0,
                first_heartbeat_estimate_ms: float = 1000.0):
        self.threshold = threshold
        self.window_size = window_size
        self.min_std_deviation_ms = min_std_deviation_ms
        self.acceptable_heartbeat_pause_ms = acceptable_heartbeat_pause_ms
        self.first_heartbeat_estimate_ms = first_heartbeat_estimate_ms

        self._intervals: deque = deque(maxlen=window_size)
        self._last_heartbeat: Optional[float] = None

    def heartbeat(self):
        """Record a heartbeat arrival."""
        now = time.time() * 1000  # ms
        if self._last_heartbeat is not None:
            interval = now - self._last_heartbeat
            self._intervals.append(interval)
        else:
            # Bootstrap with estimate
            self._intervals.append(self.first_heartbeat_estimate_ms)
        self._last_heartbeat = now

    def phi(self) -> float:
        """
        Compute the current phi (suspicion) value.
        Returns 0.0 if no heartbeats recorded yet.
        """
        if not self._intervals or self._last_heartbeat is None:
            return 0.0

        now_ms = time.time() * 1000
        elapsed = now_ms - self._last_heartbeat

        mean = sum(self._intervals) / len(self._intervals)
        variance = sum((x - mean) ** 2 for x in self._intervals) / len(self._intervals)
        std_dev = max(math.sqrt(variance), self.min_std_deviation_ms)

        # CDF of normal distribution approximation (using log for numerical stability)
        diff = elapsed - mean - self.acceptable_heartbeat_pause_ms
        exponent = -diff / std_dev
        p_later = 1.0 - (1.0 / (1.0 + math.exp(-1.5976 * exponent + 0.070566 * exponent ** 3)))

        if p_later < 1e-300:
            return 999.0
        return -math.log10(p_later)

    def is_available(self) -> bool:
        """Return True if the node is believed to be alive."""
        return self.phi() < self.threshold


class FailureDetector:
    """
    Manages failure detection for all peer nodes using phi accrual.
    Runs periodic heartbeats and notifies callbacks on state changes.
    """

    def __init__(self, node_id: str, heartbeat_interval: float = 1.0,
                 phi_threshold: float = 8.0):
        self.node_id = node_id
        self.heartbeat_interval = heartbeat_interval
        self.phi_threshold = phi_threshold

        self._detectors: Dict[str, PhiAccrualDetector] = {}
        self._known_alive: Set[str] = set()
        self._on_failure: Optional[Callable] = None
        self._on_recovery: Optional[Callable] = None
        self._running = False

    def add_node(self, node_id: str):
        """Start monitoring a new node."""
        if node_id not in self._detectors:
            self._detectors[node_id] = PhiAccrualDetector(threshold=self.phi_threshold)
            logger.info(f"[{self.node_id}] Now monitoring node {node_id}")

    def remove_node(self, node_id: str):
        """Stop monitoring a node."""
        self._detectors.pop(node_id, None)
        self._known_alive.discard(node_id)

    def record_heartbeat(self, from_node: str):
        """Record a heartbeat from a peer node."""
        if from_node not in self._detectors:
            self.add_node(from_node)
        self._detectors[from_node].heartbeat()

    def on_failure(self, callback: Callable):
        """Register a callback for node failure events."""
        self._on_failure = callback

    def on_recovery(self, callback: Callable):
        """Register a callback for node recovery events."""
        self._on_recovery = callback

    def is_alive(self, node_id: str) -> bool:
        """Check if a specific node is believed to be alive."""
        detector = self._detectors.get(node_id)
        if detector is None:
            return False
        return detector.is_available()

    def get_alive_nodes(self) -> Set[str]:
        """Return the set of currently alive nodes."""
        return {nid for nid, det in self._detectors.items() if det.is_available()}

    def get_failed_nodes(self) -> Set[str]:
        """Return the set of currently suspected failed nodes."""
        return {nid for nid, det in self._detectors.items() if not det.is_available()}

    def get_phi(self, node_id: str) -> float:
        """Get the current phi value for a node."""
        detector = self._detectors.get(node_id)
        return detector.phi() if detector else 0.0

    async def run_check_loop(self):
        """
        Periodically check phi values and emit failure/recovery events.
        """
        self._running = True
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            current_alive = self.get_alive_nodes()

            # Check for newly failed nodes
            newly_failed = self._known_alive - current_alive
            for node_id in newly_failed:
                phi = self.get_phi(node_id)
                logger.warning(
                    f"[{self.node_id}] Node {node_id} suspected FAILED (phi={phi:.2f})"
                )
                if self._on_failure:
                    try:
                        await asyncio.ensure_future(
                            self._on_failure(node_id)
                            if asyncio.iscoroutine(self._on_failure(node_id))
                            else asyncio.coroutine(lambda: self._on_failure(node_id))()
                        )
                    except Exception as e:
                        logger.error(f"Failure callback error: {e}")

            # Check for recovered nodes
            newly_recovered = current_alive - self._known_alive
            for node_id in newly_recovered:
                logger.info(f"[{self.node_id}] Node {node_id} RECOVERED")
                if self._on_recovery:
                    try:
                        await asyncio.ensure_future(
                            self._on_recovery(node_id)
                            if asyncio.iscoroutine(self._on_recovery(node_id))
                            else asyncio.coroutine(lambda: self._on_recovery(node_id))()
                        )
                    except Exception as e:
                        logger.error(f"Recovery callback error: {e}")

            self._known_alive = current_alive

    def stop(self):
        self._running = False