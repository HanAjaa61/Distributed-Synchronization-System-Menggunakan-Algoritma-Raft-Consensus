"""
Unit tests for Raft consensus implementation.
Tests leader election, log replication, and failure scenarios.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.consensus.raft import RaftNode, NodeState, LogEntry


@pytest.fixture
def single_node():
    """Single Raft node with no peers (becomes leader immediately)."""
    node = RaftNode(node_id="node-1", peers=[], 
                    election_timeout_min=0.05,
                    election_timeout_max=0.1)
    return node


@pytest.fixture
def three_node_cluster():
    """Three Raft nodes for cluster tests."""
    nodes = {}
    for i in range(1, 4):
        nid = f"node-{i}"
        peers = [f"node-{j}" for j in range(1, 4) if j != i]
        nodes[nid] = RaftNode(node_id=nid, peers=peers,
                               election_timeout_min=0.05,
                               election_timeout_max=0.1)
    return nodes


class TestRaftInitialization:
    def test_initial_state_is_follower(self, single_node):
        assert single_node.state == NodeState.FOLLOWER

    def test_initial_term_is_zero(self, single_node):
        assert single_node.persistent.current_term == 0

    def test_initial_voted_for_is_none(self, single_node):
        assert single_node.persistent.voted_for is None

    def test_initial_log_is_empty(self, single_node):
        assert len(single_node.persistent.log) == 0

    def test_initial_commit_index_is_zero(self, single_node):
        assert single_node.volatile.commit_index == 0


class TestVoteRequest:
    @pytest.mark.asyncio
    async def test_vote_granted_for_higher_term(self, single_node):
        result = await single_node.handle_vote_request({
            "term": 1,
            "candidate_id": "node-2",
            "last_log_index": 0,
            "last_log_term": 0,
        })
        assert result["vote_granted"] is True
        assert single_node.persistent.voted_for == "node-2"

    @pytest.mark.asyncio
    async def test_vote_denied_for_lower_term(self, single_node):
        single_node.persistent.current_term = 5
        result = await single_node.handle_vote_request({
            "term": 3,
            "candidate_id": "node-2",
            "last_log_index": 0,
            "last_log_term": 0,
        })
        assert result["vote_granted"] is False

    @pytest.mark.asyncio
    async def test_cannot_vote_twice_same_term(self, single_node):
        # First vote
        await single_node.handle_vote_request({
            "term": 1,
            "candidate_id": "node-2",
            "last_log_index": 0,
            "last_log_term": 0,
        })
        # Second vote for different candidate same term
        result = await single_node.handle_vote_request({
            "term": 1,
            "candidate_id": "node-3",
            "last_log_index": 0,
            "last_log_term": 0,
        })
        assert result["vote_granted"] is False

    @pytest.mark.asyncio
    async def test_vote_denied_if_log_less_updated(self, single_node):
        # Add a log entry
        single_node.persistent.log.append(LogEntry(term=1, index=1, command={"x": 1}))
        result = await single_node.handle_vote_request({
            "term": 2,
            "candidate_id": "node-2",
            "last_log_index": 0,  # Candidate has empty log
            "last_log_term": 0,
        })
        assert result["vote_granted"] is False


class TestLogReplication:
    @pytest.mark.asyncio
    async def test_append_entries_success(self, single_node):
        result = await single_node.handle_append_entries({
            "term": 1,
            "leader_id": "node-2",
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [
                {"term": 1, "index": 1, "command": {"type": "set", "key": "x", "value": 1},
                 "client_id": None, "sequence_num": 0}
            ],
            "leader_commit": 0,
        })
        assert result["success"] is True
        assert len(single_node.persistent.log) == 1

    @pytest.mark.asyncio
    async def test_append_entries_rejected_stale_term(self, single_node):
        single_node.persistent.current_term = 5
        result = await single_node.handle_append_entries({
            "term": 3,
            "leader_id": "node-2",
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0,
        })
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_log_consistency_check(self, single_node):
        """Reject AppendEntries if prevLogIndex/Term don't match."""
        single_node.persistent.log.append(LogEntry(term=1, index=1, command={}))
        result = await single_node.handle_append_entries({
            "term": 2,
            "leader_id": "node-2",
            "prev_log_index": 1,
            "prev_log_term": 99,  # Wrong term
            "entries": [],
            "leader_commit": 0,
        })
        assert result["success"] is False


class TestLeaderElection:
    @pytest.mark.asyncio
    async def test_single_node_becomes_leader(self):
        """A single-node cluster should elect itself leader."""
        node = RaftNode("solo", [], election_timeout_min=0.05, election_timeout_max=0.08)
        await node.start()
        await asyncio.sleep(0.2)  # Wait for election
        assert node.state == NodeState.LEADER
        await node.stop()

    @pytest.mark.asyncio
    async def test_get_status_structure(self, single_node):
        status = single_node.get_status()
        assert "node_id" in status
        assert "state" in status
        assert "term" in status
        assert "log_length" in status
        assert "commit_index" in status


class TestDeadlockDetector:
    def test_cycle_detection_no_deadlock(self):
        from src.nodes.lock_manager import WaitForGraph
        wfg = WaitForGraph()
        wfg.add_wait("A", "B")
        wfg.add_wait("B", "C")
        assert wfg.detect_cycle() is None

    def test_cycle_detection_with_deadlock(self):
        from src.nodes.lock_manager import WaitForGraph
        wfg = WaitForGraph()
        wfg.add_wait("A", "B")
        wfg.add_wait("B", "C")
        wfg.add_wait("C", "A")  # Cycle
        cycle = wfg.detect_cycle()
        assert cycle is not None
        assert len(cycle) >= 2

    def test_victim_selection(self):
        from src.nodes.lock_manager import WaitForGraph
        wfg = WaitForGraph()
        cycle = ["A", "B", "C"]
        victim = wfg.get_victim(cycle)
        assert victim == "C"  # Last in cycle


class TestConsistentHashing:
    def test_stable_partition_assignment(self):
        from src.nodes.queue_node import ConsistentHashRing
        ring = ConsistentHashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        key = "test-message-key"
        p1 = ring.get_partition(key, 16)
        p2 = ring.get_partition(key, 16)
        assert p1 == p2  # Deterministic

    def test_partition_range(self):
        from src.nodes.queue_node import ConsistentHashRing
        ring = ConsistentHashRing()
        ring.add_node("node-1")
        for key in [f"key-{i}" for i in range(100)]:
            p = ring.get_partition(key, 16)
            assert 0 <= p < 16

    def test_node_assignment(self):
        from src.nodes.queue_node import ConsistentHashRing
        ring = ConsistentHashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        node = ring.get_node("some-key")
        assert node in ("node-1", "node-2")


class TestMESICache:
    def test_lru_cache_basic(self):
        from src.nodes.cache_node import LRUCache, CacheLine
        cache = LRUCache(3)
        for i in range(3):
            cache.put(f"key-{i}", CacheLine(key=f"key-{i}", value=i))
        assert len(cache) == 3
        
        # Adding 4th evicts LRU
        evicted = cache.put("key-3", CacheLine(key="key-3", value=3))
        assert evicted is not None
        assert len(cache) == 3

    def test_lfu_cache_basic(self):
        from src.nodes.cache_node import LFUCache, CacheLine
        cache = LFUCache(3)
        for i in range(3):
            cache.put(f"key-{i}", CacheLine(key=f"key-{i}", value=i))
        # Access key-0 more to raise its frequency
        cache.get("key-0")
        cache.get("key-0")
        # Evict — should evict key-1 or key-2 (lower frequency)
        evicted = cache.put("key-3", CacheLine(key="key-3", value=3))
        assert evicted != "key-0"  # key-0 should survive

    @pytest.mark.asyncio
    async def test_cache_write_creates_entry(self):
        from src.nodes.cache_node import CacheNode
        node = CacheNode("test-node", capacity=100)
        await node.write("mykey", "myvalue")
        value = await node.read("mykey")
        assert value == "myvalue"

    @pytest.mark.asyncio
    async def test_cache_miss_returns_none(self):
        from src.nodes.cache_node import CacheNode
        node = CacheNode("test-node", capacity=100)
        value = await node.read("nonexistent")
        assert value is None

    def test_hit_rate_calculation(self):
        from src.nodes.cache_node import CacheNode
        node = CacheNode("test-node", capacity=100)
        node.hits = 7
        node.misses = 3
        assert node.hit_rate == 0.7


class TestPhiAccrualDetector:
    def test_initial_phi_zero(self):
        from src.communication.failure_detector import PhiAccrualDetector
        det = PhiAccrualDetector()
        assert det.phi() == 0.0

    def test_phi_increases_without_heartbeat(self):
        import time
        from src.communication.failure_detector import PhiAccrualDetector
        det = PhiAccrualDetector()
        det.heartbeat()
        import time
        time.sleep(0.1)
        assert det.phi() > 0.0

    def test_heartbeat_resets_suspicion(self):
        from src.communication.failure_detector import PhiAccrualDetector
        det = PhiAccrualDetector(threshold=8.0)
        for _ in range(10):
            det.heartbeat()
        assert det.is_available()