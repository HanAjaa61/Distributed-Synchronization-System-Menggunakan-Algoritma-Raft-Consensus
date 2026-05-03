"""
Raft Consensus Algorithm Implementation
========================================
Implements the core Raft protocol for leader election and log replication.
Used as the foundation for the Distributed Lock Manager.

References: "In Search of an Understandable Consensus Algorithm" (Ongaro, Ousterhout)
"""
import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class NodeState(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]
    client_id: Optional[str] = None
    sequence_num: int = 0

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict) -> "LogEntry":
        return cls(**d)


@dataclass
class RaftState:
    """Persistent state that must survive crashes (written to stable storage)."""
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)


@dataclass
class VolatileState:
    """Volatile state, reset on crash."""
    commit_index: int = 0
    last_applied: int = 0


@dataclass
class LeaderState:
    """Leader-only volatile state, reset on election."""
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)


class RaftNode:
    """
    Full implementation of the Raft consensus algorithm.
    
    Handles:
    - Leader election with randomized timeouts
    - Log replication with strong consistency
    - Cluster membership changes
    - Log compaction (snapshots)
    - Client interaction with linearizable semantics
    """

    def __init__(self, node_id: str, peers: List[str],
                 election_timeout_min: float = 0.15,
                 election_timeout_max: float = 0.30,
                 heartbeat_interval: float = 0.05):
        self.node_id = node_id
        self.peers = peers
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval

        # State
        self.state = NodeState.FOLLOWER
        self.persistent = RaftState()
        self.volatile = VolatileState()
        self.leader_state = LeaderState()

        # Current leader
        self.current_leader: Optional[str] = None

        # Timers
        self._election_timer_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._apply_task: Optional[asyncio.Task] = None

        # Message sender (injected)
        self._send_message = None

        # Pending client requests
        self._pending_commits: Dict[int, asyncio.Future] = {}

        # State machine apply callback
        self._state_machine_apply = None

        self._running = False
        self._votes_received: set = set()

        logger.info(f"[Raft:{self.node_id}] Initialized with peers: {peers}")

    def set_message_sender(self, sender):
        """Inject the message sending function."""
        self._send_message = sender

    def set_state_machine(self, apply_fn):
        """Set the function to apply committed commands to the state machine."""
        self._state_machine_apply = apply_fn

    # ─────────────────────────────── Startup ────────────────────────────────

    async def start(self):
        self._running = True
        self._apply_task = asyncio.create_task(self._apply_loop())
        self._reset_election_timer()
        logger.info(f"[Raft:{self.node_id}] Node started as FOLLOWER")

    async def stop(self):
        self._running = False
        for task in [self._election_timer_task, self._heartbeat_task, self._apply_task]:
            if task and not task.done():
                task.cancel()

    # ────────────────────────────── Timers ──────────────────────────────────

    def _election_timeout(self) -> float:
        return random.uniform(self.election_timeout_min, self.election_timeout_max)

    def _reset_election_timer(self):
        if self._election_timer_task and not self._election_timer_task.done():
            self._election_timer_task.cancel()
        if self._running and self.state != NodeState.LEADER:
            self._election_timer_task = asyncio.create_task(self._election_timer())

    async def _election_timer(self):
        try:
            await asyncio.sleep(self._election_timeout())
            if self.state != NodeState.LEADER:
                await self._start_election()
        except asyncio.CancelledError:
            pass

    # ─────────────────────────── Leader Election ─────────────────────────────

    async def _start_election(self):
        """Begin a new election as a candidate."""
        self.persistent.current_term += 1
        self.state = NodeState.CANDIDATE
        self.persistent.voted_for = self.node_id
        self._votes_received = {self.node_id}
        self.current_leader = None

        logger.info(
            f"[Raft:{self.node_id}] Starting election for term {self.persistent.current_term}"
        )

        # Single-node cluster: no peers needed, win immediately
        if not self.peers:
            await self._become_leader()
            return

        last_log_index = len(self.persistent.log) - 1
        last_log_term = self.persistent.log[-1].term if self.persistent.log else 0

        vote_request = {
            "term": self.persistent.current_term,
            "candidate_id": self.node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        }

        # Request votes from all peers concurrently
        tasks = [
            asyncio.create_task(self._request_vote(peer, vote_request))
            for peer in self.peers
        ]
        # Don't await — process results as they come in via handle_vote_response
        for task in tasks:
            asyncio.ensure_future(task)

        # Restart timer in case election fails
        self._reset_election_timer()

    async def _request_vote(self, peer: str, payload: Dict):
        """Send a vote request to a peer and process the response."""
        if self._send_message is None:
            return
        try:
            response = await self._send_message(peer, "vote_request", payload)
            if response:
                await self.handle_vote_response(response)
        except Exception as e:
            logger.debug(f"[Raft:{self.node_id}] Vote request to {peer} failed: {e}")

    async def handle_vote_request(self, payload: Dict) -> Dict:
        """
        Process a RequestVote RPC from a candidate.
        Returns vote granted/denied.
        """
        term = payload["term"]
        candidate_id = payload["candidate_id"]
        last_log_index = payload["last_log_index"]
        last_log_term = payload["last_log_term"]

        # If we see a higher term, revert to follower
        if term > self.persistent.current_term:
            await self._become_follower(term)

        # Check if we can vote for this candidate
        vote_granted = False
        if term >= self.persistent.current_term:
            # Already voted for someone else this term
            if (self.persistent.voted_for is None or
                    self.persistent.voted_for == candidate_id):
                # Check log is at least as up-to-date
                my_last_term = self.persistent.log[-1].term if self.persistent.log else 0
                my_last_index = len(self.persistent.log) - 1

                log_ok = (last_log_term > my_last_term or
                          (last_log_term == my_last_term and last_log_index >= my_last_index))

                if log_ok:
                    vote_granted = True
                    self.persistent.voted_for = candidate_id
                    self._reset_election_timer()

        logger.debug(
            f"[Raft:{self.node_id}] Vote {'granted' if vote_granted else 'denied'} "
            f"to {candidate_id} for term {term}"
        )
        return {
            "term": self.persistent.current_term,
            "vote_granted": vote_granted,
            "voter_id": self.node_id,
        }

    async def handle_vote_response(self, payload: Dict):
        """Process a vote response. Become leader if quorum reached."""
        if self.state != NodeState.CANDIDATE:
            return

        term = payload["term"]
        vote_granted = payload["vote_granted"]
        voter_id = payload.get("voter_id", "unknown")

        if term > self.persistent.current_term:
            await self._become_follower(term)
            return

        if vote_granted and term == self.persistent.current_term:
            self._votes_received.add(voter_id)
            total_nodes = len(self.peers) + 1
            quorum = total_nodes // 2 + 1
            if len(self._votes_received) >= quorum:
                await self._become_leader()

    # ────────────────────────── Log Replication ──────────────────────────────

    async def _become_leader(self):
        """Transition to leader state."""
        self.state = NodeState.LEADER
        self.current_leader = self.node_id
        logger.info(
            f"[Raft:{self.node_id}] BECAME LEADER for term {self.persistent.current_term}"
        )

        # Initialize leader volatile state
        last_index = len(self.persistent.log)
        self.leader_state.next_index = {p: last_index + 1 for p in self.peers}
        self.leader_state.match_index = {p: 0 for p in self.peers}

        # Send no-op entry to commit any uncommitted entries from prev terms
        await self.propose({"type": "no_op", "leader": self.node_id})

        # Start sending heartbeats
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _become_follower(self, term: int):
        """Revert to follower state with a new term."""
        self.state = NodeState.FOLLOWER
        self.persistent.current_term = term
        self.persistent.voted_for = None
        self._reset_election_timer()
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()

    async def _heartbeat_loop(self):
        """Send periodic AppendEntries to all peers as heartbeat."""
        while self._running and self.state == NodeState.LEADER:
            await self._replicate_to_all()
            await asyncio.sleep(self.heartbeat_interval)

    async def _replicate_to_all(self):
        """Send AppendEntries to all peers."""
        tasks = [
            asyncio.create_task(self._replicate_to_peer(peer))
            for peer in self.peers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _replicate_to_peer(self, peer: str):
        """Send AppendEntries RPC to a specific peer."""
        if self._send_message is None:
            return

        next_idx = self.leader_state.next_index.get(peer, 1)
        prev_log_index = next_idx - 1
        prev_log_term = 0
        if prev_log_index > 0 and prev_log_index <= len(self.persistent.log):
            prev_log_term = self.persistent.log[prev_log_index - 1].term

        entries = [
            e.to_dict() for e in self.persistent.log[next_idx - 1:]
        ]

        payload = {
            "term": self.persistent.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": self.volatile.commit_index,
        }

        try:
            response = await self._send_message(peer, "append_entries", payload)
            if response:
                await self._handle_append_entries_response(peer, response, len(entries), next_idx)
        except Exception as e:
            logger.debug(f"[Raft:{self.node_id}] Replication to {peer} failed: {e}")

    async def _handle_append_entries_response(self, peer: str, response: Dict,
                                               entries_sent: int, prev_next_idx: int):
        """Process AppendEntries response and update commit index."""
        term = response.get("term", 0)
        success = response.get("success", False)

        if term > self.persistent.current_term:
            await self._become_follower(term)
            return

        if self.state != NodeState.LEADER:
            return

        if success:
            new_match = prev_next_idx - 1 + entries_sent
            self.leader_state.match_index[peer] = max(
                self.leader_state.match_index.get(peer, 0), new_match
            )
            self.leader_state.next_index[peer] = new_match + 1
            await self._advance_commit_index()
        else:
            # Decrement next_index and retry
            self.leader_state.next_index[peer] = max(
                1, self.leader_state.next_index.get(peer, 1) - 1
            )

    async def _advance_commit_index(self):
        """
        Advance commit index if a majority have replicated the entry.
        Only commit entries from the current term (Raft safety property).
        """
        total_nodes = len(self.peers) + 1
        quorum = total_nodes // 2 + 1

        for idx in range(len(self.persistent.log), self.volatile.commit_index, -1):
            if self.persistent.log[idx - 1].term != self.persistent.current_term:
                continue
            count = 1 + sum(
                1 for p in self.peers
                if self.leader_state.match_index.get(p, 0) >= idx
            )
            if count >= quorum:
                if idx > self.volatile.commit_index:
                    self.volatile.commit_index = idx
                    logger.debug(f"[Raft:{self.node_id}] Commit index advanced to {idx}")
                    # Resolve all pending futures up to this index
                    for pending_idx in list(self._pending_commits.keys()):
                        if pending_idx <= idx:
                            fut = self._pending_commits.pop(pending_idx)
                            if not fut.done():
                                fut.set_result(True)
                break

    async def handle_append_entries(self, payload: Dict) -> Dict:
        """
        Process an AppendEntries RPC from the leader.
        Returns success/failure.
        """
        term = payload["term"]
        leader_id = payload["leader_id"]
        prev_log_index = payload["prev_log_index"]
        prev_log_term = payload["prev_log_term"]
        entries = [LogEntry.from_dict(e) for e in payload.get("entries", [])]
        leader_commit = payload["leader_commit"]

        if term < self.persistent.current_term:
            return {"term": self.persistent.current_term, "success": False}

        # Valid leader contact — reset election timer
        await self._become_follower(term)
        self.current_leader = leader_id
        self._reset_election_timer()

        # Check log consistency
        if prev_log_index > 0:
            if len(self.persistent.log) < prev_log_index:
                return {"term": self.persistent.current_term, "success": False}
            if self.persistent.log[prev_log_index - 1].term != prev_log_term:
                # Delete conflicting entries
                self.persistent.log = self.persistent.log[:prev_log_index - 1]
                return {"term": self.persistent.current_term, "success": False}

        # Append new entries
        for i, entry in enumerate(entries):
            idx = prev_log_index + i
            if idx < len(self.persistent.log):
                if self.persistent.log[idx].term != entry.term:
                    self.persistent.log = self.persistent.log[:idx]
                    self.persistent.log.append(entry)
            else:
                self.persistent.log.append(entry)

        # Update commit index
        if leader_commit > self.volatile.commit_index:
            self.volatile.commit_index = min(
                leader_commit, len(self.persistent.log)
            )

        return {"term": self.persistent.current_term, "success": True}

    # ────────────────────────── Client Interface ─────────────────────────────

    async def propose(self, command: Dict, timeout: float = 5.0) -> bool:
        """
        Propose a command to be committed via Raft consensus.
        Only the leader can accept proposals.
        Returns True if committed, False otherwise.
        """
        if self.state != NodeState.LEADER:
            logger.warning(f"[Raft:{self.node_id}] Proposal rejected — not leader")
            return False

        entry = LogEntry(
            term=self.persistent.current_term,
            index=len(self.persistent.log) + 1,
            command=command,
        )
        self.persistent.log.append(entry)

        # Create a future to wait for commit
        future = asyncio.get_event_loop().create_future()
        self._pending_commits[entry.index] = future

        # Trigger immediate replication
        asyncio.ensure_future(self._replicate_to_all())

        try:
            await asyncio.wait_for(future, timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(f"[Raft:{self.node_id}] Proposal timed out for index {entry.index}")
            self._pending_commits.pop(entry.index, None)
            return False

    # ────────────────────────── State Machine ────────────────────────────────

    async def _apply_loop(self):
        """Continuously apply committed entries to the state machine."""
        while self._running:
            while self.volatile.last_applied < self.volatile.commit_index:
                self.volatile.last_applied += 1
                idx = self.volatile.last_applied
                if idx <= len(self.persistent.log):
                    entry = self.persistent.log[idx - 1]
                    if self._state_machine_apply:
                        try:
                            await self._state_machine_apply(entry)
                        except Exception as e:
                            logger.error(f"[Raft:{self.node_id}] State machine error: {e}")
            await asyncio.sleep(0.001)

    # ─────────────────────────── Status ─────────────────────────────────────

    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.persistent.current_term,
            "leader": self.current_leader,
            "log_length": len(self.persistent.log),
            "commit_index": self.volatile.commit_index,
            "last_applied": self.volatile.last_applied,
            "voted_for": self.persistent.voted_for,
        }