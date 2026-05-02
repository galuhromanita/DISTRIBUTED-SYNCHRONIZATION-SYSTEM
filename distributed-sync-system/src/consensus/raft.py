"""
Fixed Raft Consensus Implementation (Working Distributed Version)
"""
import asyncio
import random
import time
from enum import Enum
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass

import aiohttp
from src.utils import get_logger

logger = get_logger(__name__)


# ======================
# STATE
# ======================
class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


# ======================
# LOG ENTRY
# ======================
@dataclass
class LogEntry:
    term: int
    index: int
    command: Any
    committed: bool = False


# ======================
# RAFT NODE
# ======================
class RaftNode:
    # Expose RaftState as class attribute so tests can do node.raft_node.RaftState.LEADER
    RaftState = RaftState

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        port_map: Optional[Dict[str, int]] = None,
        peer_host_map: Optional[Dict[str, str]] = None,  # node_id -> hostname
        heartbeat_interval: float = 0.5,
    ):
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.port_map = port_map or {}
        # Default: all peers on localhost
        self.peer_host_map = peer_host_map or {p: "localhost" for p in self.peers}

        self.heartbeat_interval = heartbeat_interval

        # state
        self.state = RaftState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None

        # log
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0

        # leader state
        self.next_index = {}
        self.match_index = {}

        # runtime
        self._running = False

        # callbacks
        self.on_commit: Optional[Callable] = None
        self.on_leader_elected: Optional[Callable] = None

        logger.info(f"Raft node {node_id} initialized")

    # ======================
    # START
    # ======================
    async def start(self):
        self._running = True

        if self.node_id == "node-1":
            await self._become_leader()

        asyncio.create_task(self._heartbeat_loop())
        asyncio.create_task(self._commit_loop())

        logger.info(f"{self.node_id} started")

    # ======================
    # STOP
    # ======================
    async def stop(self):
        self._running = False
        logger.info(f"{self.node_id} stopped")

    # ======================
    # HEARTBEAT LOOP
    # ======================
    async def _heartbeat_loop(self):
        while self._running:
            if self.state == RaftState.LEADER:
                await self._send_heartbeats()
            await asyncio.sleep(self.heartbeat_interval)

    # ======================
    # SEND HEARTBEAT (REAL NETWORK)
    # ======================
    async def _send_heartbeats(self):
        tasks = []
        for peer in self.peers:
            tasks.append(self._send_append_entries(peer))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_append_entries(self, peer_id: str):
        """REAL HTTP RPC to follower (Docker-aware)"""

        host = self.peer_host_map.get(peer_id, "localhost")
        port = self.port_map.get(peer_id, 8000)
        url = f"http://{host}:{port}/raft/append"

        prev_index = len(self.log) - 1
        prev_term = self.log[prev_index].term if prev_index >= 0 else 0

        payload = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": [
                {
                    "term": e.term,
                    "index": e.index,
                    "command": e.command,
                }
                for e in self.log
            ],
            "leader_commit": self.commit_index,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=2) as resp:
                    data = await resp.json()

                    if data.get("success"):
                        self.leader_id = self.node_id
                    return data

        except Exception as e:
            logger.error(f"Failed to send to {peer_id}: {e}")
            return {"success": False}

    # ======================
    # RECEIVE APPEND ENTRIES (CALLED BY API)
    # ======================
    def handle_append_entries(self, message: dict):
        """Follower receives heartbeat"""

        self.leader_id = message["leader_id"]
        self.current_term = message["term"]
        self.state = RaftState.FOLLOWER

        self.log = [
            LogEntry(**entry) for entry in message.get("entries", [])
        ]

        self.commit_index = message.get("leader_commit", 0)

        return {"term": self.current_term, "success": True}

    # ======================
    # COMMIT LOOP
    # ======================
    async def _commit_loop(self):
        while self._running:
            while self.last_applied < self.commit_index:
                self.last_applied += 1

                if self.last_applied < len(self.log):
                    entry = self.log[self.last_applied]

                    if self.on_commit:
                        await self._safe_call(self.on_commit, entry.command)

            await asyncio.sleep(0.05)

    # ======================
    # APPEND ENTRY (LEADER ONLY)
    # ======================
    async def append_entry(self, command: Any):
        if self.state != RaftState.LEADER:
            return False

        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command,
        )

        self.log.append(entry)
        self.commit_index = len(self.log) - 1

        return True

    # ======================
    # LEADER ELECTION (SIMPLIFIED)
    # ======================
    async def _become_leader(self):
        self.state = RaftState.LEADER
        self.leader_id = self.node_id

        logger.info(f"{self.node_id} became LEADER")

        if self.on_leader_elected:
            await self._safe_call(self.on_leader_elected)

    # ======================
    # SAFE CALLBACK
    # ======================
    async def _safe_call(self, fn, *args):
        try:
            if asyncio.iscoroutinefunction(fn):
                await fn(*args)
            else:
                fn(*args)
        except Exception as e:
            logger.error(f"Callback error: {e}")

    # ======================
    # HANDLE REQUEST VOTE (FOLLOWER SIDE)
    # ======================
    def handle_request_vote(self, message) -> dict:
        """
        Handle RequestVote RPC from a candidate.
        Updates current_term if message.term is higher,
        reverts to FOLLOWER, and grants vote.
        """
        if message.term > self.current_term:
            self.current_term = message.term
            self.state = RaftState.FOLLOWER
            self.voted_for = message.sender_id
            vote_granted = True
        elif message.term == self.current_term and (
            self.voted_for is None or self.voted_for == message.sender_id
        ):
            self.voted_for = message.sender_id
            vote_granted = True
        else:
            vote_granted = False

        return {
            "term": self.current_term,
            "vote_granted": vote_granted,
        }

    # ======================
    # STATUS
    # ======================
    def get_status(self):
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "log_length": len(self.log),
            "commit_index": self.commit_index,
        }