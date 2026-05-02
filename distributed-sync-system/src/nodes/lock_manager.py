from typing import List, Dict, Optional
import asyncio
import time
import uuid
from enum import Enum
from dataclasses import dataclass, field

from src.utils import get_logger
from src.nodes.base_node import BaseNode, NodeInfo
from src.consensus import RaftNode, RaftState

logger = get_logger(__name__)


class LockType(Enum):
    EXCLUSIVE = "exclusive"
    SHARED = "shared"


class LockStatus(Enum):
    GRANTED = "granted"
    WAITING = "waiting"
    DENIED = "denied"


@dataclass
class Lock:
    resource_id: str
    lock_type: LockType
    holder_id: str
    transaction_id: str
    acquired_at: float = field(default_factory=time.time)


class LockManager(BaseNode):
    def __init__(
        self,
        node_info: NodeInfo,
        peers: List[str],
        port_map: Optional[Dict[str, int]] = None,
        peer_host_map: Optional[Dict[str, str]] = None,
    ):
        super().__init__(node_info)

        self.peers = peers
        self.port_map = port_map or {}
        self.peer_host_map = peer_host_map or {}

        # resource_id -> list of Lock objects
        self.locks: Dict[str, List[Lock]] = {}
        self.transaction_locks: Dict[str, List[str]] = {}  # txn_id -> [resource_id]

        self.raft_node = RaftNode(
            node_id=self.node_id,
            peers=self.peers,
            port_map=self.port_map,
            peer_host_map=self.peer_host_map,
        )

        self.raft_node.on_leader_elected = self._on_leader_elected
        self.raft_node.on_commit = self._on_commit

        logger.info(f"LockManager {self.node_id} initialized")

    async def _start_components(self):
        pass

    async def _stop_components(self):
        pass

    # =========================
    # LOCK CORE
    # =========================
    async def acquire_lock(
        self,
        resource_id: str,
        lock_type: LockType,
        transaction_id: str,
        client_id: str,
    ):
        """
        Attempt to acquire a lock.

        Returns:
            (LockStatus, Lock | None)
        """
        self.locks.setdefault(resource_id, [])
        existing = self.locks[resource_id]

        if self.raft_node.state != RaftState.LEADER:
            # Non-leader: check if resource is already globally locked
            # and return WAITING to indicate request is pending/forwarded
            if existing:
                return LockStatus.WAITING, None
            # No lock held yet — also waiting (not authoritative to grant)
            return LockStatus.WAITING, None

        # --- LEADER path ---
        # Check compatibility
        if existing:
            # Any EXCLUSIVE holder blocks everything
            if any(l.lock_type == LockType.EXCLUSIVE for l in existing):
                return LockStatus.WAITING, None

            # If we want EXCLUSIVE but there are SHARED holders -> wait
            if lock_type == LockType.EXCLUSIVE:
                return LockStatus.WAITING, None

            # Multiple SHARED locks can coexist, fall through to grant

        lock = Lock(
            resource_id=resource_id,
            lock_type=lock_type,
            holder_id=client_id,
            transaction_id=transaction_id,
        )
        self.locks[resource_id].append(lock)

        # Track per transaction
        self.transaction_locks.setdefault(transaction_id, [])
        self.transaction_locks[transaction_id].append(resource_id)

        return LockStatus.GRANTED, lock

    async def release_lock(
        self,
        resource_id: str,
        transaction_id: str,
        client_id: str,
    ) -> bool:
        if resource_id not in self.locks:
            return False

        before = len(self.locks[resource_id])
        self.locks[resource_id] = [
            l for l in self.locks[resource_id]
            if l.transaction_id != transaction_id
        ]
        after = len(self.locks[resource_id])

        return before > after

    # =========================
    # RAFT CALLBACK
    # =========================
    async def _on_commit(self, command):
        logger.info(f"Committed: {command}")

    async def _on_leader_elected(self):
        logger.info(f"{self.node_id} is LEADER")