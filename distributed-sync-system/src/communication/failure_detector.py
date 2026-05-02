"""
Failure detector untuk mendeteksi node failures dalam distributed system
"""
import asyncio
import time
from typing import Dict, Set, Callable, Optional
from dataclasses import dataclass
from enum import Enum

from src.utils import get_logger

logger = get_logger(__name__)


class NodeStatus(Enum):
    """Status node dalam sistem"""
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"


@dataclass
class NodeHeartbeat:
    """Record heartbeat dari node"""
    node_id: str
    last_heartbeat: float
    status: NodeStatus


class FailureDetector:
    """
    Failure detector menggunakan heartbeat mechanism
    Untuk mendeteksi node failures dalam distributed system
    """
    
    def __init__(
        self,
        heartbeat_interval: float = 1.0,
        heartbeat_timeout: float = 5.0,
        suspected_timeout: float = 10.0,
    ):
        """
        Initialize failure detector
        
        Args:
            heartbeat_interval: Interval untuk send heartbeat
            heartbeat_timeout: Timeout untuk heartbeat (consider suspected)
            suspected_timeout: Timeout untuk suspected node (consider dead)
        """
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.suspected_timeout = suspected_timeout
        
        self.heartbeats: Dict[str, NodeHeartbeat] = {}
        self.alive_nodes: Set[str] = set()
        self.suspected_nodes: Set[str] = set()
        self.dead_nodes: Set[str] = set()
        
        self.failure_callbacks: list[Callable] = []
        self.recovery_callbacks: list[Callable] = []
        
        self._running = False
        self._check_task: Optional[asyncio.Task] = None
    
    def register_node(self, node_id: str):
        """Register node baru ke failure detector"""
        self.heartbeats[node_id] = NodeHeartbeat(
            node_id=node_id,
            last_heartbeat=time.time(),
            status=NodeStatus.ALIVE
        )
        self.alive_nodes.add(node_id)
        logger.info(f"Node {node_id} registered")
    
    def on_heartbeat(self, node_id: str):
        """Receive heartbeat dari node"""
        if node_id not in self.heartbeats:
            self.register_node(node_id)
        
        hb = self.heartbeats[node_id]
        old_status = hb.status
        hb.last_heartbeat = time.time()
        hb.status = NodeStatus.ALIVE
        
        # Node recovered from suspected/dead state
        if old_status != NodeStatus.ALIVE:
            self.suspected_nodes.discard(node_id)
            self.dead_nodes.discard(node_id)
            self.alive_nodes.add(node_id)
            
            logger.info(f"Node {node_id} recovered from {old_status.value}")
            self._trigger_recovery_callbacks(node_id)
    
    async def start_detection(self):
        """Start failure detection"""
        if self._running:
            return
        
        self._running = True
        self._check_task = asyncio.create_task(self._check_heartbeats_loop())
        logger.info("Failure detector started")
    
    async def stop_detection(self):
        """Stop failure detection"""
        self._running = False
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        logger.info("Failure detector stopped")
    
    async def _check_heartbeats_loop(self):
        """Main loop untuk check heartbeats"""
        while self._running:
            try:
                current_time = time.time()
                
                for node_id, hb in self.heartbeats.items():
                    time_since_heartbeat = current_time - hb.last_heartbeat
                    
                    if hb.status == NodeStatus.ALIVE:
                        # Check jika alive node should be suspected
                        if time_since_heartbeat > self.heartbeat_timeout:
                            hb.status = NodeStatus.SUSPECTED
                            self.alive_nodes.discard(node_id)
                            self.suspected_nodes.add(node_id)
                            logger.warning(f"Node {node_id} suspected failed")
                            self._trigger_failure_callbacks(node_id, NodeStatus.SUSPECTED)
                    
                    elif hb.status == NodeStatus.SUSPECTED:
                        # Check jika suspected node should be declared dead
                        if time_since_heartbeat > self.suspected_timeout:
                            hb.status = NodeStatus.DEAD
                            self.suspected_nodes.discard(node_id)
                            self.dead_nodes.add(node_id)
                            logger.error(f"Node {node_id} declared dead")
                            self._trigger_failure_callbacks(node_id, NodeStatus.DEAD)
                
                await asyncio.sleep(self.heartbeat_interval)
            
            except Exception as e:
                logger.error(f"Error in heartbeat check loop: {e}", exc_info=True)
    
    def register_failure_callback(self, callback: Callable):
        """Register callback untuk node failures"""
        self.failure_callbacks.append(callback)
    
    def register_recovery_callback(self, callback: Callable):
        """Register callback untuk node recovery"""
        self.recovery_callbacks.append(callback)
    
    def _trigger_failure_callbacks(self, node_id: str, status: NodeStatus):
        """Trigger failure callbacks"""
        for callback in self.failure_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(node_id, status))
                else:
                    callback(node_id, status)
            except Exception as e:
                logger.error(f"Error in failure callback: {e}", exc_info=True)
    
    def _trigger_recovery_callbacks(self, node_id: str):
        """Trigger recovery callbacks"""
        for callback in self.recovery_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(node_id))
                else:
                    callback(node_id)
            except Exception as e:
                logger.error(f"Error in recovery callback: {e}", exc_info=True)
    
    def get_alive_nodes(self) -> Set[str]:
        """Get set of alive nodes"""
        return self.alive_nodes.copy()
    
    def get_suspected_nodes(self) -> Set[str]:
        """Get set of suspected nodes"""
        return self.suspected_nodes.copy()
    
    def get_dead_nodes(self) -> Set[str]:
        """Get set of dead nodes"""
        return self.dead_nodes.copy()
    
    def get_status(self, node_id: str) -> Optional[NodeStatus]:
        """Get status dari node"""
        if node_id in self.heartbeats:
            return self.heartbeats[node_id].status
        return None
