"""
Base node class untuk distributed system
"""
import asyncio
import uuid
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from src.utils import get_logger, MetricsCollector
from src.communication import MessageBus, FailureDetector
from src.consensus import RaftNode

logger = get_logger(__name__)


@dataclass
class NodeInfo:
    """Informasi tentang node"""
    node_id: str
    host: str
    port: int
    cluster_name: str


class BaseNode(ABC):
    """
    Base class untuk semua node dalam distributed system
    """
    
    def __init__(self, node_info: NodeInfo):
        """
        Initialize base node
        
        Args:
            node_info: Informasi node
        """
        self.node_info = node_info
        self.node_id = node_info.node_id
        
        # Komponyen utama
        self.message_bus = MessageBus(self.node_id)
        self.failure_detector = FailureDetector()
        self.raft_node: Optional[RaftNode] = None
        
        # Metrics
        self.metrics = MetricsCollector()
        
        # State
        self._running = False
        self._startup_tasks: List[asyncio.Task] = []
    
    async def start(self):
        """Start node"""
        if self._running:
            return
        
        self._running = True
        logger.info(f"Starting node {self.node_id}")
        
        # Start failure detector
        await self.failure_detector.start_detection()
        
        # Start Raft consensus
        if self.raft_node:
            await self.raft_node.start()
        
        # Start node-specific components
        await self._start_components()
        
        logger.info(f"Node {self.node_id} started successfully")
    
    async def stop(self):
        """Stop node"""
        if not self._running:
            return
        
        self._running = False
        logger.info(f"Stopping node {self.node_id}")
        
        # Stop node-specific components
        await self._stop_components()
        
        # Stop Raft consensus
        if self.raft_node:
            await self.raft_node.stop()
        
        # Stop failure detector
        await self.failure_detector.stop_detection()
        
        # Cancel startup tasks
        for task in self._startup_tasks:
            if not task.done():
                task.cancel()
        
        logger.info(f"Node {self.node_id} stopped")
    
    @abstractmethod
    async def _start_components(self):
        """Start node-specific components"""
        pass
    
    @abstractmethod
    async def _stop_components(self):
        """Stop node-specific components"""
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """Get status dari node"""
        return {
            "node_id": self.node_id,
            "is_running": self._running,
            "raft_status": self.raft_node.get_status() if self.raft_node else None,
            "alive_nodes": list(self.failure_detector.get_alive_nodes()),
            "suspected_nodes": list(self.failure_detector.get_suspected_nodes()),
            "dead_nodes": list(self.failure_detector.get_dead_nodes()),
        }
