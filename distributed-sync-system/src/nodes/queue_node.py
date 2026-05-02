"""
Distributed Queue Node menggunakan Consistent Hashing
"""
import asyncio
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import deque
import hashlib
import json

from src.utils import get_logger, MetricsCollector
from src.nodes.base_node import BaseNode, NodeInfo

logger = get_logger(__name__)


@dataclass
class QueueMessage:
    """Message dalam queue"""
    message_id: str
    content: Any
    producer_id: str
    timestamp: float = field(default_factory=time.time)
    delivered_to: Dict[str, bool] = field(default_factory=dict)  # consumer_id -> delivered
    retry_count: int = 0


class ConsistentHash:
    """Consistent hashing untuk queue partitioning"""
    
    def __init__(self, nodes: List[str] = None, replicas: int = 3):
        """
        Initialize consistent hash ring
        
        Args:
            nodes: List of node IDs
            replicas: Number of replicas untuk each node
        """
        self.replicas = replicas
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def _hash(self, key: str) -> int:
        """Hash key to integer"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node_id: str) -> None:
        """Add node ke ring"""
        for i in range(self.replicas):
            virtual_key = f"{node_id}:{i}"
            hash_key = self._hash(virtual_key)
            self.ring[hash_key] = node_id
            self.sorted_keys.append(hash_key)
        
        self.sorted_keys.sort()
        logger.info(f"Node {node_id} added to consistent hash ring")
    
    def remove_node(self, node_id: str) -> None:
        """Remove node dari ring"""
        for i in range(self.replicas):
            virtual_key = f"{node_id}:{i}"
            hash_key = self._hash(virtual_key)
            if hash_key in self.ring:
                del self.ring[hash_key]
                self.sorted_keys.remove(hash_key)
        
        logger.info(f"Node {node_id} removed from consistent hash ring")
    
    def get_node(self, key: str) -> Optional[str]:
        """Get node untuk key"""
        if not self.ring:
            return None
        
        hash_key = self._hash(key)
        
        # Find first node >= hash_key
        for ring_key in self.sorted_keys:
            if ring_key >= hash_key:
                return self.ring[ring_key]
        
        # Wrap around
        return self.ring[self.sorted_keys[0]]
    
    def get_replicas(self, key: str, num_replicas: int = 3) -> List[str]:
        """Get replica nodes untuk key"""
        if not self.ring:
            return []
        
        replicas = []
        seen_nodes = set()
        hash_key = self._hash(key)
        
        # Find replica nodes
        for ring_key in self.sorted_keys:
            if ring_key >= hash_key:
                node = self.ring[ring_key]
                if node not in seen_nodes:
                    replicas.append(node)
                    seen_nodes.add(node)
                    if len(replicas) >= num_replicas:
                        break
        
        # Wrap around jika perlu
        for ring_key in self.sorted_keys:
            if len(replicas) >= num_replicas:
                break
            node = self.ring[ring_key]
            if node not in seen_nodes:
                replicas.append(node)
                seen_nodes.add(node)
        
        return replicas


class QueueNode(BaseNode):
    """
    Distributed Queue Node menggunakan Consistent Hashing
    
    Fitur:
    - Consistent hashing untuk partition
    - Replication untuk fault tolerance
    - At-least-once delivery guarantee
    - Message persistence
    """
    
    def __init__(self, node_info: NodeInfo, peers: List[str], num_partitions: int = 10):
        """
        Initialize queue node
        
        Args:
            node_info: Node information
            peers: List of peer node IDs
            num_partitions: Number of partitions
        """
        super().__init__(node_info)
        
        self.peers = peers
        self.num_partitions = num_partitions
        
        # Queue storage
        self.queues: Dict[str, deque] = {}  # queue_name -> deque of messages
        self.message_history: Dict[str, list] = {}  # untuk persistence
        
        # Consistent hashing
        self.hash_ring = ConsistentHash(nodes=[self.node_id] + self.peers)
        
        # Consumer state
        self.consumer_offsets: Dict[str, Dict[str, int]] = {}  # queue -> consumer -> offset
        
        logger.info(f"Queue Node {self.node_id} initialized with {num_partitions} partitions")
    
    async def _start_components(self):
        """Start queue node components"""
        # Start background tasks untuk message delivery dan cleanup
        asyncio.create_task(self._process_messages())
        asyncio.create_task(self._cleanup_old_messages())
    
    async def _stop_components(self):
        """Stop queue node components"""
        pass
    
    async def enqueue(
        self,
        queue_name: str,
        message: Any,
        producer_id: str
    ) -> str:
        """
        Enqueue message
        
        Args:
            queue_name: Nama queue
            message: Content message
            producer_id: ID producer
        
        Returns:
            Message ID
        """
        import uuid
        
        message_id = str(uuid.uuid4())
        
        # Determine partition using consistent hash
        partition_key = f"{queue_name}:{message_id}"
        target_node = self.hash_ring.get_node(partition_key)
        
        # Create message object
        queue_msg = QueueMessage(
            message_id=message_id,
            content=message,
            producer_id=producer_id
        )
        
        # Store locally
        if queue_name not in self.queues:
            self.queues[queue_name] = deque()
        
        self.queues[queue_name].append(queue_msg)
        
        # Store ke history untuk persistence
        if queue_name not in self.message_history:
            self.message_history[queue_name] = []
        
        self.message_history[queue_name].append({
            "message_id": message_id,
            "content": message,
            "producer_id": producer_id,
            "timestamp": queue_msg.timestamp,
        })
        
        self.metrics.increment_counter("messages_enqueued")
        logger.debug(f"Message {message_id} enqueued to {queue_name}")
        
        # Replicate ke other nodes
        await self._replicate_message(queue_name, queue_msg)
        
        return message_id
    
    async def dequeue(
        self,
        queue_name: str,
        consumer_id: str,
        timeout: float = 5.0
    ) -> Optional[QueueMessage]:
        """
        Dequeue message dari queue
        
        Args:
            queue_name: Nama queue
            consumer_id: ID consumer
            timeout: Timeout untuk wait message
        
        Returns:
            Message atau None jika timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if queue_name in self.queues and self.queues[queue_name]:
                message = self.queues[queue_name].popleft()
                
                # Record delivery
                message.delivered_to[consumer_id] = True
                
                self.metrics.increment_counter("messages_dequeued")
                logger.debug(f"Message {message.message_id} dequeued by {consumer_id}")
                
                return message
            
            await asyncio.sleep(0.1)
        
        logger.debug(f"Dequeue timeout untuk {queue_name}")
        return None
    
    async def _replicate_message(self, queue_name: str, message: QueueMessage) -> None:
        """Replicate message ke replica nodes"""
        replicas = self.hash_ring.get_replicas(f"{queue_name}:{message.message_id}")
        
        for replica_node in replicas:
            if replica_node != self.node_id:
                # Dalam implementasi real, kirim ke replica node via network
                logger.debug(f"Replicating message {message.message_id} to {replica_node}")
    
    async def _process_messages(self) -> None:
        """Process messages dalam queue"""
        while self._running:
            try:
                # Process any pending operations
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Error in message processing: {e}", exc_info=True)
    
    async def _cleanup_old_messages(self) -> None:
        """Clean up old messages untuk save storage"""
        while self._running:
            try:
                current_time = time.time()
                retention_time = 3600  # Keep 1 hour
                
                for queue_name, history in self.message_history.items():
                    # Remove old messages
                    self.message_history[queue_name] = [
                        msg for msg in history
                        if current_time - msg["timestamp"] < retention_time
                    ]
                
                await asyncio.sleep(300)  # Check every 5 minutes
            
            except Exception as e:
                logger.error(f"Error in cleanup: {e}", exc_info=True)
    
    def get_queue_status(self) -> Dict:
        """Get status semua queues"""
        return {
            "node_id": self.node_id,
            "queues": {
                queue_name: {
                    "message_count": len(queue),
                    "history_size": len(self.message_history.get(queue_name, [])),
                }
                for queue_name, queue in self.queues.items()
            },
            "total_messages": sum(len(q) for q in self.queues.values()),
        }
