"""
Integration tests untuk Distributed Synchronization System
"""
import pytest
import asyncio
from src.nodes import LockManager, QueueNode, CacheNode, NodeInfo, LockType
from src.communication import FailureDetector, NodeStatus


@pytest.mark.asyncio
async def test_multi_node_lock_coordination():
    """Test lock coordination antara multiple nodes"""
    # Create 3 nodes
    nodes = []
    for i in range(1, 4):
        node_info = NodeInfo(
            node_id=f"node-{i}",
            host="localhost",
            port=5000 + i,
            cluster_name="test-cluster"
        )
        peers = [f"node-{j}" for j in range(1, 4) if j != i]
        manager = LockManager(node_info, peers)
        nodes.append(manager)
    
    # Set first node as leader
    nodes[0].raft_node.state = nodes[0].raft_node.RaftState.LEADER
    
    # Acquire locks dari different nodes
    from src.nodes import LockStatus
    
    status1, _ = await nodes[0].acquire_lock(
        "resource-1", LockType.EXCLUSIVE, "txn-1", "client-1"
    )
    assert status1 == LockStatus.GRANTED
    
    status2, _ = await nodes[1].acquire_lock(
        "resource-1", LockType.SHARED, "txn-2", "client-2"
    )
    assert status2 == LockStatus.WAITING


@pytest.mark.asyncio
async def test_failure_detection():
    """Test failure detection mechanism"""
    detector = FailureDetector(
        heartbeat_interval=0.1,
        heartbeat_timeout=2.0,   # node-1 gets heartbeat, must stay alive for 1s
        suspected_timeout=5.0
    )
    
    await detector.start_detection()
    
    # Register nodes
    detector.register_node("node-1")
    detector.register_node("node-2")
    
    # Send heartbeat untuk node-1
    detector.on_heartbeat("node-1")
    
    # Wait untuk detect node-2 as suspected
    await asyncio.sleep(1.0)
    
    assert "node-1" in detector.get_alive_nodes()
    
    await detector.stop_detection()


@pytest.mark.asyncio
async def test_queue_enqueue_dequeue():
    """Test queue enqueue dan dequeue operations"""
    node_info = NodeInfo(
        node_id="node-1",
        host="localhost",
        port=5001,
        cluster_name="test-cluster"
    )
    
    queue_node = QueueNode(node_info, ["node-2", "node-3"])
    await queue_node.start()
    
    # Enqueue messages
    msg_id_1 = await queue_node.enqueue("test-queue", {"data": "msg1"}, "producer-1")
    msg_id_2 = await queue_node.enqueue("test-queue", {"data": "msg2"}, "producer-1")
    
    assert msg_id_1 is not None
    assert msg_id_2 is not None
    
    # Dequeue messages
    msg1 = await queue_node.dequeue("test-queue", "consumer-1", timeout=1.0)
    assert msg1 is not None
    assert msg1.content == {"data": "msg1"}
    
    msg2 = await queue_node.dequeue("test-queue", "consumer-1", timeout=1.0)
    assert msg2 is not None
    assert msg2.content == {"data": "msg2"}
    
    await queue_node.stop()


@pytest.mark.asyncio
async def test_cache_hit_and_miss():
    """Test cache hit dan miss"""
    node_info = NodeInfo(
        node_id="node-1",
        host="localhost",
        port=5001,
        cluster_name="test-cluster"
    )
    
    cache_node = CacheNode(node_info, ["node-2", "node-3"])
    await cache_node.start()
    
    # Cache miss
    value1 = await cache_node.get("key-1")
    assert value1 is None
    
    # Put value
    await cache_node.put("key-1", "value-1")
    
    # Cache hit
    value2 = await cache_node.get("key-1")
    assert value2 == "value-1"
    
    await cache_node.stop()
