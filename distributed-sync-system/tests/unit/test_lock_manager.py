"""
Unit tests untuk Lock Manager
"""
import pytest
import asyncio
from src.nodes import LockManager, NodeInfo, LockType
from src.consensus.raft import RaftState



@pytest.fixture
def lock_manager():
    """Create lock manager fixture"""
    node_info = NodeInfo(
        node_id="node-1",
        host="localhost",
        port=5000,
        cluster_name="test-cluster"
    )
    
    manager = LockManager(node_info, ["node-2", "node-3"])
    return manager


@pytest.mark.asyncio
async def test_lock_acquisition(lock_manager):
    """Test acquiring lock"""
    # Set as leader
    lock_manager.raft_node.state = RaftState.LEADER
    
    status, lock = await lock_manager.acquire_lock(
        resource_id="resource-1",
        lock_type=LockType.EXCLUSIVE,
        transaction_id="txn-1",
        client_id="client-1"
    )
    
    from src.nodes import LockStatus
    assert status == LockStatus.GRANTED
    assert lock is not None
    assert lock.resource_id == "resource-1"


@pytest.mark.asyncio
async def test_exclusive_lock_blocking(lock_manager):
    """Test exclusive lock blocks shared lock"""
    lock_manager.raft_node.state = RaftState.LEADER
    
    # Acquire exclusive lock
    status1, _ = await lock_manager.acquire_lock(
        resource_id="resource-1",
        lock_type=LockType.EXCLUSIVE,
        transaction_id="txn-1",
        client_id="client-1"
    )
    
    from src.nodes import LockStatus
    assert status1 == LockStatus.GRANTED
    
    # Try to acquire shared lock (should wait)
    status2, _ = await lock_manager.acquire_lock(
        resource_id="resource-1",
        lock_type=LockType.SHARED,
        transaction_id="txn-2",
        client_id="client-2"
    )
    
    assert status2 == LockStatus.WAITING


@pytest.mark.asyncio
async def test_shared_locks_concurrent(lock_manager):
    """Test multiple shared locks can coexist"""
    lock_manager.raft_node.state = RaftState.LEADER
    
    from src.nodes import LockStatus
    
    # Acquire first shared lock
    status1, lock1 = await lock_manager.acquire_lock(
        resource_id="resource-1",
        lock_type=LockType.SHARED,
        transaction_id="txn-1",
        client_id="client-1"
    )
    assert status1 == LockStatus.GRANTED
    
    # Acquire second shared lock on same resource
    status2, lock2 = await lock_manager.acquire_lock(
        resource_id="resource-1",
        lock_type=LockType.SHARED,
        transaction_id="txn-2",
        client_id="client-2"
    )
    assert status2 == LockStatus.GRANTED


@pytest.mark.asyncio
async def test_lock_release(lock_manager):
    """Test releasing lock"""
    lock_manager.raft_node.state = RaftState.LEADER
    
    from src.nodes import LockStatus
    
    # Acquire lock
    status, _ = await lock_manager.acquire_lock(
        resource_id="resource-1",
        lock_type=LockType.EXCLUSIVE,
        transaction_id="txn-1",
        client_id="client-1"
    )
    assert status == LockStatus.GRANTED
    
    # Release lock
    released = await lock_manager.release_lock(
        resource_id="resource-1",
        transaction_id="txn-1",
        client_id="client-1"
    )
    
    assert released == True
    assert "resource-1" not in lock_manager.locks or len(lock_manager.locks["resource-1"]) == 0
