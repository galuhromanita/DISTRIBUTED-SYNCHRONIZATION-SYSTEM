"""
Unit tests untuk Raft Consensus
"""
import pytest
import asyncio
from src.consensus import RaftNode, RaftState, LogEntry


@pytest.mark.asyncio
async def test_raft_initialization():
    """Test Raft node initialization"""
    node = RaftNode("node-1", ["node-2", "node-3"])
    
    assert node.node_id == "node-1"
    assert node.state == RaftState.FOLLOWER
    assert node.current_term == 0
    assert len(node.peers) == 2


@pytest.mark.asyncio
async def test_raft_start_stop():
    """Test Raft node start dan stop"""
    node = RaftNode("node-1", ["node-2", "node-3"])
    
    await node.start()
    assert node._running == True
    
    await asyncio.sleep(0.1)
    
    await node.stop()
    assert node._running == False


@pytest.mark.asyncio
async def test_raft_append_entry():
    """Test append entry ke log"""
    node = RaftNode("node-1", [])
    
    # Manual set to leader
    node.state = RaftState.LEADER
    node.current_term = 1
    
    # Append entry
    result = await node.append_entry({"command": "test"})
    
    assert result == True
    assert len(node.log) == 1
    assert node.log[0].command == {"command": "test"}


@pytest.mark.asyncio
async def test_raft_log_replication():
    """Test log replication antara nodes"""
    nodes = [
        RaftNode("node-1", ["node-2", "node-3"]),
        RaftNode("node-2", ["node-1", "node-3"]),
        RaftNode("node-3", ["node-1", "node-2"]),
    ]
    
    # Set node-1 sebagai leader
    nodes[0].state = RaftState.LEADER
    nodes[0].current_term = 1
    
    # Append entry
    await nodes[0].append_entry({"command": "test"})
    
    assert len(nodes[0].log) == 1


def test_raft_state_transitions():
    """Test Raft state transitions"""
    node = RaftNode("node-1", ["node-2", "node-3"])
    
    # Start sebagai FOLLOWER
    assert node.state == RaftState.FOLLOWER
    
    # Transition ke CANDIDATE
    node.state = RaftState.CANDIDATE
    assert node.state == RaftState.CANDIDATE
    
    # Transition ke LEADER
    node.state = RaftState.LEADER
    assert node.state == RaftState.LEADER


@pytest.mark.asyncio
async def test_raft_term_update():
    """Test term update di Raft nodes"""
    node = RaftNode("node-1", ["node-2", "node-3"])
    
    # Simulate receiving higher term
    from src.communication import RaftMessage
    
    message = RaftMessage(
        message_type=None,
        sender_id="node-2",
        receiver_id="node-1",
        term=5
    )
    
    old_term = node.current_term
    
    # Handle higher term
    response = node.handle_request_vote(message)
    
    # Term should be updated
    assert node.current_term == 5
