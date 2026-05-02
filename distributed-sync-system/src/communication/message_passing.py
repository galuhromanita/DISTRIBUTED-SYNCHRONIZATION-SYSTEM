"""
Message passing module untuk komunikasi antar nodes
"""
import json
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Callable, List
from enum import Enum
import time
from src.utils import get_logger

logger = get_logger(__name__)


class MessageType(Enum):
    """Tipe-tipe message dalam sistem"""
    # Raft messages
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"
    REQUEST_VOTE = "request_vote"
    REQUEST_VOTE_RESPONSE = "request_vote_response"
    
    # Lock messages
    LOCK_REQUEST = "lock_request"
    LOCK_RESPONSE = "lock_response"
    LOCK_RELEASE = "lock_release"
    DEADLOCK_DETECTION = "deadlock_detection"
    
    # Queue messages
    ENQUEUE = "enqueue"
    DEQUEUE = "dequeue"
    QUEUE_RESPONSE = "queue_response"
    
    # Cache messages
    CACHE_INVALIDATE = "cache_invalidate"
    CACHE_UPDATE = "cache_update"
    CACHE_COHERENCE = "cache_coherence"
    
    # General
    HEARTBEAT = "heartbeat"
    PING = "ping"
    PONG = "pong"
    ERROR = "error"


@dataclass
class Message:
    """Base message class"""
    message_type: MessageType
    sender_id: str
    receiver_id: str
    timestamp: float = field(default_factory=time.time)
    message_id: str = field(default="")
    payload: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message ke dictionary"""
        return {
            "message_type": self.message_type.value,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
            "payload": self.payload,
        }
    
    def to_json(self) -> str:
        """Convert message ke JSON"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """Create message dari dictionary"""
        return cls(
            message_type=MessageType(data["message_type"]),
            sender_id=data["sender_id"],
            receiver_id=data["receiver_id"],
            timestamp=data.get("timestamp", time.time()),
            message_id=data.get("message_id", ""),
            payload=data.get("payload", {}),
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        """Create message dari JSON"""
        return cls.from_dict(json.loads(json_str))


@dataclass
class RaftMessage(Message):
    """Message untuk Raft consensus"""
    term: int = 0
    leader_id: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        d["term"] = self.term
        d["leader_id"] = self.leader_id
        return d


@dataclass
class AppendEntriesMessage(RaftMessage):
    """Append entries message (heartbeat atau log replication)"""
    prev_log_index: int = 0
    prev_log_term: int = 0
    leader_commit: int = 0
    entries: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class LockMessage(Message):
    """Message untuk lock operations"""
    resource_id: str = ""
    lock_type: str = "exclusive"  # exclusive atau shared
    transaction_id: str = ""


class MessageHandler(ABC):
    """Abstract base class untuk message handlers"""
    
    @abstractmethod
    async def handle(self, message: Message) -> Any:
        """Handle incoming message"""
        pass


class MessageBus:
    """Central message bus untuk routing messages"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.handlers: Dict[MessageType, List[Callable]] = {}
        self.pending_responses: Dict[str, asyncio.Future] = {}
        
    def subscribe(self, message_type: MessageType, handler: Callable):
        """Subscribe ke message type tertentu"""
        if message_type not in self.handlers:
            self.handlers[message_type] = []
        self.handlers[message_type].append(handler)
        logger.debug(f"Handler subscribed untuk {message_type.value}")
    
    async def publish(self, message: Message) -> None:
        """Publish message ke semua subscribers"""
        if message.message_type not in self.handlers:
            logger.warning(f"No handlers untuk {message.message_type.value}")
            return
        
        for handler in self.handlers[message.message_type]:
            try:
                await handler(message)
            except Exception as e:
                logger.error(f"Error handling message: {e}", exc_info=True)
    
    def register_response_waiter(self, message_id: str) -> asyncio.Future:
        """Register waiter untuk response"""
        future = asyncio.Future()
        self.pending_responses[message_id] = future
        return future
    
    async def deliver_response(self, message_id: str, response: Any) -> None:
        """Deliver response ke waiter"""
        if message_id in self.pending_responses:
            future = self.pending_responses.pop(message_id)
            if not future.done():
                future.set_result(response)


class NetworkInterface(ABC):
    """Abstract interface untuk network communication"""
    
    @abstractmethod
    async def send_message(self, message: Message) -> None:
        """Send message ke node lain"""
        pass
    
    @abstractmethod
    async def receive_messages(self) -> None:
        """Receive messages dari network"""
        pass
    
    @abstractmethod
    async def start(self) -> None:
        """Start network interface"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop network interface"""
        pass
