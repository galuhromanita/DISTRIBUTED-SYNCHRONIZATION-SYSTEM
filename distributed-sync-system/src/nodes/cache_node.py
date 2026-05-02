"""
Distributed Cache Coherence Node menggunakan MESI protocol
"""
import asyncio
import time
from typing import List, Dict, Optional, Any, Set
from enum import Enum
from dataclasses import dataclass, field

from src.utils import get_logger, MetricsCollector
from src.nodes.base_node import BaseNode, NodeInfo

logger = get_logger(__name__)


class CacheLineState(Enum):
    """MESI protocol states"""
    MODIFIED = "modified"      # M - Modified (only cache has valid copy)
    EXCLUSIVE = "exclusive"    # E - Exclusive (only cache, not modified)
    SHARED = "shared"          # S - Shared (multiple copies, clean)
    INVALID = "invalid"        # I - Invalid (data stale)


@dataclass
class CacheLine:
    """Entry dalam cache"""
    key: str
    value: Any
    state: CacheLineState
    timestamp: float = field(default_factory=time.time)
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)


class CacheNode(BaseNode):
    """
    Distributed Cache Coherence Node menggunakan MESI protocol
    
    Fitur:
    - MESI cache coherence protocol
    - LRU replacement policy
    - Automatic invalidation propagation
    - Performance monitoring
    """
    
    def __init__(
        self,
        node_info: NodeInfo,
        peers: List[str],
        max_cache_size: int = 10000,
        replacement_policy: str = "LRU"
    ):
        """
        Initialize cache node
        
        Args:
            node_info: Node information
            peers: List of peer node IDs
            max_cache_size: Maximum cache size
            replacement_policy: LRU atau LFU
        """
        super().__init__(node_info)
        
        self.peers = peers
        self.max_cache_size = max_cache_size
        self.replacement_policy = replacement_policy
        
        # Cache storage
        self.cache: Dict[str, CacheLine] = {}
        
        # Tracking untuk invalidation
        self.pending_invalidations: Set[str] = set()
        
        logger.info(f"Cache Node {self.node_id} initialized (capacity: {max_cache_size}, policy: {replacement_policy})")
    
    async def _start_components(self):
        """Start cache node components"""
        asyncio.create_task(self._process_invalidations())
    
    async def _stop_components(self):
        """Stop cache node components"""
        pass
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value dari cache
        
        Args:
            key: Cache key
        
        Returns:
            Value atau None jika cache miss
        """
        if key in self.cache:
            cache_line = self.cache[key]
            
            # Update access info
            cache_line.access_count += 1
            cache_line.last_accessed = time.time()
            
            # Check state
            if cache_line.state == CacheLineState.INVALID:
                logger.debug(f"Cache miss (invalid): {key}")
                self.metrics.increment_counter("cache_miss_invalid")
                return None
            
            logger.debug(f"Cache hit: {key}")
            self.metrics.increment_counter("cache_hit")
            
            return cache_line.value
        
        logger.debug(f"Cache miss: {key}")
        self.metrics.increment_counter("cache_miss")
        return None
    
    async def put(self, key: str, value: Any) -> None:
        """
        Put value ke cache
        
        Args:
            key: Cache key
            value: Value untuk di-cache
        """
        # Check cache capacity
        if len(self.cache) >= self.max_cache_size:
            await self._evict_line()
        
        # Create atau update cache line
        if key in self.cache:
            cache_line = self.cache[key]
            cache_line.value = value
            cache_line.state = CacheLineState.MODIFIED
            cache_line.timestamp = time.time()
            cache_line.access_count += 1
            cache_line.last_accessed = time.time()
        else:
            self.cache[key] = CacheLine(
                key=key,
                value=value,
                state=CacheLineState.MODIFIED
            )
        
        self.metrics.increment_counter("cache_write")
        logger.debug(f"Cache put: {key} (MODIFIED)")
        
        # Invalidate di nodes lain
        await self._broadcast_invalidation(key)
    
    async def invalidate(self, key: str) -> None:
        """
        Invalidate cache line
        
        Args:
            key: Cache key untuk di-invalidate
        """
        if key in self.cache:
            self.cache[key].state = CacheLineState.INVALID
            logger.debug(f"Cache invalidated: {key}")
            self.metrics.increment_counter("cache_invalidation")
    
    async def _broadcast_invalidation(self, key: str) -> None:
        """Broadcast invalidation ke peers"""
        for peer in self.peers:
            # Dalam implementasi real, kirim invalidation message
            logger.debug(f"Broadcasting invalidation for {key} to {peer}")
    
    async def _evict_line(self) -> None:
        """Evict cache line based on replacement policy"""
        if not self.cache:
            return
        
        if self.replacement_policy == "LRU":
            # Remove least recently used
            victim_key = min(
                self.cache.keys(),
                key=lambda k: self.cache[k].last_accessed
            )
        else:  # LFU
            # Remove least frequently used
            victim_key = min(
                self.cache.keys(),
                key=lambda k: self.cache[k].access_count
            )
        
        del self.cache[victim_key]
        logger.debug(f"Cache evicted ({self.replacement_policy}): {victim_key}")
        self.metrics.increment_counter("cache_eviction")
    
    async def _process_invalidations(self) -> None:
        """Process pending invalidations"""
        while self._running:
            try:
                # Process any pending invalidations
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Error in invalidation processing: {e}", exc_info=True)
    
    def get_cache_status(self) -> Dict:
        """Get cache status"""
        total_size = len(self.cache)
        
        state_counts = {}
        for state in CacheLineState:
            state_counts[state.value] = sum(
                1 for line in self.cache.values()
                if line.state == state
            )
        
        return {
            "node_id": self.node_id,
            "cache_size": total_size,
            "max_capacity": self.max_cache_size,
            "utilization": f"{(total_size / self.max_cache_size * 100):.2f}%",
            "state_distribution": state_counts,
            "policy": self.replacement_policy,
        }
    
    def get_cache_stats(self) -> Dict:
        """Get cache performance stats"""
        total_hits = self.metrics.counters.get("cache_hit", 0)
        total_misses = self.metrics.counters.get("cache_miss", 0)
        total_accesses = total_hits + total_misses
        
        hit_rate = (total_hits / total_accesses * 100) if total_accesses > 0 else 0
        
        return {
            "hits": total_hits,
            "misses": total_misses,
            "hit_rate": f"{hit_rate:.2f}%",
            "writes": self.metrics.counters.get("cache_write", 0),
            "invalidations": self.metrics.counters.get("cache_invalidation", 0),
            "evictions": self.metrics.counters.get("cache_eviction", 0),
        }


from typing import List
