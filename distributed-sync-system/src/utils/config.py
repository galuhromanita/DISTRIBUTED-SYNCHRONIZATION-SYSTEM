"""
Configuration module untuk Distributed Synchronization System
"""
import os
import logging
import sys
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

load_dotenv()


@dataclass
class NodeConfig:
    """Konfigurasi untuk setiap node"""
    node_id: str
    host: str
    port: int
    cluster_name: str = "default-cluster"


@dataclass
class RaftConfig:
    """Konfigurasi untuk Raft Consensus"""
    election_timeout_min: float = 1.5
    election_timeout_max: float = 3.0
    heartbeat_interval: float = 0.5
    rpc_timeout: float = 2.0


@dataclass
class QueueConfig:
    """Konfigurasi untuk Distributed Queue"""
    num_partitions: int = 10
    replication_factor: int = 3
    persistence_enabled: bool = True
    persistence_dir: str = "./data/queue"


@dataclass
class CacheConfig:
    """Konfigurasi untuk Distributed Cache"""
    protocol: str = "MESI"  # MESI, MOSI, atau MOESI
    replacement_policy: str = "LRU"  # LRU atau LFU
    max_cache_size: int = 10000
    coherence_timeout: float = 5.0


@dataclass
class SystemConfig:
    """Konfigurasi sistem secara keseluruhan"""
    node_config: NodeConfig
    raft_config: RaftConfig = None
    queue_config: QueueConfig = None
    cache_config: CacheConfig = None
    redis_host: str = "localhost"
    redis_port: int = 6379
    log_level: str = "INFO"
    metrics_enabled: bool = True
    metrics_port: int = 8000

    def __post_init__(self):
        if self.raft_config is None:
            self.raft_config = RaftConfig()
        if self.queue_config is None:
            self.queue_config = QueueConfig()
        if self.cache_config is None:
            self.cache_config = CacheConfig()


def load_config_from_env() -> SystemConfig:
    """Load konfigurasi dari environment variables"""
    node_id = os.getenv("NODE_ID", "node-1")
    host = os.getenv("NODE_HOST", "localhost")
    port = int(os.getenv("NODE_PORT", "5000"))
    cluster_name = os.getenv("CLUSTER_NAME", "default-cluster")
    
    node_config = NodeConfig(
        node_id=node_id,
        host=host,
        port=port,
        cluster_name=cluster_name
    )
    
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    
    config = SystemConfig(
        node_config=node_config,
        redis_host=redis_host,
        redis_port=redis_port,
        log_level=os.getenv("LOG_LEVEL", "INFO")
    )
    
    return config


# ======================
# Logging helpers (kept here to match the requested minimal utils structure)
# ======================

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'


def setup_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Setup logger with a consistent console format."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter(LOG_FORMAT)
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get logger instance."""
    return logging.getLogger(name)
