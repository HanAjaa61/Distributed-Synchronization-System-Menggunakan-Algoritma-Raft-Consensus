"""
Configuration management for distributed sync system.
Loads settings from environment variables with sensible defaults.
"""
import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

load_dotenv()


@dataclass
class NodeConfig:
    node_id: str = field(default_factory=lambda: os.getenv("NODE_ID", "node-1"))
    host: str = field(default_factory=lambda: os.getenv("NODE_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("NODE_PORT", "8001")))
    cluster_nodes: List[str] = field(default_factory=lambda: [
        n.strip() for n in os.getenv("CLUSTER_NODES", "node-1:8001").split(",")
    ])


@dataclass
class RedisConfig:
    host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    password: str = field(default_factory=lambda: os.getenv("REDIS_PASSWORD", ""))
    db: int = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))

    @property
    def url(self) -> str:
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


@dataclass
class RaftConfig:
    election_timeout_min: int = field(
        default_factory=lambda: int(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "150"))
    )
    election_timeout_max: int = field(
        default_factory=lambda: int(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "300"))
    )
    heartbeat_interval: int = field(
        default_factory=lambda: int(os.getenv("RAFT_HEARTBEAT_INTERVAL", "50"))
    )
    log_replication_batch: int = field(
        default_factory=lambda: int(os.getenv("RAFT_LOG_REPLICATION_BATCH", "100"))
    )


@dataclass
class QueueConfig:
    partitions: int = field(
        default_factory=lambda: int(os.getenv("QUEUE_PARTITIONS", "16"))
    )
    replication_factor: int = field(
        default_factory=lambda: int(os.getenv("QUEUE_REPLICATION_FACTOR", "2"))
    )
    max_message_size: int = field(
        default_factory=lambda: int(os.getenv("QUEUE_MAX_MESSAGE_SIZE", "1048576"))
    )
    persistence_path: str = field(
        default_factory=lambda: os.getenv("QUEUE_PERSISTENCE_PATH", "/tmp/queue")
    )


@dataclass
class CacheConfig:
    max_size: int = field(
        default_factory=lambda: int(os.getenv("CACHE_MAX_SIZE", "10000"))
    )
    replacement_policy: str = field(
        default_factory=lambda: os.getenv("CACHE_REPLACEMENT_POLICY", "LRU")
    )
    coherence_protocol: str = field(
        default_factory=lambda: os.getenv("CACHE_COHERENCE_PROTOCOL", "MESI")
    )
    invalidation_timeout: int = field(
        default_factory=lambda: int(os.getenv("CACHE_INVALIDATION_TIMEOUT", "5000"))
    )


@dataclass
class MetricsConfig:
    enabled: bool = field(
        default_factory=lambda: os.getenv("METRICS_ENABLED", "true").lower() == "true"
    )
    port: int = field(
        default_factory=lambda: int(os.getenv("METRICS_PORT", "9090"))
    )
    export_interval: int = field(
        default_factory=lambda: int(os.getenv("METRICS_EXPORT_INTERVAL", "15"))
    )


@dataclass
class SystemConfig:
    node: NodeConfig = field(default_factory=NodeConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    raft: RaftConfig = field(default_factory=RaftConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    network_timeout: int = field(
        default_factory=lambda: int(os.getenv("NETWORK_TIMEOUT", "5000"))
    )
    network_retry_max: int = field(
        default_factory=lambda: int(os.getenv("NETWORK_RETRY_MAX", "3"))
    )


# Singleton config instance
config = SystemConfig()