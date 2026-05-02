"""
Metrics collection module untuk monitoring performa
"""
import time
from dataclasses import dataclass, field
from typing import Dict, List
from collections import defaultdict, deque
import json


@dataclass
class Metric:
    """Representasi single metric"""
    name: str
    value: float
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Collector untuk berbagai metrics sistem"""
    
    def __init__(self, window_size: int = 1000):
        """
        Initialize metrics collector
        
        Args:
            window_size: Ukuran window untuk metric aggregation
        """
        self.window_size = window_size
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = {}
        
    def record_metric(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record sebuah metric"""
        metric = Metric(name=name, value=value, labels=labels or {})
        self.metrics[name].append(metric)
    
    def increment_counter(self, name: str, value: int = 1, labels: Dict[str, str] = None):
        """Increment counter metric"""
        key = self._create_key(name, labels)
        self.counters[key] += value
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set gauge metric"""
        key = self._create_key(name, labels)
        self.gauges[key] = value
    
    def _create_key(self, name: str, labels: Dict[str, str] = None) -> str:
        """Create unique key untuk metric dengan labels"""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
    
    def get_stats(self, name: str) -> Dict:
        """Get statistik untuk metric tertentu"""
        if name not in self.metrics or not self.metrics[name]:
            return {}
        
        values = [m.value for m in self.metrics[name]]
        return {
            "count": len(values),
            "sum": sum(values),
            "mean": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
            "latest": values[-1] if values else None
        }
    
    def get_all_stats(self) -> Dict:
        """Get semua statistik"""
        result = {}
        for name in self.metrics:
            result[name] = self.get_stats(name)
        result["counters"] = dict(self.counters)
        result["gauges"] = dict(self.gauges)
        return result
    
    def reset(self):
        """Reset semua metrics"""
        self.metrics.clear()
        self.counters.clear()
        self.gauges.clear()


class LatencyTracker:
    """Tracker untuk mengukur latency operasi"""
    
    def __init__(self):
        self.latencies: Dict[str, List[float]] = defaultdict(list)
    
    def record(self, operation: str, latency_ms: float):
        """Record latency untuk operasi"""
        self.latencies[operation].append(latency_ms)
    
    def get_stats(self, operation: str) -> Dict:
        """Get statistik latency untuk operasi"""
        if operation not in self.latencies or not self.latencies[operation]:
            return {}
        
        values = self.latencies[operation]
        values_sorted = sorted(values)
        n = len(values)
        
        return {
            "count": n,
            "mean": sum(values) / n,
            "min": min(values),
            "max": max(values),
            "p50": values_sorted[n // 2],
            "p95": values_sorted[int(n * 0.95)] if n > 20 else max(values),
            "p99": values_sorted[int(n * 0.99)] if n > 100 else max(values),
        }
    
    def get_all_stats(self) -> Dict:
        """Get semua latency stats"""
        return {op: self.get_stats(op) for op in self.latencies}
