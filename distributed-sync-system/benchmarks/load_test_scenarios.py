"""
Performance benchmarks untuk Distributed Synchronization System
"""
import argparse
import asyncio
import time
from src.nodes import LockManager, QueueNode, CacheNode, NodeInfo, LockType
from src.utils import LatencyTracker


class LockManagerBenchmark:
    """Benchmark untuk lock manager performa"""
    
    @staticmethod
    async def benchmark_lock_acquisition(num_operations: int = 1000):
        """Benchmark lock acquisition performa"""
        node_info = NodeInfo(
            node_id="node-1",
            host="localhost",
            port=5000,
            cluster_name="benchmark"
        )
        
        lock_manager = LockManager(node_info, ["node-2", "node-3"])
        lock_manager.raft_node.state = lock_manager.raft_node.RaftState.LEADER
        
        await lock_manager.start()
        
        latency_tracker = LatencyTracker()
        
        print(f"\n=== Lock Acquisition Benchmark ({num_operations} operations) ===")
        
        start_time = time.time()
        
        for i in range(num_operations):
            op_start = time.time()
            
            status, _ = await lock_manager.acquire_lock(
                resource_id=f"resource-{i % 100}",  # 100 different resources
                lock_type=LockType.EXCLUSIVE if i % 2 == 0 else LockType.SHARED,
                transaction_id=f"txn-{i}",
                client_id=f"client-{i % 10}"
            )
            
            latency_ms = (time.time() - op_start) * 1000
            latency_tracker.record("lock_acquire", latency_ms)
        
        total_time = time.time() - start_time
        
        stats = latency_tracker.get_stats("lock_acquire")
        
        print(f"Total time: {total_time:.3f}s")
        print(f"Throughput: {num_operations/total_time:.0f} ops/sec")
        print(f"Mean latency: {stats['mean']:.3f}ms")
        print(f"Min latency: {stats['min']:.3f}ms")
        print(f"Max latency: {stats['max']:.3f}ms")
        print(f"p95 latency: {stats['p95']:.3f}ms")
        print(f"p99 latency: {stats['p99']:.3f}ms")
        
        await lock_manager.stop()
        
        return {
            "throughput": num_operations / total_time,
            "mean_latency": stats["mean"],
            "p95_latency": stats["p95"],
            "p99_latency": stats["p99"],
        }


class QueueBenchmark:
    """Benchmark untuk queue performa"""
    
    @staticmethod
    async def benchmark_enqueue_dequeue(num_messages: int = 1000):
        """Benchmark queue enqueue/dequeue performa"""
        node_info = NodeInfo(
            node_id="node-1",
            host="localhost",
            port=5001,
            cluster_name="benchmark"
        )
        
        queue_node = QueueNode(node_info, ["node-2", "node-3"])
        await queue_node.start()
        
        latency_tracker = LatencyTracker()
        
        print(f"\n=== Queue Benchmark ({num_messages} messages) ===")
        
        # Enqueue benchmark
        print("\nEnqueue benchmark:")
        start_time = time.time()
        
        for i in range(num_messages):
            op_start = time.time()
            
            await queue_node.enqueue(
                queue_name="benchmark-queue",
                message={"id": i, "data": f"message-{i}"},
                producer_id="producer-1"
            )
            
            latency_ms = (time.time() - op_start) * 1000
            latency_tracker.record("enqueue", latency_ms)
        
        enqueue_time = time.time() - start_time
        stats = latency_tracker.get_stats("enqueue")
        
        print(f"Throughput: {num_messages/enqueue_time:.0f} msgs/sec")
        print(f"Mean latency: {stats['mean']:.3f}ms")
        print(f"p95 latency: {stats['p95']:.3f}ms")
        
        # Dequeue benchmark
        print("\nDequeue benchmark:")
        latency_tracker = LatencyTracker()
        start_time = time.time()
        
        for i in range(num_messages):
            op_start = time.time()
            
            await queue_node.dequeue(
                queue_name="benchmark-queue",
                consumer_id="consumer-1",
                timeout=1.0
            )
            
            latency_ms = (time.time() - op_start) * 1000
            latency_tracker.record("dequeue", latency_ms)
        
        dequeue_time = time.time() - start_time
        stats = latency_tracker.get_stats("dequeue")
        
        print(f"Throughput: {num_messages/dequeue_time:.0f} msgs/sec")
        print(f"Mean latency: {stats['mean']:.3f}ms")
        print(f"p95 latency: {stats['p95']:.3f}ms")
        
        await queue_node.stop()


class CacheBenchmark:
    """Benchmark untuk cache performa"""
    
    @staticmethod
    async def benchmark_cache_operations(num_operations: int = 10000):
        """Benchmark cache get/put performa"""
        node_info = NodeInfo(
            node_id="node-1",
            host="localhost",
            port=5002,
            cluster_name="benchmark"
        )
        
        cache_node = CacheNode(node_info, ["node-2", "node-3"])
        await cache_node.start()
        
        latency_tracker = LatencyTracker()
        
        print(f"\n=== Cache Benchmark ({num_operations} operations) ===")
        
        # Warm up cache
        for i in range(100):
            await cache_node.put(f"key-{i}", f"value-{i}")
        
        # Benchmark cache hits (after warm-up)
        print("\nCache hit benchmark:")
        start_time = time.time()
        
        for i in range(num_operations):
            op_start = time.time()
            
            await cache_node.get(f"key-{i % 100}")
            
            latency_ms = (time.time() - op_start) * 1000
            latency_tracker.record("cache_hit", latency_ms)
        
        hit_time = time.time() - start_time
        stats = latency_tracker.get_stats("cache_hit")
        
        print(f"Throughput: {num_operations/hit_time:.0f} ops/sec")
        print(f"Mean latency: {stats['mean']:.3f}ms")
        print(f"p99 latency: {stats['p99']:.3f}ms")
        
        # Benchmark cache writes
        print("\nCache write benchmark:")
        latency_tracker = LatencyTracker()
        start_time = time.time()
        
        for i in range(num_operations):
            op_start = time.time()
            
            await cache_node.put(f"write-key-{i}", f"value-{i}")
            
            latency_ms = (time.time() - op_start) * 1000
            latency_tracker.record("cache_write", latency_ms)
        
        write_time = time.time() - start_time
        stats = latency_tracker.get_stats("cache_write")
        
        print(f"Throughput: {num_operations/write_time:.0f} ops/sec")
        print(f"Mean latency: {stats['mean']:.3f}ms")
        print(f"p99 latency: {stats['p99']:.3f}ms")
        
        cache_status = cache_node.get_cache_status()
        cache_stats = cache_node.get_cache_stats()
        
        print(f"\nCache Status:")
        print(f"Size: {cache_status['cache_size']}/{cache_status['max_capacity']}")
        print(f"Hit rate: {cache_stats['hit_rate']}")
        
        await cache_node.stop()


async def run_all_benchmarks():
    """Run all benchmarks"""
    print("=" * 60)
    print("DISTRIBUTED SYNCHRONIZATION SYSTEM - PERFORMANCE BENCHMARKS")
    print("=" * 60)
    
    # Lock Manager benchmarks
    await LockManagerBenchmark.benchmark_lock_acquisition(1000)
    
    # Queue benchmarks
    await QueueBenchmark.benchmark_enqueue_dequeue(1000)
    
    # Cache benchmarks
    await CacheBenchmark.benchmark_cache_operations(10000)
    
    print("\n" + "=" * 60)
    print("BENCHMARKS COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Sync System benchmarks")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run a small/fast benchmark set (recommended for video demo)",
    )
    parser.add_argument(
        "--lock-ops",
        type=int,
        default=None,
        help="Override number of lock operations",
    )
    parser.add_argument(
        "--queue-msgs",
        type=int,
        default=None,
        help="Override number of queue messages",
    )
    parser.add_argument(
        "--cache-ops",
        type=int,
        default=None,
        help="Override number of cache operations",
    )
    args = parser.parse_args()

    # NOTE: the benchmarks are in-process and are meant for a quick demo of throughput/latency.
    if args.quick:
        lock_ops = args.lock_ops or 200
        queue_msgs = args.queue_msgs or 200
        cache_ops = args.cache_ops or 2000
    else:
        lock_ops = args.lock_ops or 1000
        queue_msgs = args.queue_msgs or 1000
        cache_ops = args.cache_ops or 10000

    async def run_custom():
        print("=" * 60)
        print("DISTRIBUTED SYNCHRONIZATION SYSTEM - PERFORMANCE BENCHMARKS")
        print("=" * 60)

        await LockManagerBenchmark.benchmark_lock_acquisition(lock_ops)
        await QueueBenchmark.benchmark_enqueue_dequeue(queue_msgs)
        await CacheBenchmark.benchmark_cache_operations(cache_ops)

        print("\n" + "=" * 60)
        print("BENCHMARKS COMPLETED")
        print("=" * 60)

    asyncio.run(run_custom())
