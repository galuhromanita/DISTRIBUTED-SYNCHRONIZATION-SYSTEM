#!/bin/bash

# Distributed Synchronization System - Deployment Guide

## Prerequisites

- Docker dan Docker Compose
- Python 3.8+ (untuk local development)
- Redis (atau gunakan docker-compose version)

## Quick Start dengan Docker Compose

### 1. Setup Environment

```bash
cd distributed-sync-system
cp .env.example .env
```

### 2. Build dan Run

```bash
# Build images
docker-compose build

# Start system
docker-compose up -d

# Check status
docker-compose ps
```

### 3. Access Services

- **Node 1**: http://localhost:5001 (metrics: 8001)
- **Node 2**: http://localhost:5002 (metrics: 8002)
- **Node 3**: http://localhost:5003 (metrics: 8003)
- **Redis**: localhost:6379
- **Prometheus**: http://localhost:9090

### 4. View Logs

```bash
# View all logs
docker-compose logs -f

# View specific node
docker-compose logs -f node-1
```

## Local Development Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup Redis

```bash
# Start Redis locally
redis-server
```

### 3. Start Nodes

Terminal 1:
```bash
export NODE_ID=node-1 NODE_PORT=5000
python -m main
```

Terminal 2:
```bash
export NODE_ID=node-2 NODE_PORT=5001
python -m main
```

Terminal 3:
```bash
export NODE_ID=node-3 NODE_PORT=5002
python -m main
```

## Running Tests

### Unit Tests

```bash
pytest tests/unit/ -v
```

### Integration Tests

```bash
pytest tests/integration/ -v
```

### All Tests

```bash
pytest tests/ -v --cov=src
```

## Running Benchmarks

```bash
python benchmarks/load_test_scenarios.py
```

## Troubleshooting

### Issue: Nodes tidak bisa communicate

**Solution:**
- Check Docker network: `docker network ls`
- Check firewall settings
- Verify environment variables

### Issue: Memory usage tinggi

**Solution:**
- Reduce cache size: `CACHE_MAX_SIZE=5000`
- Reduce queue partitions: `QUEUE_NUM_PARTITIONS=5`
- Clean up old messages manually

### Issue: Deadlock detected

**Solution:**
- Check transaction isolation levels
- Review lock timeout settings
- Monitor lock acquisition patterns

## Scaling

### Add More Nodes

1. Update docker-compose.yml untuk add node-4, node-5, etc
2. Update configuration dengan peer lists
3. Rebuild dan restart

### Performance Tuning

```yaml
# docker-compose.yml adjustments
environment:
  RAFT_ELECTION_TIMEOUT_MIN: 1.0
  RAFT_ELECTION_TIMEOUT_MAX: 2.0
  RAFT_HEARTBEAT_INTERVAL: 0.3
  CACHE_MAX_SIZE: 20000
  QUEUE_NUM_PARTITIONS: 20
```

## Monitoring

### Prometheus Metrics

Available at: http://localhost:9090

Key metrics:
- `locks_granted`: Total locks granted
- `locks_waiting`: Locks currently waiting
- `cache_hit_rate`: Cache hit percentage
- `queue_throughput`: Messages per second

### Health Check

```bash
curl http://localhost:5000/health
```

## Cleanup

```bash
# Stop all containers
docker-compose down

# Remove volumes
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Support

Untuk masalah atau pertanyaan, lihat:
- docs/architecture.md
- docs/api_spec.yaml
- README.md
