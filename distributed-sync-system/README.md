# Distributed Sync System

Sistem terdistribusi (3 node) dengan **Raft (leader/follower)** yang menyediakan:
- Distributed lock (mutual exclusion)
- Distributed queue (leader-only enqueue, dequeue bisa dari node mana pun)
- Cache coherence sederhana (write leader-only, read anywhere)
- Monitoring (Prometheus + Grafana)


## Quick Start (Docker)

### Prerequisites
- Docker Desktop

### Start

Jalankan dari root project:

```powershell
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps
```

Kalau kamu masih pakai `docker-compose` (legacy), bisa:

```powershell
docker-compose -f docker/docker-compose.yml up -d
docker-compose -f docker/docker-compose.yml ps
```

Ports (host):
- node-1 API: http://localhost:8001
- node-2 API: http://localhost:8002
- node-3 API: http://localhost:8003


## Testing

Catatan penting:
- Semua command dibuat **1 baris** (aman untuk PowerShell)

### 1) Container

Command:

```powershell
docker ps
```


### 2) Distributed Lock 

#### Test 1: Lock success (Leader)

Command:

```powershell
curl.exe -X POST "http://localhost:8001/lock/resource1"
```

Expected output (contoh):

```json
{
  "status": "success",
  "resource": "resource1",
  "transaction_id": "...",
  "message": "Lock granted via Raft consensus"
}
```


#### Test 2: Mutual exclusion (lock kedua ditolak)

Command (ulang command yang sama):

```powershell
curl.exe -X POST "http://localhost:8001/lock/resource1"
```

Expected output (contoh):

```json
{
  "status": "failed",
  "reason": "lock_already_held",
  "resource": "resource1"
}
```


### 3) Raft Leadership (follower tidak boleh memutuskan)

Command:

```powershell
curl.exe -X POST "http://localhost:8002/lock/resource2"
```

Expected output (contoh):

```json
{
  "status": "error",
  "reason": "not_leader",
  "leader_id": "node-1"
}
```

### 4) Distributed State Check (status 3 node)

Node-1:

```powershell
curl.exe "http://localhost:8001/status"
```

Node-2:

```powershell
curl.exe "http://localhost:8002/status"
```

Node-3:

```powershell
curl.exe "http://localhost:8003/status"
```

Expected (contoh):

```json
{
  "node_id": "node-1",
  "raft_state": "LEADER",
  "is_leader": true,
  "leader_id": "node-1"
}
```

### 5) Distributed Queue

#### Producer (enqueue dari leader)

```powershell
curl.exe -X POST "http://localhost:8001/queue" -H "Content-Type: application/json" -d "{\"message\":\"Hello Distributed System\"}"
```

Expected (contoh):

```json
{
  "status": "queued",
  "message_id": "...",
  "queue_size": 1,
  "message": "Message stored successfully"
}
```

#### Consumer (dequeue dari node lain)

```powershell
curl.exe "http://localhost:8002/queue"
```

Expected (contoh):

```json
{
  "status": "consumed",
  "message": "Hello Distributed System",
  "message_id": "...",
  "remaining": 0
}
```


### 6) Cache Coherence

#### Update cache (leader-only)

```powershell
curl.exe -X POST "http://localhost:8001/cache" -H "Content-Type: application/json" -d "{\"key\":\"user1\",\"value\":\"updated\"}"
```

Expected (contoh):

```json
{
  "status": "updated",
  "key": "user1"
}
```

####  Read dari node lain

```powershell
curl.exe "http://localhost:8002/cache/user1"
```

Expected (contoh):

```json
{
  "key": "user1",
  "value": "updated"
}
```


### 7) Failure Simulation

Matikan node-2:

```powershell
docker stop distributed-sync-node-2
docker ps
```

Tes sistem masih hidup (leader masih aktif):

```powershell
curl.exe -X POST "http://localhost:8001/lock/resourceX"
```


```powershell
docker start distributed-sync-node-2
```

---

### 8) Performance Test 
```powershell
python benchmarks/load_test_scenarios.py
```

Yang ditunjukkan:
- Throughput
- Latency (p95 / p99)

---


## Performance Benchmarks

```bash
# Run benchmarks
python benchmarks/load_test_scenarios.py
```

Expected results:
- Lock acquisition: ~1000-2000 ops/sec
- Queue throughput: ~5000-10000 msgs/sec
- Cache hit rate: 95%+



Galuh Juliviana Romanita
Sistem Parallel dan Terdistribusi
Tugas: Implementasi Distributed Synchronization System
