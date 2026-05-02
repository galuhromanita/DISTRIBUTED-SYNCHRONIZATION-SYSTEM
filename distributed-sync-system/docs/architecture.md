# Arsitektur Distributed Synchronization System

## Overview

Sistem ini mengimplementasikan distributed synchronization menggunakan beberapa komponen:

```
┌─────────────────────────────────────────────────────┐
│   Distributed Synchronization System                │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌──────────────────────────────────────────────┐  │
│  │         Application Layer                    │  │
│  │  (Clients, APIs, Applications)               │  │
│  └────────────────┬─────────────────────────────┘  │
│                   │                                │
│  ┌────────────────▼─────────────────────────────┐  │
│  │      Distributed System Components           │  │
│  │                                               │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐   │  │
│  │  │   Lock   │  │  Queue   │  │  Cache   │   │  │
│  │  │ Manager  │  │   Node   │  │   Node   │   │  │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘   │  │
│  │       │             │             │          │  │
│  │       └─────────────┼─────────────┘          │  │
│  │                     │                        │  │
│  └────────────────┬────┴────────────────────────┘  │
│                   │                                │
│  ┌────────────────▼─────────────────────────────┐  │
│  │         Consensus & Coordination             │  │
│  │                                               │  │
│  │  ┌───────────────────────────────────────┐   │  │
│  │  │    Raft Consensus Algorithm           │   │  │
│  │  │  (Leader Election, Log Replication)   │   │  │
│  │  └───────────────────────────────────────┘   │  │
│  │                                               │  │
│  │  ┌───────────────────────────────────────┐   │  │
│  │  │    Failure Detector                   │   │  │
│  │  │  (Heartbeat, Node Status Monitoring)  │   │  │
│  │  └───────────────────────────────────────┘   │  │
│  │                                               │  │
│  └────────────────┬─────────────────────────────┘  │
│                   │                                │
│  ┌────────────────▼─────────────────────────────┐  │
│  │      Communication Layer                     │  │
│  │                                               │  │
│  │  ┌──────────────┐  ┌──────────────────────┐  │  │
│  │  │   Message    │  │   Network Interface  │  │  │
│  │  │   Passing    │  │   (asyncio/gRPC)     │  │  │
│  │  └──────────────┘  └──────────────────────┘  │  │
│  │                                               │  │
│  └────────────────┬─────────────────────────────┘  │
│                   │                                │
│  ┌────────────────▼─────────────────────────────┐  │
│  │      Data Layer & Storage                    │  │
│  │                                               │  │
│  │  ┌──────────────┐  ┌──────────────────────┐  │  │
│  │  │    Redis     │  │   Local Storage      │  │  │
│  │  │  (Shared)    │  │   (Persistence)      │  │  │
│  │  └──────────────┘  └──────────────────────┘  │  │
│  │                                               │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
└─────────────────────────────────────────────────────┘
```

## Komponen Utama

### 1. Lock Manager

**Fungsi:** Manage distributed locks dengan support untuk shared dan exclusive locks.

**Key Features:**
- Raft Consensus untuk consistency
- Deadlock detection dan resolution
- Automatic timeout handling
- Transaction tracking

**Data Structure:**
```
Locks: {
  resource_id -> [Lock objects]
}

Lock Object: {
  resource_id: str
  lock_type: EXCLUSIVE | SHARED
  holder_id: str
  transaction_id: str
  acquired_at: timestamp
  timeout: seconds
}

WaitingQueue: {
  resource_id -> [waiting requests]
}
```

### 2. Queue Node

**Fungsi:** Distributed message queue dengan at-least-once delivery.

**Key Features:**
- Consistent hashing untuk partitioning
- Replication untuk fault tolerance
- Message persistence
- Consumer offset tracking

**Data Structure:**
```
Queues: {
  queue_name -> deque of messages
}

Message: {
  message_id: uuid
  content: any
  producer_id: str
  timestamp: timestamp
  delivered_to: {consumer_id -> bool}
  retry_count: int
}
```

### 3. Cache Node

**Fungsi:** Distributed cache dengan MESI coherence protocol.

**Key Features:**
- MESI protocol untuk cache coherence
- LRU/LFU replacement policies
- Automatic invalidation
- Performance monitoring

**Cache Line State Machine:**
```
         MODIFIED (M)
            ^   |
            |   v
        SHARED (S)
            ^   |
            |   v
       EXCLUSIVE (E)
            ^   |
            |   v
        INVALID (I)

Transitions:
- EXCLUSIVE -> SHARED (read from another CPU)
- MODIFIED -> SHARED (snoop invalidate)
- Any -> INVALID (explicit invalidation)
```

### 4. Raft Consensus

**Fungsi:** Ensure agreement antara nodes untuk decisions.

**Key Properties:**
- Leader election
- Log replication
- State machine safety

**State Transitions:**
```
           timeout
        ┌─────────┐
        │         v
   FOLLOWER ──→ CANDIDATE
   ^            │   │
   │            │   │ votes < quorum
   │ higher     │   v
   │ term   win election
   │            LEADER
   └────────────────┘
```

## Algoritma Utama

### 1. Raft Consensus

**Election Process:**
1. Follower waits untuk heartbeat
2. Jika timeout, become CANDIDATE
3. Send RequestVote ke all peers
4. Grant vote jika:
   - No vote yet di term ini
   - Candidate's log sama atau lebih up-to-date
5. Become LEADER jika dapat quorum votes

**Log Replication:**
1. Leader menerima command dari client
2. Append ke own log
3. Send AppendEntries ke followers
4. Followers append dan acknowledge
5. Leader commits ketika majority acknowledge

### 2. Deadlock Detection

**Wait-for Graph:**
- Node = Transaction
- Edge: Txn A -> Txn B (A menunggu lock yg dipegang B)

**Cycle Detection:**
- Use DFS untuk detect cycles
- Jika cycle terdeteksi, abort victim transaction
- Victim = transaction dengan earliest timestamp

### 3. Consistent Hashing

**Purpose:** Minimize data redistribution saat node join/leave

**Formula:**
```
position = hash(key) mod ring_size
assigned_node = first node >= position pada ring
```

**Advantages:**
- Adding node: hanya ~1/n data perlu dipindahkan
- Removing node: keys redistributed ke adjacent nodes
- Virtual nodes: balance load distribution

### 4. MESI Protocol

**States:**
- **Modified (M)**: Only this cache has valid copy, modified
- **Exclusive (E)**: Only this cache has valid copy, clean
- **Shared (S)**: Multiple caches have valid copy
- **Invalid (I)**: Cache line invalid

**Operations:**
- **Local Read**: M/E/S -> use value, S -> remain S
- **Local Write**: * -> M (invalidate others)
- **Remote Read**: M -> S, E -> S
- **Remote Write**: M/E/S -> I (invalidate)

## Network Communication

```
Node A                          Node B
   │                               │
   │──────── TCP Connection ───────│
   │                               │
   └──── Message Passing (JSON) ───┘
   │                               │
   └──── Heartbeat (periodic) ─────┘
   │                               │
   └──── Failure Detection ────────┘
```

## Data Consistency

### Eventual Consistency Model

1. **Write Priority**: Writes always succeed pada leader
2. **Read Consistency**: Reads dari cache mungkin stale
3. **Consistency Verification**: Periodic reconciliation
4. **Conflict Resolution**: Last-write-wins

### Transaction Semantics

```
BEGIN TRANSACTION
  ├─ ACQUIRE lock (shared/exclusive)
  ├─ READ/WRITE data
  ├─ [DEADLOCK DETECTION]
  └─ END TRANSACTION
      ├─ RELEASE lock
      └─ UPDATE cache coherence
```

## Performance Characteristics

### Lock Manager
- **Acquire Lock**: O(log n) lookups + Raft consensus
- **Release Lock**: O(1) removal + cascade grant
- **Deadlock Detection**: O(V + E) cycle detection

### Queue
- **Enqueue**: O(1) append + replication
- **Dequeue**: O(1) removal
- **Replication**: O(R) where R = replication factor

### Cache
- **Get**: O(1) lookup + state check
- **Put**: O(1) write + invalidation broadcast
- **Eviction**: O(log n) untuk policy (LRU/LFU)

## Failure Modes & Recovery

### Node Failure
```
ALIVE → SUSPECTED (timeout > heartbeat_timeout)
     → DEAD (timeout > suspected_timeout)
```

**Recovery:**
- Heartbeat received → back to ALIVE
- Replica nodes takeover
- Leader re-election jika leader fails

### Network Partition
```
Leader dapat communicate dengan majority
  → Continue operating (consensus works)

Leader tidak bisa reach majority
  → Become FOLLOWER
  → New leader elected dalam healthy partition
```

### Lock Deadlock
```
Detect: Cycle dalam wait-for graph
Resolve: Abort victim (youngest txn)
Prevent: Timeout + backoff strategy
```

## Scalability

### Horizontal Scaling
- Add nodes ke cluster
- Consistent hash redistribute
- No downtime rebalancing

### Vertical Scaling
- Increase cache size
- More queue partitions
- Larger timeouts

### Bottlenecks
- Single leader untuk consensus
- Network bandwidth untuk replication
- Cache coherence traffic
