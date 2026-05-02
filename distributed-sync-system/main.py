import asyncio
import os
import uuid
import time as _time
from aiohttp import web

from prometheus_client import (
    Counter, Histogram, Gauge, generate_latest
)

from src.utils import setup_logger, load_config_from_env
from src.nodes import LockType, LockManager, NodeInfo, LockStatus
from src.consensus import RaftState

# ======================
# LOGGER
# ======================
logger = setup_logger(__name__)

# ======================
# GLOBAL STATE
# ======================
lock_manager = None

# ======================
# SHARED DISTRIBUTED STATE (SIMULATED)
# ======================
request_history = []
cache_store = {}
queue_store = []

# ======================
# PROMETHEUS METRICS
# ======================
lock_requests_total = Counter(
    'lock_requests_total',
    'Total lock requests',
    ['node_id', 'status']
)

lock_request_duration_seconds = Histogram(
    'lock_request_duration_seconds',
    'Lock request latency',
    ['node_id']
)

locks_held_gauge = Gauge(
    'locks_held_gauge',
    'Current locks held',
    ['node_id']
)

raft_state_gauge = Gauge(
    'raft_state_gauge',
    'Raft state (1=leader, 0=follower)',
    ['node_id']
)

# ======================
# HELPERS
# ======================
def add_history(resource, action, status):
    request_history.append({
        "resource": resource,
        "action": action,
        "status": status,
        "timestamp": _time.time()
    })


import json as _json


async def parse_body(request) -> dict:
    """
    Parse JSON body dari request.
    Menggunakan raw bytes untuk menghindari masalah encoding aiohttp.
    Handles: valid JSON, single-quote wrapped (Windows curl quirk).
    """
    # Read raw bytes — paling reliable
    body_bytes = await request.read()

    # Decode ke string
    try:
        raw = body_bytes.decode('utf-8').strip()
    except Exception:
        raw = body_bytes.decode('latin-1').strip()

    logger.info(f"[parse_body] raw bytes len={len(body_bytes)} | repr={repr(raw[:80])}")

    if not raw:
        raise ValueError("Request body kosong")

    # Strip outer single-quote (Windows curl.exe quirk: '{"key":"val"}')
    if len(raw) >= 2 and raw[0] == "'" and raw[-1] == "'":
        raw = raw[1:-1].strip()
        logger.info(f"[parse_body] stripped single-quotes → {repr(raw[:80])}")

    # Strip outer double-quote wrapping (edge case)
    if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
        inner = raw[1:-1]
        if inner.lstrip().startswith('{') or inner.lstrip().startswith('['):
            raw = inner.strip()

    result = _json.loads(raw)
    logger.info(f"[parse_body] parsed OK: keys={list(result.keys()) if isinstance(result, dict) else type(result)}")
    return result



# ======================
# BASIC ENDPOINTS
# ======================
async def health(request):
    return web.json_response({"status": "ok"})


async def metrics(request):
    global lock_manager

    if lock_manager:
        is_leader = 1 if lock_manager.raft_node.state == RaftState.LEADER else 0
        raft_state_gauge.labels(node_id=lock_manager.node_id).set(is_leader)

        total_locks = sum(len(v) for v in lock_manager.locks.values())
        locks_held_gauge.labels(node_id=lock_manager.node_id).set(total_locks)

    return web.Response(
        body=generate_latest(),
        content_type='text/plain'
    )


async def status(request):
    if not lock_manager:
        return web.json_response({
            "status": "starting"
        }, status=503)

    return web.json_response({
        "node_id": lock_manager.node_id,
        "raft_state": lock_manager.raft_node.state.name,
        "raft_state_value": lock_manager.raft_node.state.value,
        "is_leader": lock_manager.raft_node.state == RaftState.LEADER,
        "leader_id": lock_manager.raft_node.leader_id
    })


# ======================
# RAFT INTERNAL ENDPOINT
# Dipakai oleh leader untuk kirim heartbeat ke follower
# ======================
async def raft_append_entries(request):
    """Follower menerima heartbeat / log replication dari leader."""
    if not lock_manager:
        return web.json_response({"success": False, "reason": "not_ready"}, status=503)

    try:
        data = await request.json()
        response = lock_manager.raft_node.handle_append_entries(data)
        return web.json_response(response)
    except Exception as e:
        logger.error(f"raft_append_entries error: {e}")
        return web.json_response({"success": False, "error": str(e)}, status=500)


# ======================
# DISTRIBUTED LOCK (CORE)
# ======================
async def lock_resource(request):
    resource = request.match_info['resource']

    transaction_id = str(uuid.uuid4())
    client_id = "client-1"

    try:
        if request.can_read_body:
            body = await request.json()
            if isinstance(body, dict) and body.get("client_id"):
                client_id = str(body["client_id"])
    except Exception:
        pass

    start = _time.time()

    if not lock_manager:
        return web.json_response({"status": "error", "reason": "system_not_ready"}, status=503)

    if lock_manager.raft_node.state != RaftState.LEADER:
        return web.json_response({
            "status": "error",
            "reason": "not_leader",
            "leader_id": lock_manager.raft_node.leader_id
        }, status=503)

    lock_status, lock_obj = await lock_manager.acquire_lock(
        resource,
        LockType.EXCLUSIVE,
        transaction_id,
        client_id
    )

    duration = _time.time() - start
    lock_request_duration_seconds.labels(node_id=lock_manager.node_id).observe(duration)

    if lock_status == LockStatus.GRANTED:
        lock_requests_total.labels(node_id=lock_manager.node_id, status='success').inc()
        add_history(resource, "LOCK", "SUCCESS")

        return web.json_response({
            "status": "success",
            "resource": resource,
            "transaction_id": transaction_id,
            "message": "Lock granted via Raft consensus"
        })

    lock_requests_total.labels(node_id=lock_manager.node_id, status='failed').inc()
    add_history(resource, "LOCK", "FAILED")

    return web.json_response({
        "status": "failed",
        "reason": "lock_already_held",
        "resource": resource
    }, status=409)


async def unlock_resource(request):
    resource = request.match_info['resource']

    transaction_id = request.query.get("transaction_id")
    if not transaction_id:
        try:
            body = await request.json()
            if isinstance(body, dict):
                transaction_id = body.get("transaction_id")
        except Exception:
            transaction_id = None

    if not transaction_id:
        return web.json_response({
            "status": "error",
            "reason": "transaction_id_required"
        }, status=400)

    if not lock_manager:
        return web.json_response({"status": "error", "reason": "system_not_ready"}, status=503)

    if lock_manager.raft_node.state != RaftState.LEADER:
        return web.json_response({
            "status": "error",
            "reason": "not_leader",
            "leader_id": lock_manager.raft_node.leader_id
        }, status=503)

    success = await lock_manager.release_lock(
        resource,
        transaction_id,
        "client-1"
    )

    if success:
        add_history(resource, "UNLOCK", "SUCCESS")
        return web.json_response({
            "status": "released",
            "resource": resource,
            "transaction_id": transaction_id
        })

    return web.json_response({
        "status": "failed",
        "reason": "lock_not_found",
        "resource": resource
    }, status=404)


# ======================
# LOCK STATE
# ======================
async def get_locks(request):
    if not lock_manager:
        return web.json_response({"locks": {}})

    return web.json_response({
        "locks": {
            r: [
                {
                    "holder": l.holder_id,
                    "type": l.lock_type.value
                }
                for l in locks
            ]
            for r, locks in lock_manager.locks.items()
        }
    })


# ======================
# DISTRIBUTED QUEUE
# ======================
async def enqueue(request):
    global queue_store, lock_manager

    try:
        if not lock_manager:
            return web.json_response({
                "status": "error",
                "reason": "system_not_ready"
            }, status=503)

        # Only leader can write (single writer for consistency)
        if lock_manager.raft_node.state != RaftState.LEADER:
            return web.json_response({
                "status": "error",
                "reason": "not_leader",
                "leader_id": lock_manager.raft_node.leader_id
            }, status=503)

        # Parse body — strip PowerShell single-quote quirk then parse JSON
        message = None
        try:
            data = await parse_body(request)
            if isinstance(data, dict):
                message = data.get("message") or data.get("msg") or data.get("data")
            elif isinstance(data, str):
                message = data
        except Exception as parse_err:
            logger.warning(f"Body parse failed: {parse_err}")
            message = None

        if not message:
            return web.json_response({
                "status": "error",
                "reason": "message_required — body: {\"message\": \"your text\"}"
            }, status=400)

        queue_id = str(uuid.uuid4())
        queue_store.append({
            "id": queue_id,
            "message": message
        })

        add_history("queue", "ENQUEUE", "SUCCESS")

        return web.json_response({
            "status": "queued",
            "message_id": queue_id,
            "queue_size": len(queue_store),
            "message": "Message stored successfully"
        })

    except Exception as e:
        logger.error(f"enqueue error: {e}", exc_info=True)
        add_history("queue", "ENQUEUE", "FAILED")
        return web.json_response({
            "status": "error",
            "message": str(e)
        }, status=500)


async def dequeue(request):
    global queue_store

    try:
        if len(queue_store) == 0:
            return web.json_response({
                "status": "empty",
                "queue_size": 0,
                "message": None
            })

        item = queue_store.pop(0)
        add_history("queue", "DEQUEUE", "SUCCESS")

        return web.json_response({
            "status": "consumed",
            "message": item.get("message"),
            "message_id": item.get("id"),
            "remaining": len(queue_store)
        })

    except Exception as e:
        logger.error(f"dequeue error: {e}", exc_info=True)
        add_history("queue", "DEQUEUE", "FAILED")
        return web.json_response({
            "status": "error",
            "message": str(e)
        }, status=500)


async def queue_status(request):
    return web.json_response({
        "status": "ok",
        "queue_size": len(queue_store),
        "pending_items": queue_store[-10:] if queue_store else []
    })


# ======================
# CACHE COHERENCE
# ======================
async def set_cache(request):
    global lock_manager

    if not lock_manager:
        return web.json_response({
            "status": "error",
            "reason": "system_not_ready"
        }, status=503)

    # Leader-only write for coherence consistency
    if lock_manager.raft_node.state != RaftState.LEADER:
        return web.json_response({
            "status": "error",
            "reason": "not_leader",
            "leader_id": lock_manager.raft_node.leader_id
        }, status=503)

    try:
        data = await parse_body(request)
    except Exception:
        return web.json_response({
            "status": "error",
            "reason": "invalid_json — contoh body: {\"key\": \"user1\", \"value\": \"abc\"}"
        }, status=400)

    key = data.get("key") if isinstance(data, dict) else None
    value = data.get("value") if isinstance(data, dict) else None

    if not key:
        return web.json_response({
            "status": "error",
            "reason": "key_required"
        }, status=400)

    cache_store[str(key)] = value
    add_history("cache", "SET", "SUCCESS")
    return web.json_response({"status": "updated", "key": str(key)})


async def get_cache(request):
    key = request.match_info['key']
    add_history("cache", "GET", "SUCCESS")
    return web.json_response({
        "key": key,
        "value": cache_store.get(key)
    })


# ======================
# HISTORY (AUDIT TRAIL)
# ======================
async def get_history(request):
    # Return last 50 entries, all values are JSON-safe
    return web.json_response(request_history[-50:])


# ======================
# RAFT DEBUG
# ======================
async def raft_info(request):
    if not lock_manager:
        return web.json_response({"error": "not ready"}, status=503)
    return web.json_response({
        "node": lock_manager.node_id,
        "state": lock_manager.raft_node.state.name,
        "leader": lock_manager.raft_node.leader_id,
        "term": lock_manager.raft_node.current_term,
        "log_length": len(lock_manager.raft_node.log),
    })


# ======================
# LIFECYCLE
# ======================
async def on_shutdown(app):
    global lock_manager
    if lock_manager:
        await lock_manager.stop()
        logger.info("LockManager stopped")


async def on_startup(app):
    global lock_manager

    config = load_config_from_env()

    node = NodeInfo(
        node_id=config.node_config.node_id,
        host=config.node_config.host,
        port=config.node_config.port,
        cluster_name=config.node_config.cluster_name
    )

    node_id = node.node_id

    # Inside Docker: nodes talk via container name on port 8000 (internal)
    # Outside Docker (local dev): use localhost with mapped ports
    in_docker = os.path.exists("/.dockerenv")

    if in_docker:
        port_map = {
            "node-1": 8000,
            "node-2": 8000,
            "node-3": 8000,
        }
        # Build peer URLs with proper container hostnames
        peer_host_map = {
            "node-1": "node-1",
            "node-2": "node-2",
            "node-3": "node-3",
        }
    else:
        port_map = {
            "node-1": 8001,
            "node-2": 8002,
            "node-3": 8003,
        }
        peer_host_map = {
            "node-1": "localhost",
            "node-2": "localhost",
            "node-3": "localhost",
        }

    peers = [n for n in port_map.keys() if n != node_id]

    lock_manager = LockManager(
        node_info=node,
        peers=peers,
        port_map=port_map,
        peer_host_map=peer_host_map,
    )

    await lock_manager.start()

    logger.info(f"Node {node_id} started | in_docker={in_docker} | leader={lock_manager.raft_node.state.name}")


# ======================
# APP
# ======================
def create_app():
    app = web.Application()

    # core
    app.router.add_get('/health', health)
    app.router.add_get('/metrics', metrics)
    app.router.add_get('/status', status)

    # raft internal (for inter-node heartbeat)
    app.router.add_post('/raft/append', raft_append_entries)
    app.router.add_get('/raft', raft_info)

    # lock
    app.router.add_post('/lock/{resource}', lock_resource)
    app.router.add_delete('/lock/{resource}', unlock_resource)
    app.router.add_post('/unlock/{resource}', unlock_resource)
    app.router.add_get('/locks', get_locks)

    # queue
    app.router.add_post('/queue', enqueue)
    app.router.add_get('/queue', dequeue)
    app.router.add_get('/queue/status', queue_status)

    # cache
    app.router.add_post('/cache', set_cache)
    app.router.add_get('/cache/{key}', get_cache)

    # extras
    app.router.add_get('/history', get_history)

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    return app


# ======================
# RUN
# ======================
if __name__ == "__main__":
    web.run_app(create_app(), host="0.0.0.0", port=8000)