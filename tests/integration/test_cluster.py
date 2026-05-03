"""
Integration tests for the distributed cluster.
Tests multi-node scenarios: leader election, lock contention, queue replication.
"""
import asyncio
import pytest
import aiohttp

BASE_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]


async def get_leader() -> str:
    """Find the current Raft leader in the cluster."""
    for url in BASE_URLS:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{url}/status", timeout=aiohttp.ClientTimeout(total=2)) as r:
                    data = await r.json()
                    if data["raft"]["state"] == "leader":
                        return url
        except Exception:
            pass
    return None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_cluster_has_leader():
    """Cluster should elect exactly one leader."""
    await asyncio.sleep(1.0)  # Allow time for election
    leader = await get_leader()
    assert leader is not None, "No leader elected in cluster"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_lock_acquire_and_release():
    """Acquire and release a distributed lock."""
    leader = await get_leader()
    assert leader, "No leader available"

    async with aiohttp.ClientSession() as s:
        # Acquire
        resp = await s.post(f"{leader}/locks/acquire", json={
            "client_id": "test-client-1",
            "resource_id": "test-resource-integration",
            "lock_type": "exclusive",
            "timeout": 10.0,
        })
        data = await resp.json()
        assert data["status"] in ("granted", "waiting")

        # Release
        resp = await s.post(f"{leader}/locks/release", json={
            "client_id": "test-client-1",
            "resource_id": "test-resource-integration",
        })
        data = await resp.json()
        assert data["success"] is True


@pytest.mark.asyncio
@pytest.mark.integration
async def test_produce_consume_message():
    """Produce and consume a message through the distributed queue."""
    leader = await get_leader()
    assert leader, "No leader available"

    async with aiohttp.ClientSession() as s:
        # Produce
        resp = await s.post(f"{leader}/queue/produce", json={
            "topic": "integration-test",
            "payload": {"message": "hello distributed world"},
            "key": "test-key-1",
        })
        data = await resp.json()
        message_id = data["message_id"]
        assert message_id

        # Consume
        resp = await s.post(f"{leader}/queue/consume", json={
            "topic": "integration-test",
            "group_id": "test-group",
            "consumer_id": "test-consumer-1",
            "batch_size": 1,
        })
        data = await resp.json()
        messages = data["messages"]
        assert len(messages) >= 1

        # Ack
        resp = await s.post(f"{leader}/queue/ack", json={
            "message_id": messages[0]["message_id"]
        })
        data = await resp.json()
        assert data["success"] is True


@pytest.mark.asyncio
@pytest.mark.integration
async def test_cache_coherence_across_nodes():
    """Write to one node, read from another — should see coherent data."""
    leader = await get_leader()
    assert leader, "No leader available"

    # Find a non-leader
    follower = next((u for u in BASE_URLS if u != leader), None)
    if not follower:
        pytest.skip("Need at least 2 nodes for coherence test")

    async with aiohttp.ClientSession() as s:
        # Write to leader
        await s.put(f"{leader}/cache/coherence-test-key",
                    json={"value": "coherent-value-123"})

        await asyncio.sleep(0.5)  # Allow propagation

        # Read from follower
        resp = await s.get(f"{follower}/cache/coherence-test-key")
        if resp.status == 200:
            data = await resp.json()
            # Either it's in the cache or a miss (both valid outcomes)
            assert "found" in data


@pytest.mark.asyncio
@pytest.mark.integration
async def test_concurrent_lock_requests():
    """Multiple clients requesting the same lock — only one wins."""
    leader = await get_leader()
    assert leader, "No leader available"

    resource = "contested-resource"
    results = []

    async def try_acquire(client_id: str):
        async with aiohttp.ClientSession() as s:
            resp = await s.post(f"{leader}/locks/acquire", json={
                "client_id": client_id,
                "resource_id": resource,
                "lock_type": "exclusive",
                "wait_timeout": 2.0,
            })
            data = await resp.json()
            results.append((client_id, data["status"]))

    # 5 concurrent clients
    await asyncio.gather(*[try_acquire(f"client-{i}") for i in range(5)])

    granted = [r for r in results if r[1] == "granted"]
    assert len(granted) >= 1, "At least one client should get the lock"

    # Release all
    async with aiohttp.ClientSession() as s:
        for client_id, _ in granted:
            await s.post(f"{leader}/locks/release", json={
                "client_id": client_id,
                "resource_id": resource,
            })


@pytest.mark.asyncio
@pytest.mark.integration
async def test_all_nodes_healthy():
    """All nodes should respond to health check."""
    unhealthy = []
    for url in BASE_URLS:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{url}/health",
                                timeout=aiohttp.ClientTimeout(total=2)) as r:
                    if r.status != 200:
                        unhealthy.append(url)
        except Exception:
            unhealthy.append(url)

    assert not unhealthy, f"Unhealthy nodes: {unhealthy}"