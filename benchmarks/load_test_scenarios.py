"""
Load testing scenarios using Locust.
Tests throughput and latency for all system components.

Run with:
    locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8001
"""
import json
import random
import string
import time
from locust import HttpUser, task, between, events
from locust.runners import MasterRunner
import logging

logger = logging.getLogger(__name__)


def random_key(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def random_payload(size_bytes: int = 256) -> dict:
    return {
        "data": "x" * size_bytes,
        "timestamp": time.time(),
        "id": random_key(16),
    }


class LockManagerUser(HttpUser):
    """
    Simulates clients competing for distributed locks.
    Models realistic read/write ratio (80% shared, 20% exclusive).
    """
    wait_time = between(0.01, 0.1)
    weight = 3

    def on_start(self):
        self.client_id = f"locust-lock-{random_key(8)}"
        self.held_locks = []

    @task(4)
    def acquire_shared_lock(self):
        resource = f"resource-{random.randint(1, 20)}"
        with self.client.post(
            "/locks/acquire",
            json={
                "client_id": self.client_id,
                "resource_id": resource,
                "lock_type": "shared",
                "timeout": 5.0,
                "wait_timeout": 1.0,
            },
            name="/locks/acquire [shared]",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data["status"] == "granted":
                    self.held_locks.append(("shared", resource))
                    resp.success()
                else:
                    resp.success()  # Waiting/denied is also valid
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def acquire_exclusive_lock(self):
        resource = f"resource-{random.randint(1, 5)}"  # More contention
        with self.client.post(
            "/locks/acquire",
            json={
                "client_id": self.client_id,
                "resource_id": resource,
                "lock_type": "exclusive",
                "timeout": 5.0,
                "wait_timeout": 2.0,
            },
            name="/locks/acquire [exclusive]",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                if data["status"] == "granted":
                    self.held_locks.append(("exclusive", resource))
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(5)
    def release_lock(self):
        if not self.held_locks:
            return
        lock_type, resource = self.held_locks.pop()
        self.client.post(
            "/locks/release",
            json={"client_id": self.client_id, "resource_id": resource},
            name="/locks/release",
        )

    def on_stop(self):
        # Release all held locks
        for _, resource in self.held_locks:
            self.client.post(
                "/locks/release",
                json={"client_id": self.client_id, "resource_id": resource},
            )


class QueueUser(HttpUser):
    """
    Simulates producers and consumers on the distributed queue.
    Ratio: 2 produces per consume.
    """
    wait_time = between(0.005, 0.05)
    weight = 4

    def on_start(self):
        self.producer_id = f"locust-producer-{random_key(8)}"
        self.consumer_id = f"locust-consumer-{random_key(8)}"
        self.topics = ["orders", "events", "notifications", "logs"]
        self.pending_acks = []

    @task(3)
    def produce_message(self):
        topic = random.choice(self.topics)
        with self.client.post(
            "/queue/produce",
            json={
                "topic": topic,
                "payload": random_payload(random.choice([128, 512, 1024])),
                "key": random_key(16),
            },
            name="/queue/produce",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                self.pending_acks.append(data.get("message_id"))
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(2)
    def consume_messages(self):
        topic = random.choice(self.topics)
        with self.client.post(
            "/queue/consume",
            json={
                "topic": topic,
                "group_id": "locust-group",
                "consumer_id": self.consumer_id,
                "batch_size": random.randint(1, 5),
            },
            name="/queue/consume",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                messages = resp.json().get("messages", [])
                for msg in messages:
                    self.pending_acks.append(msg["message_id"])
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(2)
    def acknowledge_messages(self):
        if not self.pending_acks:
            return
        # Ack a batch
        batch = self.pending_acks[:5]
        self.pending_acks = self.pending_acks[5:]
        for msg_id in batch:
            self.client.post(
                "/queue/ack",
                json={"message_id": msg_id},
                name="/queue/ack",
            )


class CacheUser(HttpUser):
    """
    Simulates cache read/write workload.
    Zipfian distribution — some keys much more popular than others.
    """
    wait_time = between(0.001, 0.02)
    weight = 5

    def on_start(self):
        self.hot_keys = [f"hot-key-{i}" for i in range(10)]
        self.cold_keys = [f"cold-key-{random_key(12)}" for _ in range(100)]
        # Pre-populate hot keys
        for key in self.hot_keys[:3]:
            self.client.put(f"/cache/{key}", json={"value": f"initial-{key}"})

    @task(7)
    def cache_read(self):
        # 80% hot keys, 20% cold (simulates zipfian)
        if random.random() < 0.8:
            key = random.choice(self.hot_keys)
        else:
            key = random.choice(self.cold_keys)

        with self.client.get(
            f"/cache/{key}",
            name="/cache/[key] [read]",
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 404):
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(2)
    def cache_write(self):
        if random.random() < 0.5:
            key = random.choice(self.hot_keys)
        else:
            key = f"write-key-{random_key(8)}"

        with self.client.put(
            f"/cache/{key}",
            json={"value": random_payload(64)},
            name="/cache/[key] [write]",
            catch_response=True,
        ) as resp:
            if resp.status_code == 200:
                resp.success()
            else:
                resp.failure(f"HTTP {resp.status_code}")

    @task(1)
    def cache_invalidate(self):
        key = random.choice(self.cold_keys)
        self.client.delete(f"/cache/{key}", name="/cache/[key] [invalidate]")


class MixedWorkloadUser(HttpUser):
    """
    Mixed workload simulating a realistic application.
    """
    wait_time = between(0.01, 0.1)
    weight = 2

    def on_start(self):
        self.session_id = random_key(16)

    @task(1)
    def health_check(self):
        self.client.get("/health", name="/health")

    @task(2)
    def full_transaction(self):
        """Simulate a complete distributed transaction."""
        resource = f"tx-resource-{random.randint(1, 50)}"
        topic = "transactions"

        # 1. Acquire lock
        resp = self.client.post(
            "/locks/acquire",
            json={
                "client_id": self.session_id,
                "resource_id": resource,
                "lock_type": "exclusive",
                "wait_timeout": 1.0,
            },
            name="/full-tx/lock-acquire",
        )

        if resp.status_code != 200 or resp.json().get("status") != "granted":
            return

        # 2. Read cache
        self.client.get(f"/cache/{resource}", name="/full-tx/cache-read")

        # 3. Produce event
        self.client.post(
            "/queue/produce",
            json={
                "topic": topic,
                "payload": {"resource": resource, "session": self.session_id},
                "key": resource,
            },
            name="/full-tx/queue-produce",
        )

        # 4. Write cache
        self.client.put(
            f"/cache/{resource}",
            json={"value": {"updated": True, "by": self.session_id}},
            name="/full-tx/cache-write",
        )

        # 5. Release lock
        self.client.post(
            "/locks/release",
            json={"client_id": self.session_id, "resource_id": resource},
            name="/full-tx/lock-release",
        )


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    logger.info("Load test starting...")
    logger.info(f"Target: {environment.host}")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats
    logger.info("=== Load Test Results ===")
    logger.info(f"Total requests: {stats.total.num_requests}")
    logger.info(f"Total failures: {stats.total.num_failures}")
    logger.info(f"Avg response time: {stats.total.avg_response_time:.2f}ms")
    logger.info(f"P95 response time: {stats.total.get_response_time_percentile(0.95):.2f}ms")
    logger.info(f"P99 response time: {stats.total.get_response_time_percentile(0.99):.2f}ms")
    logger.info(f"RPS: {stats.total.current_rps:.2f}")