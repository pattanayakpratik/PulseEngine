import random
import uuid
from locust import HttpUser, task, between


class PulseEngineLoadTester(HttpUser):
    wait_time = between(0.001, 0.05)  # High RPS stress testing

    headers = {
        "X-API-Key": "pulse_live_test_api_key_884920",
        "Content-Type": "application/json",
    }

    @task(10)
    def send_critical_notification(self):
        """Simulates high-priority transactional OTP or security alert dispatch."""
        idemp_key = str(uuid.uuid4())
        req_headers = {**self.headers, "Idempotency-Key": idemp_key}
        payload = {
            "external_user_id": f"usr_{random.randint(1, 100000)}",
            "channel": random.choice(["FCM", "WHATSAPP", "EMAIL"]),
            "priority": "CRITICAL",
            "template_id": "otp_verification_v2",
            "variables": {
                "otp_code": str(random.randint(100000, 999999)),
                "expiry_min": 5,
            },
        }
        self.client.post("/v1/notify", json=payload, headers=req_headers)

    @task(3)
    def preview_dynamic_segment(self):
        """Simulates product managers testing live segment filter ASTs."""
        payload = {
            "filter_ast": {
                "logic": "AND",
                "rules": [
                    {"field": "country", "op": "eq", "value": random.choice(["IN", "US", "GB", "DE"])},
                    {"field": "metadata.lifetime_spend", "op": "gte", "value": random.choice([500, 1000, 5000])},
                ],
            },
            "limit": 10,
        }
        self.client.post("/v1/segments/preview", json=payload, headers=self.headers)

    @task(2)
    def get_metrics_snapshot(self):
        """Polls queue depths and circuit breaker status."""
        self.client.get("/v1/metrics/summary")
