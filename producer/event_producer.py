"""Synthetic clickstream producer (with a small share of malformed events)."""
import json
import logging
import os
import random
import signal
import time
import uuid

from confluent_kafka import Producer

EVENT_TYPES = ["click", "view", "purchase", "add_to_cart", "logout"]
EVENT_WEIGHTS = [50, 30, 5, 10, 5]

log = logging.getLogger("producer")


def generate_event() -> dict:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 10_000),
        "event_type": event_type,
        "amount": round(random.uniform(5, 500), 2) if event_type == "purchase" else None,
        "timestamp": time.time(),
    }


def generate_malformed() -> bytes:
    kind = random.choice(["garbage", "missing_field", "bad_type", "unknown_event"])
    event = generate_event()
    if kind == "garbage":
        return b"{this is not json"
    if kind == "missing_field":
        del event["user_id"]
    elif kind == "bad_type":
        event["user_id"] = "not-a-number"
    elif kind == "unknown_event":
        event["event_type"] = "teleport"
    return json.dumps(event).encode()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    topic = os.getenv("TOPIC", "user-events")
    per_tick = int(os.getenv("EVENTS_PER_TICK", "5"))
    tick = float(os.getenv("TICK_SECONDS", "0.5"))
    bad_rate = float(os.getenv("BAD_EVENT_RATE", "0.03"))

    producer = Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
        }
    )

    stats = {"ok": 0, "failed": 0}

    def on_delivery(err, _msg):
        stats["failed" if err else "ok"] += 1
        if err:
            log.error("delivery failed: %s", err)

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    last_log = time.time()
    while running:
        for _ in range(per_tick):
            if random.random() < bad_rate:
                key, value = None, generate_malformed()
            else:
                event = generate_event()
                key, value = str(event["user_id"]), json.dumps(event).encode()
            try:
                producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
            except BufferError:
                producer.poll(1)
        producer.poll(0)
        if time.time() - last_log >= 10:
            log.info("delivered=%d failed=%d", stats["ok"], stats["failed"])
            last_log = time.time()
        time.sleep(tick)

    producer.flush(10)
    log.info("shutdown complete: %s", stats)


if __name__ == "__main__":
    main()
