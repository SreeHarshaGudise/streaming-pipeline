"""Synthetic clickstream producer."""
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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    topic = os.getenv("TOPIC", "user-events")
    per_tick = int(os.getenv("EVENTS_PER_TICK", "5"))
    tick = float(os.getenv("TICK_SECONDS", "0.5"))

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
            event = generate_event()
            try:
                producer.produce(
                    topic,
                    key=str(event["user_id"]),
                    value=json.dumps(event).encode(),
                    on_delivery=on_delivery,
                )
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
