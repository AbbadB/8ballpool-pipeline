"""Synthetic 8 Ball Pool event producer.

Guarantees `init` is sent before any match/purchase for a user (brief: init is
the first event of all). Validates each event against its schema before sending.
Publishes JSON to topic `events.raw`. Bootstrap server from $KAFKA_BOOTSTRAP.
"""
import json
import os
import random
import time

from eightball.events import make_init, make_match, make_purchase
from eightball.schemas import validate_event
from kafka import KafkaProducer

# In Docker, services set KAFKA_BOOTSTRAP=kafka:9092 (internal listener).
# On the host, the default uses the external listener on localhost:29092.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = "events.raw"
COUNTRIES = ["1", "2", "3", "4", "5"]
PLATFORMS = ["ios", "android", "web"]
PRODUCTS = ["coins_100", "coins_500", "cue_gold", "spin_pack"]


def now_ms() -> int:
    return int(time.time() * 1000)


def send(producer, event):
    validate_event(event)                      # fail fast on bad data
    producer.send(TOPIC, event)


def run(n_users=20, rate_per_sec=10):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    seen = set()
    while True:
        uid = f"u{random.randint(1, n_users)}"
        if uid not in seen:                    # init must come first
            send(producer, make_init(user_id=uid, country_id=random.choice(COUNTRIES),
                                     platform=random.choice(PLATFORMS), time_ms=now_ms()))
            seen.add(uid)
            continue
        if random.random() < 0.5:
            other = f"u{random.randint(1, n_users)}"
            send(producer, make_match(user_a=uid, user_b=other,
                                      winner=random.choice([uid, other]),
                                      time_ms=now_ms(),
                                      platform=random.choice(PLATFORMS)))
        else:
            send(producer, make_purchase(user_id=uid,
                                         value=round(random.uniform(0.99, 49.99), 2),
                                         product_id=random.choice(PRODUCTS),
                                         time_ms=now_ms()))
        time.sleep(1.0 / rate_per_sec)


if __name__ == "__main__":
    run()
