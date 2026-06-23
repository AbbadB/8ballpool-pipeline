"""Consume raw events, apply the generic DQ rule set, produce clean events.

This is intentionally a *pure column transformer* (uppercase, id->name). It does
NOT do the country enrichment join for match/purchase -- that is relational work
left to Spark (ADR-0001). Consumes `events.raw`, produces `events.clean`.
"""
import json
import os

from kafka import KafkaConsumer, KafkaProducer

from eightball.dq.config import rules_for
from eightball.dq.rules import apply_rules

# In Docker, services set KAFKA_BOOTSTRAP=kafka:9092 (internal listener).
# On the host, the default uses the external listener on localhost:29092.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
IN, OUT = "events.raw", "events.clean"


def run():
    consumer = KafkaConsumer(
        IN, bootstrap_servers=BOOTSTRAP, group_id="dq-app",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for msg in consumer:                       # at-least-once: see ADR-0002
        event = msg.value
        clean = apply_rules(event, rules_for(event.get("event-type")))
        producer.send(OUT, clean)


if __name__ == "__main__":
    run()
