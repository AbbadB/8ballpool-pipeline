"""Replay dead-lettered events back onto events.raw.

The DLQ is the reprocessing path (ADR-0009): after fixing the offending rule or
upstream producer, run this to re-feed the original payloads through the pipeline.
Reads events.dlq once (until idle), re-produces each original to events.raw.
Skips records whose original is a non-JSON string (unfixable without manual edit).
"""
import json
import logging
import os

from eightball.dq.replay import extract_original
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
DLQ, RAW = "events.dlq", "events.raw"

logging.basicConfig(level=logging.INFO, format='{"evt":"replay","msg":%(message)s}')
logging.getLogger("kafka").setLevel(logging.WARNING)
log = logging.getLogger("replay")


def run(idle_ms: int = 5000):
    consumer = KafkaConsumer(
        DLQ, bootstrap_servers=BOOTSTRAP, group_id="dlq-replay",
        auto_offset_reset="earliest", consumer_timeout_ms=idle_ms,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    replayed = skipped = 0
    for msg in consumer:
        original = extract_original(msg.value)
        if isinstance(original, dict):           # only structured payloads are replayable
            producer.send(RAW, original)
            replayed += 1
        else:
            skipped += 1                          # e.g. malformed-JSON string originals
    producer.flush()
    log.info(json.dumps({"replayed": replayed, "skipped": skipped}))


if __name__ == "__main__":
    run()
