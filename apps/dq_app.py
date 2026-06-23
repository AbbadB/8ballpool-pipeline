"""Consume raw events, validate + apply the generic DQ rule set, produce clean
events. Failures never crash the loop: they are routed to a dead-letter topic
(events.dlq) with a reason, logged, and counted.

This stays a *pure column transformer* (uppercase, id->name). It does NOT enrich
match/purchase with country -- that is Spark's job (ADR-0001). Error-handling and
reprocessing design: ADR-0009.
"""
import json
import logging
import os
import time

from kafka import KafkaConsumer, KafkaProducer

from eightball.dq.pipeline import process_event

# In Docker, services set KAFKA_BOOTSTRAP=kafka:9092 (internal listener).
# On the host, the default uses the external listener on localhost:29092.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
IN, OUT, DLQ = "events.raw", "events.clean", "events.dlq"
LOG_EVERY = 500                                   # counter cadence

logging.basicConfig(level=logging.INFO,
                    format='{"lvl":"%(levelname)s","evt":"dq","msg":%(message)s}')
# Quiet kafka-python's chatty INFO logs so our structured DQ lines are the signal.
logging.getLogger("kafka").setLevel(logging.WARNING)
log = logging.getLogger("dq")


def _dead_letter(producer, reason, original):
    record = {"reason": reason, "original": original,
              "failed_at": int(time.time() * 1000)}
    producer.send(DLQ, record)
    log.warning(json.dumps({"action": "dead_letter", "reason": reason}))


def run():
    consumer = KafkaConsumer(
        IN, bootstrap_servers=BOOTSTRAP, group_id="dq-app",
        auto_offset_reset="earliest",
        # Raw bytes: we decode inside the loop so a bad payload becomes a DLQ
        # record instead of crashing the consumer's deserializer.
        value_deserializer=lambda b: b,
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    counts = {"processed": 0, "clean": 0, "dead_letter": 0}
    for msg in consumer:                          # at-least-once: see ADR-0002
        counts["processed"] += 1
        # 1. Decode -> bad JSON is a dead letter, not a crash.
        try:
            raw = json.loads(msg.value.decode("utf-8"))
        except Exception as e:                    # noqa: BLE001
            _dead_letter(producer, f"decode: {e}",
                         msg.value.decode("utf-8", errors="replace"))
            counts["dead_letter"] += 1
            continue
        # 2. Validate + transform via the pure pipeline.
        result = process_event(raw)
        if result.status == "ok":
            producer.send(OUT, result.event)
            counts["clean"] += 1
        else:
            _dead_letter(producer, result.reason, result.event)
            counts["dead_letter"] += 1
        # 3. Periodic counter line for observability.
        if counts["processed"] % LOG_EVERY == 0:
            log.info(json.dumps({"action": "progress", **counts}))


if __name__ == "__main__":
    run()
