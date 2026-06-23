"""End-to-end smoke test: bring the whole docker stack up and assert the
streaming pipeline produces aggregate output. Gated behind RUN_E2E=1 because it
needs Docker and takes a couple of minutes -- it is not part of the unit suite.
"""
import glob
import os
import subprocess
import time

import pytest

pytestmark = pytest.mark.skipif(os.getenv("RUN_E2E") != "1",
                                reason="set RUN_E2E=1 to run the docker e2e test")


def _has_output() -> bool:
    # The append-only _enriched_purchases store is the stable source of truth;
    # the minute_* outputs are overwritten each batch. Accept either as evidence
    # that the streaming pipeline produced enriched, aggregatable data.
    return bool(glob.glob("output/_enriched_purchases/*.parquet")
                or glob.glob("output/minute_purchase_metrics/*.parquet"))


def test_pipeline_produces_output():
    subprocess.run(["docker", "compose", "up", "--build", "-d"], check=True)
    try:
        # Generous budget: on a cold runner Spark must pull its image and resolve
        # the spark-sql-kafka connector via Ivy before the first batch runs.
        deadline = time.time() + 300
        while time.time() < deadline and not _has_output():
            time.sleep(5)
        if not _has_output():
            # Surface container logs before teardown so CI failures are debuggable.
            subprocess.run(["docker", "compose", "logs", "--tail", "250",
                            "spark-streaming"], check=False)
        assert _has_output(), "no streaming aggregate output appeared within timeout"
    finally:
        subprocess.run(["docker", "compose", "down", "-v"], check=True)
