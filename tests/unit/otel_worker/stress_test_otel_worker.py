import asyncio
import json
import logging
import time
from argparse import ArgumentParser
from statistics import mean, median

import aiohttp

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("stress_test")

SAMPLE_TRACE_BODY = {
    "resourceSpans": [
        {
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "stress-test-service"}}
                ]
            },
            "scopeSpans": [
                {
                    "spans": [
                        {
                            "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
                            "spanId": "051581bf3cb55c1f",
                            "name": "stress-test-span",
                            "kind": 2,
                            "startTimeUnixNano": "1627464000000000000",
                            "endTimeUnixNano": "1627464000100000000",
                        }
                    ]
                }
            ],
        }
    ]
}


async def send_traces(
    url: str,
    concurrency: int,
    duration: int,
    rps_target: int,
    auth_token: str = None,
):
    """Run stress test."""
    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    msg_body = json.dumps(SAMPLE_TRACE_BODY)

    # Pre-calculate sleep time to hit rps
    # If rps_target is total, each worker does rps_target / concurrency
    worker_target_rps = rps_target / concurrency
    sleep_time = 1.0 / worker_target_rps if worker_target_rps > 0 else 0

    stats = {
        "sent": 0,
        "202": 0,
        "429": 0,
        "5xx": 0,
        "errors": 0,
        "latencies": [],
    }

    async def worker(id: int):
        end_time = time.time() + duration
        async with aiohttp.ClientSession() as session:
            while time.time() < end_time:
                start = time.time()
                try:
                    async with session.post(url, data=msg_body, headers=headers) as resp:
                        status = resp.status
                        stats["sent"] += 1
                        if status == 202:
                            stats["202"] += 1
                        elif status == 429:
                            stats["429"] += 1
                        elif status >= 500:
                            stats["5xx"] += 1
                            txt = await resp.text()
                            logger.error(f"Worker {id} got 5xx: {txt}")
                        else:
                            stats["errors"] += 1

                        latency = (time.time() - start) * 1000
                        stats["latencies"].append(latency)

                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Worker {id} error: {e}")

                # Rate limiting sleep
                elapsed = time.time() - start
                remaining = sleep_time - elapsed
                if remaining > 0:
                    await asyncio.sleep(remaining)

    logger.info(
        f"Starting {concurrency} workers for {duration}s aimed at {rps_target} RPS total..."
    )
    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await asyncio.gather(*tasks)

    logger.info("--- Results ---")
    logger.info(f"Total Sent: {stats['sent']}")
    logger.info(f"Success (202): {stats['202']}")
    logger.info(f"Throttled (429): {stats['429']}")
    logger.info(f"Failed (5xx): {stats['5xx']}")
    logger.info(f"Errors: {stats['errors']}")

    if stats["latencies"]:
        msg = (
            f"Latency (ms) - Avg: {mean(stats['latencies']):.2f}, "
            f"Median: {median(stats['latencies']):.2f}"
        )
        logger.info(msg)

    # CI Mode Assertion: Fail on any 5xx
    if stats["5xx"] > 0:
        logger.error("Test Failed: 5xx errors detected")
        exit(1)

    logger.info("Test Passed")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8002/v1/traces")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--duration", type=int, default=10)  # seconds
    parser.add_argument("--rps", type=int, default=100)
    args = parser.parse_args()

    asyncio.run(send_traces(args.url, args.concurrency, args.duration, args.rps))
