import sys
import logging
import requests

if __name__ == "__main__":
    LOG_LEVEL = "DEBUG"  # if args.debug else 'INFO'
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    for role, group in [("broker", "default"), ("controller", "automatic-log-config")]:
        response = requests.get(
            f"http://test-kafka-{role}-{group}-metrics:9606/metrics", timeout=30
        )
        assert response.status_code == 200, (
            f"Expected HTTP 200 from {role} metrics, got {response.status_code}"
        )
        samples = {
            line.split()[0]: line.split()[1]
            for line in response.text.splitlines()
            if line and not line.startswith("#")
        }
        assert float(samples["jmx_scrape_error"]) == 0, f"JMX scrape failed for {role}"
        assert any(name.startswith("kafka_") for name in samples), (
            f"Expected Kafka metrics for {role}"
        )
