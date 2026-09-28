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

    response = requests.get(
        "http://test-kafka-broker-default-metrics:9606/metrics", timeout=30
    )
    assert response.status_code == 200, (
        f"Expected HTTP 200 from broker metrics, got {response.status_code}"
    )
    samples = {
        line.split()[0]: line.split()[1]
        for line in response.text.splitlines()
        if line and not line.startswith("#")
    }
    assert float(samples["jmx_scrape_error"]) == 0, "JMX scrape failed for broker"
    assert any(name.startswith("kafka_") for name in samples), (
        "Expected Kafka metrics for broker"
    )
