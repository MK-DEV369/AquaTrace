from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = ("ais.raw", "ais.clean", "ais.invalid", "ais.anomalies")


def ensure_topics(bootstrap_servers: str) -> None:
    """Create local development topics if they do not already exist."""

    client = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = client.create_topics(
        [NewTopic(name, num_partitions=3, replication_factor=1) for name in TOPICS]
    )
    for name, future in futures.items():
        try:
            future.result()
        except Exception as exc:  # Kafka returns an error when the topic already exists.
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"could not create topic {name}") from exc

