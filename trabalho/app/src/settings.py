from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092,kafka-2:29093,kafka-3:29094"
    )
    kafka_topic_orders: str = os.getenv("KAFKA_TOPIC_ORDERS", "blackfriday_orders")
    kafka_consumer_group_processor: str = os.getenv(
        "KAFKA_CONSUMER_GROUP_PROCESSOR", "blackfriday_processor"
    )

    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "lakehouse")

    iceberg_catalog_uri: str = os.getenv(
        "ICEBERG_CATALOG_URI", "sqlite:////app/data/out/pyiceberg_catalog.db"
    )
    iceberg_namespace: str = os.getenv("ICEBERG_NAMESPACE", "blackfriday")
    iceberg_table: str = os.getenv("ICEBERG_TABLE", "orders_events")

    dashboard_metrics_key: str = os.getenv(
        "DASHBOARD_METRICS_KEY", "metrics/dashboard_latest.json"
    )
    output_root_dir: str = os.getenv("OUTPUT_ROOT_DIR", "/app/data/out")
    bronze_events_dir: str = os.getenv(
        "BRONZE_EVENTS_DIR", "/app/data/out/bronze/orders_events"
    )
    silver_events_dir: str = os.getenv(
        "SILVER_EVENTS_DIR", "/app/data/out/silver/orders_events"
    )
    dashboard_metrics_local_path: str = os.getenv(
        "DASHBOARD_METRICS_LOCAL_PATH", "/app/data/out/dashboard/dashboard_latest.json"
    )
    dashboard_state_local_path: str = os.getenv(
        "DASHBOARD_STATE_LOCAL_PATH", "/app/data/out/dashboard/dashboard_state.json"
    )
    spark_checkpoint_dir: str = os.getenv(
        "SPARK_CHECKPOINT_DIR", "/app/data/out/checkpoints/processor_spark"
    )

    seed_orders_path: str = os.getenv("SEED_ORDERS_PATH", "/app/data/seed/orders_seed.csv")
    seed_targets_path: str = os.getenv(
        "SEED_TARGETS_PATH", "/app/data/seed/seller_targets.csv"
    )

    generator_events_per_second: int = int(os.getenv("GENERATOR_EVENTS_PER_SECOND", "25"))
    processor_batch_size: int = int(os.getenv("PROCESSOR_BATCH_SIZE", "400"))
    processor_flush_seconds: float = float(os.getenv("PROCESSOR_FLUSH_SECONDS", "3"))


SETTINGS = Settings()
