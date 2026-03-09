from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
import polars as pl
from confluent_kafka import Consumer, KafkaException

from kafka_utils import wait_for_kafka
from metrics import build_dashboard_snapshot, build_orders_frame, concat_orders, empty_orders_frame
from minio_utils import build_s3_client, ensure_bucket, upload_json, upload_parquet
from settings import SETTINGS


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _load_existing_silver() -> pl.DataFrame:
    silver_dir = Path(SETTINGS.silver_events_dir)
    files = sorted(silver_dir.glob("*.parquet"))
    if not files:
        return empty_orders_frame()
    return pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")


def _write_local_parquet(df: pl.DataFrame, directory: str, prefix: str) -> Path:
    output_dir = Path(directory)
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{prefix}_{_utc_stamp()}_{uuid.uuid4().hex[:8]}.parquet"
    df.write_parquet(file_path)
    return file_path


def _upload_batch(df: pl.DataFrame, key_prefix: str, client) -> None:
    key = f"{key_prefix}/{_utc_stamp()}_{uuid.uuid4().hex[:8]}.parquet"
    upload_parquet(client, key, df.to_arrow())


def _persist_snapshot(snapshot: dict[str, Any], client) -> None:
    local_path = Path(SETTINGS.dashboard_metrics_local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    upload_json(client, SETTINGS.dashboard_metrics_key, snapshot)


def _flush_batch(records: list[dict[str, Any]], all_orders_df: pl.DataFrame, client) -> pl.DataFrame:
    raw_df = pl.DataFrame(records)
    clean_df = build_orders_frame(records)

    _write_local_parquet(raw_df, SETTINGS.bronze_events_dir, "bronze_orders")
    _upload_batch(raw_df, "bronze/orders_events", client)

    _write_local_parquet(clean_df, SETTINGS.silver_events_dir, "silver_orders")
    _upload_batch(clean_df, "silver/orders_events", client)

    updated_orders = concat_orders(all_orders_df, clean_df)
    snapshot = build_dashboard_snapshot(updated_orders)
    _persist_snapshot(snapshot, client)

    totals = snapshot["totals"]
    print(
        "Batch flushed: "
        f"rows={len(records)} total_rows={snapshot['row_count']} gmv={totals['gmv']:.2f} "
        f"profit={totals['net_profit']:.2f}"
    )
    return updated_orders


def main() -> None:
    wait_for_kafka(timeout_seconds=240, min_brokers=3)

    s3_client = build_s3_client()
    ensure_bucket(s3_client)

    consumer = Consumer(
        {
            "bootstrap.servers": SETTINGS.kafka_bootstrap_servers,
            "group.id": SETTINGS.kafka_consumer_group_processor,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([SETTINGS.kafka_topic_orders])

    all_orders_df = _load_existing_silver()
    if all_orders_df.height > 0:
        snapshot = build_dashboard_snapshot(all_orders_df)
        _persist_snapshot(snapshot, s3_client)
        print(f"Recovered {all_orders_df.height} silver rows from local state")

    pending_records: list[dict[str, Any]] = []
    last_flush_at = time.monotonic()

    print(
        "Stream processor started "
        f"topic={SETTINGS.kafka_topic_orders} group={SETTINGS.kafka_consumer_group_processor}"
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.monotonic()

            if msg is None:
                if pending_records and (now - last_flush_at) >= SETTINGS.processor_flush_seconds:
                    all_orders_df = _flush_batch(pending_records, all_orders_df, s3_client)
                    consumer.commit(asynchronous=False)
                    pending_records.clear()
                    last_flush_at = now
                continue

            if msg.error():
                raise KafkaException(msg.error())

            payload = msg.value()
            if payload is None:
                continue

            pending_records.append(orjson.loads(payload))

            if len(pending_records) >= SETTINGS.processor_batch_size:
                all_orders_df = _flush_batch(pending_records, all_orders_df, s3_client)
                consumer.commit(asynchronous=False)
                pending_records.clear()
                last_flush_at = now

    finally:
        if pending_records:
            all_orders_df = _flush_batch(pending_records, all_orders_df, s3_client)
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    main()