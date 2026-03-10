from __future__ import annotations

import json
from datetime import UTC, date, datetime
from math import isnan
from pathlib import Path
from typing import Any

import boto3
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from kafka_utils import wait_for_kafka
from settings import SETTINGS


EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("ingestion_time", T.StringType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("order_date", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("seller_id", T.StringType()),
        T.StructField("seller_name", T.StringType()),
        T.StructField("store_id", T.StringType()),
        T.StructField("store_name", T.StringType()),
        T.StructField("region", T.StringType()),
        T.StructField("channel", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("product_name", T.StringType()),
        T.StructField("quantity", T.LongType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("gross_amount", T.DoubleType()),
        T.StructField("discount_pct", T.DoubleType()),
        T.StructField("discount_amount", T.DoubleType()),
        T.StructField("shipping_amount", T.DoubleType()),
        T.StructField("net_revenue", T.DoubleType()),
        T.StructField("cogs", T.DoubleType()),
        T.StructField("status", T.StringType()),
        T.StructField("payment_method", T.StringType()),
        T.StructField("campaign", T.StringType()),
        T.StructField("target_gmv", T.DoubleType()),
        T.StructField("target_orders", T.LongType()),
    ]
)


def _normalize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        if isnan(value):
            return 0.0
        return round(value, 2)
    return value


def _row_to_record(row) -> dict[str, Any]:
    return {key: _normalize_value(value) for key, value in row.asDict(recursive=True).items()}


def _rows_to_records(rows: list[Any]) -> list[dict[str, Any]]:
    return [_row_to_record(row) for row in rows]


def _empty_snapshot() -> dict[str, Any]:
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "generated_at": generated_at,
        "row_count": 0,
        "totals": {
            "gmv": 0.0,
            "net_profit": 0.0,
            "avg_ticket": 0.0,
            "gross_margin_pct": 0.0,
            "cogs": 0.0,
            "discount_impact": 0.0,
            "confirmed_orders": 0,
            "cancelled_orders": 0,
            "returned_orders": 0,
        },
        "sales_by_seller": [],
        "sales_by_store": [],
        "sales_by_channel": [],
        "sales_by_category": [],
        "status_summary": [],
        "recent_gmv": [],
        "top_performers": [],
        "bottom_performers": [],
    }


def _empty_state() -> dict[str, Any]:
    return {
        "row_count": 0,
        "totals": {
            "gmv": 0.0,
            "net_profit": 0.0,
            "cogs": 0.0,
            "discount_impact": 0.0,
            "confirmed_orders": 0,
            "cancelled_orders": 0,
            "returned_orders": 0,
        },
        "seller_metrics": {},
        "store_metrics": {},
        "channel_metrics": {},
        "category_metrics": {},
        "status_metrics": {},
        "minute_metrics": {},
    }


def _build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.minio_endpoint,
        aws_access_key_id=SETTINGS.minio_access_key,
        aws_secret_access_key=SETTINGS.minio_secret_key,
        region_name="us-east-1",
    )


def _ensure_bucket(s3_client) -> None:
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if SETTINGS.minio_bucket not in existing:
        s3_client.create_bucket(Bucket=SETTINGS.minio_bucket)


def _upload_json(s3_client, key: str, payload: dict[str, Any]) -> None:
    s3_client.put_object(
        Bucket=SETTINGS.minio_bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def _state_path() -> Path:
    return Path(SETTINGS.dashboard_state_local_path)


def _load_state() -> dict[str, Any] | None:
    state_path = _state_path()
    if not state_path.exists():
        return None
    return json.loads(state_path.read_text(encoding="utf-8"))


def _persist_state(state: dict[str, Any]) -> None:
    state_path = _state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_silver_df(df: DataFrame) -> DataFrame:
    event_ts = F.to_timestamp("event_time")
    status = F.col("status")
    return df.withColumn("event_ts", event_ts).withColumn(
        "ingestion_ts", F.to_timestamp("ingestion_time")
    ).withColumn(
        "event_minute", F.date_trunc("minute", event_ts)
    ).withColumn(
        "paid_order_count", F.when(status == F.lit("PAID"), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "cancelled_order_count",
        F.when(status == F.lit("CANCELLED"), F.lit(1)).otherwise(F.lit(0)),
    ).withColumn(
        "returned_order_count",
        F.when(status == F.lit("RETURNED"), F.lit(1)).otherwise(F.lit(0)),
    ).withColumn(
        "recognized_revenue",
        F.when(status == F.lit("PAID"), F.col("net_revenue"))
        .when(status == F.lit("RETURNED"), -F.col("net_revenue"))
        .otherwise(F.lit(0.0)),
    ).withColumn(
        "recognized_cogs",
        F.when(status == F.lit("PAID"), F.col("cogs"))
        .when(status == F.lit("RETURNED"), -F.col("cogs"))
        .otherwise(F.lit(0.0)),
    ).withColumn(
        "discount_rate_pct", F.col("discount_pct") * F.lit(100.0)
    ).withColumn(
        "gross_profit", F.col("recognized_revenue") - F.col("recognized_cogs")
    )


def _persist_snapshot(snapshot: dict[str, Any], s3_client) -> None:
    local_path = Path(SETTINGS.dashboard_metrics_local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    _upload_json(s3_client, SETTINGS.dashboard_metrics_key, snapshot)


def _sort_records(records: list[dict[str, Any]], key: str, descending: bool = True) -> list[dict[str, Any]]:
    return sorted(records, key=lambda item: float(item.get(key, 0.0) or 0.0), reverse=descending)


def _round_dict_values(values: dict[str, Any]) -> dict[str, Any]:
    return {key: _normalize_value(value) for key, value in values.items()}


def _snapshot_from_state(state: dict[str, Any]) -> dict[str, Any]:
    totals_state = state["totals"]
    gmv = float(totals_state["gmv"] or 0.0)
    net_profit = float(totals_state["net_profit"] or 0.0)
    confirmed_orders = int(totals_state["confirmed_orders"] or 0)

    sales_by_seller = []
    for seller in state["seller_metrics"].values():
        avg_discount_pct = (
            float(seller["discount_sum"]) / int(seller["event_count"])
            if int(seller["event_count"])
            else 0.0
        )
        avg_ticket = float(seller["gmv"]) / int(seller["orders"]) if int(seller["orders"]) else 0.0
        target_gmv = float(seller.get("target_gmv", 0.0) or 0.0)
        target_orders = int(seller.get("target_orders", 0) or 0)
        sales_by_seller.append(
            _round_dict_values(
                {
                    "seller_id": seller["seller_id"],
                    "seller_name": seller["seller_name"],
                    "gmv": seller["gmv"],
                    "net_profit": seller["net_profit"],
                    "orders": seller["orders"],
                    "avg_discount_pct": avg_discount_pct,
                    "target_gmv": target_gmv,
                    "target_orders": target_orders,
                    "avg_ticket": avg_ticket,
                    "target_gmv_pct": (float(seller["gmv"]) / target_gmv) * 100.0 if target_gmv else 0.0,
                    "target_orders_pct": (int(seller["orders"]) / target_orders) * 100.0 if target_orders else 0.0,
                }
            )
        )
    sales_by_seller = _sort_records(sales_by_seller, key="gmv", descending=True)

    sales_by_store = []
    for store in state["store_metrics"].values():
        avg_discount_pct = (
            float(store["discount_sum"]) / int(store["event_count"])
            if int(store["event_count"])
            else 0.0
        )
        avg_ticket = float(store["gmv"]) / int(store["orders"]) if int(store["orders"]) else 0.0
        sales_by_store.append(
            _round_dict_values(
                {
                    "store_id": store["store_id"],
                    "store_name": store["store_name"],
                    "gmv": store["gmv"],
                    "orders": store["orders"],
                    "avg_discount_pct": avg_discount_pct,
                    "avg_ticket": avg_ticket,
                }
            )
        )
    sales_by_store = _sort_records(sales_by_store, key="gmv", descending=True)

    sales_by_channel = _sort_records(
        [_round_dict_values(channel) for channel in state["channel_metrics"].values()],
        key="net_profit",
        descending=True,
    )
    sales_by_category = _sort_records(
        [_round_dict_values(category) for category in state["category_metrics"].values()],
        key="net_profit",
        descending=True,
    )
    status_summary = _sort_records(
        [_round_dict_values(status) for status in state["status_metrics"].values()],
        key="events",
        descending=True,
    )

    recent_gmv = [
        _round_dict_values(state["minute_metrics"][key])
        for key in sorted(state["minute_metrics"].keys())[-60:]
    ]

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "generated_at": generated_at,
        "row_count": int(state["row_count"]),
        "totals": {
            "gmv": round(gmv, 2),
            "net_profit": round(net_profit, 2),
            "avg_ticket": round(gmv / confirmed_orders, 2) if confirmed_orders else 0.0,
            "gross_margin_pct": round((net_profit / gmv) * 100.0, 2) if gmv else 0.0,
            "cogs": round(float(totals_state["cogs"] or 0.0), 2),
            "discount_impact": round(float(totals_state["discount_impact"] or 0.0), 2),
            "confirmed_orders": confirmed_orders,
            "cancelled_orders": int(totals_state["cancelled_orders"] or 0),
            "returned_orders": int(totals_state["returned_orders"] or 0),
        },
        "sales_by_seller": sales_by_seller,
        "sales_by_store": sales_by_store,
        "sales_by_channel": sales_by_channel,
        "sales_by_category": sales_by_category,
        "status_summary": status_summary,
        "recent_gmv": recent_gmv,
        "top_performers": sales_by_seller[:5],
        "bottom_performers": _sort_records(sales_by_seller, key="gmv", descending=False)[:5],
    }


def _collect_records(df: DataFrame, order_by: str | None = None, descending: bool = True, limit: int | None = None) -> list[dict[str, Any]]:
    result_df = df
    if order_by is not None:
        result_df = result_df.orderBy(F.col(order_by).desc() if descending else F.col(order_by).asc())
    if limit is not None:
        result_df = result_df.limit(limit)
    return _rows_to_records(result_df.collect())


def _build_state_from_silver(spark: SparkSession) -> dict[str, Any]:
    silver_dir = Path(SETTINGS.silver_events_dir)
    if not silver_dir.exists() or not any(silver_dir.rglob("*.parquet")):
        return _empty_state()

    silver_df = spark.read.parquet(str(silver_dir))
    if silver_df.limit(1).count() == 0:
        return _empty_state()

    state = _empty_state()
    totals_row = silver_df.agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("gross_profit").alias("net_profit"),
        F.sum("recognized_cogs").alias("cogs"),
        F.sum("discount_amount").alias("discount_impact"),
        F.sum("paid_order_count").alias("confirmed_orders"),
        F.sum("cancelled_order_count").alias("cancelled_orders"),
        F.sum("returned_order_count").alias("returned_orders"),
        F.count(F.lit(1)).alias("row_count"),
    ).collect()[0]
    state["row_count"] = int(totals_row["row_count"] or 0)
    state["totals"] = {
        "gmv": float(totals_row["gmv"] or 0.0),
        "net_profit": float(totals_row["net_profit"] or 0.0),
        "cogs": float(totals_row["cogs"] or 0.0),
        "discount_impact": float(totals_row["discount_impact"] or 0.0),
        "confirmed_orders": int(totals_row["confirmed_orders"] or 0),
        "cancelled_orders": int(totals_row["cancelled_orders"] or 0),
        "returned_orders": int(totals_row["returned_orders"] or 0),
    }

    for row in silver_df.groupBy("seller_id", "seller_name").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("gross_profit").alias("net_profit"),
        F.sum("paid_order_count").alias("orders"),
        F.sum("discount_rate_pct").alias("discount_sum"),
        F.count(F.lit(1)).alias("event_count"),
        F.first("target_gmv").alias("target_gmv"),
        F.first("target_orders").alias("target_orders"),
    ).collect():
        record = _row_to_record(row)
        state["seller_metrics"][record["seller_id"]] = record

    for row in silver_df.groupBy("store_id", "store_name").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("paid_order_count").alias("orders"),
        F.sum("discount_rate_pct").alias("discount_sum"),
        F.count(F.lit(1)).alias("event_count"),
    ).collect():
        record = _row_to_record(row)
        state["store_metrics"][record["store_id"]] = record

    for row in silver_df.groupBy("channel").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("gross_profit").alias("net_profit"),
        F.sum("recognized_cogs").alias("cogs"),
    ).collect():
        record = _row_to_record(row)
        state["channel_metrics"][record["channel"]] = record

    for row in silver_df.groupBy("category").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("gross_profit").alias("net_profit"),
        F.sum("recognized_cogs").alias("cogs"),
    ).collect():
        record = _row_to_record(row)
        state["category_metrics"][record["category"]] = record

    for row in silver_df.groupBy("status").agg(
        F.count(F.lit(1)).alias("events"),
        F.sum("net_revenue").alias("listed_revenue"),
        F.sum("discount_amount").alias("discount_amount"),
    ).collect():
        record = _row_to_record(row)
        state["status_metrics"][record["status"]] = record

    trend_rows = silver_df.groupBy("event_minute").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("paid_order_count").alias("orders"),
    ).orderBy(F.col("event_minute").desc()).limit(60).collect()
    for row in trend_rows:
        record = _row_to_record(row)
        minute_key = str(record["event_minute"])
        state["minute_metrics"][minute_key] = record

    return state


def _increment_metric_map(state_map: dict[str, dict[str, Any]], key: str, base_record: dict[str, Any], sum_fields: list[str]) -> None:
    current = state_map.setdefault(key, dict(base_record))
    for field in sum_fields:
        current[field] = _normalize_value(float(current.get(field, 0.0) or 0.0) + float(base_record.get(field, 0.0) or 0.0))
    for field, value in base_record.items():
        if field not in sum_fields:
            current[field] = value


def _trim_minute_metrics(state: dict[str, Any], keep: int = 60) -> None:
    minute_keys = sorted(state["minute_metrics"].keys())
    for old_key in minute_keys[:-keep]:
        state["minute_metrics"].pop(old_key, None)


def _merge_batch_into_state(state: dict[str, Any], batch_df: DataFrame) -> dict[str, Any]:
    totals_row = batch_df.agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("gross_profit").alias("net_profit"),
        F.sum("recognized_cogs").alias("cogs"),
        F.sum("discount_amount").alias("discount_impact"),
        F.sum("paid_order_count").alias("confirmed_orders"),
        F.sum("cancelled_order_count").alias("cancelled_orders"),
        F.sum("returned_order_count").alias("returned_orders"),
        F.count(F.lit(1)).alias("row_count"),
    ).collect()[0]

    state["row_count"] += int(totals_row["row_count"] or 0)
    for field in ["gmv", "net_profit", "cogs", "discount_impact"]:
        state["totals"][field] = _normalize_value(
            float(state["totals"].get(field, 0.0) or 0.0) + float(totals_row[field] or 0.0)
        )
    for field in ["confirmed_orders", "cancelled_orders", "returned_orders"]:
        state["totals"][field] = int(state["totals"].get(field, 0) or 0) + int(totals_row[field] or 0)

    for row in batch_df.groupBy("seller_id", "seller_name").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("gross_profit").alias("net_profit"),
        F.sum("paid_order_count").alias("orders"),
        F.sum("discount_rate_pct").alias("discount_sum"),
        F.count(F.lit(1)).alias("event_count"),
        F.first("target_gmv").alias("target_gmv"),
        F.first("target_orders").alias("target_orders"),
    ).collect():
        record = _row_to_record(row)
        _increment_metric_map(
            state["seller_metrics"],
            record["seller_id"],
            record,
            ["gmv", "net_profit", "orders", "discount_sum", "event_count"],
        )

    for row in batch_df.groupBy("store_id", "store_name").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("paid_order_count").alias("orders"),
        F.sum("discount_rate_pct").alias("discount_sum"),
        F.count(F.lit(1)).alias("event_count"),
    ).collect():
        record = _row_to_record(row)
        _increment_metric_map(
            state["store_metrics"],
            record["store_id"],
            record,
            ["gmv", "orders", "discount_sum", "event_count"],
        )

    for row in batch_df.groupBy("channel").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("gross_profit").alias("net_profit"),
        F.sum("recognized_cogs").alias("cogs"),
    ).collect():
        record = _row_to_record(row)
        _increment_metric_map(
            state["channel_metrics"], record["channel"], record, ["gmv", "net_profit", "cogs"]
        )

    for row in batch_df.groupBy("category").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("gross_profit").alias("net_profit"),
        F.sum("recognized_cogs").alias("cogs"),
    ).collect():
        record = _row_to_record(row)
        _increment_metric_map(
            state["category_metrics"], record["category"], record, ["gmv", "net_profit", "cogs"]
        )

    for row in batch_df.groupBy("status").agg(
        F.count(F.lit(1)).alias("events"),
        F.sum("net_revenue").alias("listed_revenue"),
        F.sum("discount_amount").alias("discount_amount"),
    ).collect():
        record = _row_to_record(row)
        _increment_metric_map(
            state["status_metrics"],
            record["status"],
            record,
            ["events", "listed_revenue", "discount_amount"],
        )

    for row in batch_df.groupBy("event_minute").agg(
        F.sum("recognized_revenue").alias("gmv"),
        F.sum("paid_order_count").alias("orders"),
    ).collect():
        record = _row_to_record(row)
        minute_key = str(record["event_minute"])
        _increment_metric_map(state["minute_metrics"], minute_key, record, ["gmv", "orders"])

    _trim_minute_metrics(state)
    return state


def _process_batch(batch_df: DataFrame, batch_id: int, state: dict[str, Any], s3_client) -> None:
    if batch_df.limit(1).count() == 0:
        return

    bronze_path = Path(SETTINGS.bronze_events_dir)
    silver_path = Path(SETTINGS.silver_events_dir)
    bronze_path.mkdir(parents=True, exist_ok=True)
    silver_path.mkdir(parents=True, exist_ok=True)

    bronze_df = batch_df.select([field.name for field in EVENT_SCHEMA.fields])
    silver_df = _build_silver_df(bronze_df)

    bronze_df.write.mode("append").parquet(str(bronze_path))
    silver_df.write.mode("append").parquet(str(silver_path))

    state = _merge_batch_into_state(state, silver_df)
    snapshot = _snapshot_from_state(state)
    _persist_state(state)
    _persist_snapshot(snapshot, s3_client)

    totals = snapshot["totals"]
    print(
        "Spark batch flushed: "
        f"batch_id={batch_id} rows={bronze_df.count()} total_rows={snapshot['row_count']} "
        f"gmv={totals['gmv']:.2f} profit={totals['net_profit']:.2f}"
    )


def main() -> None:
    wait_for_kafka(timeout_seconds=240, min_brokers=3)

    s3_client = _build_s3_client()
    _ensure_bucket(s3_client)

    spark = (
        SparkSession.builder.appName("blackfriday-spark-processor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    state = _load_state()
    if state is None:
        state = _build_state_from_silver(spark)
        _persist_state(state)

    initial_snapshot = _snapshot_from_state(state)
    _persist_snapshot(initial_snapshot, s3_client)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", SETTINGS.kafka_bootstrap_servers)
        .option("subscribe", SETTINGS.kafka_topic_orders)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = raw_stream.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    trigger_seconds = max(1, int(round(SETTINGS.processor_flush_seconds)))
    query = (
        parsed_stream.writeStream.option("checkpointLocation", SETTINGS.spark_checkpoint_dir)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .foreachBatch(lambda df, batch_id: _process_batch(df, batch_id, state, s3_client))
        .start()
    )

    print(
        "Spark stream processor started "
        f"topic={SETTINGS.kafka_topic_orders} checkpoint={SETTINGS.spark_checkpoint_dir}"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()