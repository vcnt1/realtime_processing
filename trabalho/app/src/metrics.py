from __future__ import annotations

from datetime import UTC, datetime
from math import isnan
from typing import Any

import polars as pl


BASE_SCHEMA: dict[str, pl.DataType] = {
    "event_id": pl.Utf8,
    "event_time": pl.Utf8,
    "ingestion_time": pl.Utf8,
    "order_id": pl.Utf8,
    "order_date": pl.Utf8,
    "customer_id": pl.Utf8,
    "seller_id": pl.Utf8,
    "seller_name": pl.Utf8,
    "store_id": pl.Utf8,
    "store_name": pl.Utf8,
    "region": pl.Utf8,
    "channel": pl.Utf8,
    "category": pl.Utf8,
    "product_id": pl.Utf8,
    "product_name": pl.Utf8,
    "quantity": pl.Int64,
    "unit_price": pl.Float64,
    "gross_amount": pl.Float64,
    "discount_pct": pl.Float64,
    "discount_amount": pl.Float64,
    "shipping_amount": pl.Float64,
    "net_revenue": pl.Float64,
    "cogs": pl.Float64,
    "status": pl.Utf8,
    "payment_method": pl.Utf8,
    "campaign": pl.Utf8,
    "target_gmv": pl.Float64,
    "target_orders": pl.Int64,
}

FULL_SCHEMA: dict[str, pl.DataType] = {
    **BASE_SCHEMA,
    "event_ts": pl.Datetime(time_zone="UTC"),
    "ingestion_ts": pl.Datetime(time_zone="UTC"),
    "event_minute": pl.Datetime(time_zone="UTC"),
    "paid_order_count": pl.Int64,
    "cancelled_order_count": pl.Int64,
    "returned_order_count": pl.Int64,
    "recognized_revenue": pl.Float64,
    "recognized_cogs": pl.Float64,
    "gross_profit": pl.Float64,
    "discount_rate_pct": pl.Float64,
}


def _empty_frame(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame({name: pl.Series(name=name, values=[], dtype=dtype) for name, dtype in schema.items()})


def empty_orders_frame() -> pl.DataFrame:
    return _empty_frame(FULL_SCHEMA)


def _round_number(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        if isnan(value):
            return 0.0
        return round(value, 2)
    return value


def _to_records(df: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        item: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                item[key] = value.astimezone(UTC).isoformat().replace("+00:00", "Z")
            else:
                item[key] = _round_number(value)
        rows.append(item)
    return rows


def build_orders_frame(records: list[dict[str, Any]]) -> pl.DataFrame:
    if not records:
        return empty_orders_frame()

    raw_df = pl.DataFrame(records)
    columns = []
    for name, dtype in BASE_SCHEMA.items():
        if name in raw_df.columns:
            columns.append(pl.col(name).cast(dtype, strict=False).alias(name))
        else:
            columns.append(pl.lit(None, dtype=dtype).alias(name))

    df = raw_df.select(columns).with_columns(
        pl.col("event_time").str.to_datetime(strict=False, time_zone="UTC").alias("event_ts"),
        pl.col("ingestion_time").str.to_datetime(strict=False, time_zone="UTC").alias("ingestion_ts"),
    )

    status = pl.col("status")

    return df.with_columns(
        pl.col("event_ts").dt.truncate("1m").alias("event_minute"),
        pl.when(status == "PAID").then(1).otherwise(0).alias("paid_order_count"),
        pl.when(status == "CANCELLED").then(1).otherwise(0).alias("cancelled_order_count"),
        pl.when(status == "RETURNED").then(1).otherwise(0).alias("returned_order_count"),
        pl.when(status == "PAID")
        .then(pl.col("net_revenue"))
        .when(status == "RETURNED")
        .then(-pl.col("net_revenue"))
        .otherwise(0.0)
        .alias("recognized_revenue"),
        pl.when(status == "PAID")
        .then(pl.col("cogs"))
        .when(status == "RETURNED")
        .then(-pl.col("cogs"))
        .otherwise(0.0)
        .alias("recognized_cogs"),
        (pl.col("discount_pct") * 100.0).alias("discount_rate_pct"),
    ).with_columns((pl.col("recognized_revenue") - pl.col("recognized_cogs")).alias("gross_profit"))


def concat_orders(existing_df: pl.DataFrame, batch_df: pl.DataFrame) -> pl.DataFrame:
    if existing_df.height == 0:
        return batch_df
    if batch_df.height == 0:
        return existing_df
    return pl.concat([existing_df, batch_df], how="vertical_relaxed")


def build_dashboard_snapshot(orders_df: pl.DataFrame) -> dict[str, Any]:
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if orders_df.height == 0:
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

    totals_row = orders_df.select(
        pl.sum("recognized_revenue").alias("gmv"),
        pl.sum("gross_profit").alias("net_profit"),
        pl.sum("recognized_cogs").alias("cogs"),
        pl.sum("discount_amount").alias("discount_impact"),
        pl.sum("paid_order_count").alias("confirmed_orders"),
        pl.sum("cancelled_order_count").alias("cancelled_orders"),
        pl.sum("returned_order_count").alias("returned_orders"),
    ).to_dicts()[0]

    confirmed_orders = int(totals_row["confirmed_orders"] or 0)
    gmv = float(totals_row["gmv"] or 0.0)
    net_profit = float(totals_row["net_profit"] or 0.0)
    totals = {
        "gmv": round(gmv, 2),
        "net_profit": round(net_profit, 2),
        "avg_ticket": round(gmv / confirmed_orders, 2) if confirmed_orders else 0.0,
        "gross_margin_pct": round((net_profit / gmv) * 100.0, 2) if gmv else 0.0,
        "cogs": round(float(totals_row["cogs"] or 0.0), 2),
        "discount_impact": round(float(totals_row["discount_impact"] or 0.0), 2),
        "confirmed_orders": confirmed_orders,
        "cancelled_orders": int(totals_row["cancelled_orders"] or 0),
        "returned_orders": int(totals_row["returned_orders"] or 0),
    }

    seller_df = (
        orders_df.group_by(["seller_id", "seller_name"]).agg(
            pl.sum("recognized_revenue").alias("gmv"),
            pl.sum("gross_profit").alias("net_profit"),
            pl.sum("paid_order_count").alias("orders"),
            pl.mean("discount_rate_pct").alias("avg_discount_pct"),
            pl.first("target_gmv").alias("target_gmv"),
            pl.first("target_orders").alias("target_orders"),
        )
    ).with_columns(
        pl.when(pl.col("orders") > 0)
        .then(pl.col("gmv") / pl.col("orders"))
        .otherwise(0.0)
        .alias("avg_ticket"),
        pl.when(pl.col("target_gmv") > 0)
        .then((pl.col("gmv") / pl.col("target_gmv")) * 100.0)
        .otherwise(0.0)
        .alias("target_gmv_pct"),
        pl.when(pl.col("target_orders") > 0)
        .then((pl.col("orders") / pl.col("target_orders")) * 100.0)
        .otherwise(0.0)
        .alias("target_orders_pct"),
    ).sort("gmv", descending=True)

    store_df = (
        orders_df.group_by(["store_id", "store_name"]).agg(
            pl.sum("recognized_revenue").alias("gmv"),
            pl.sum("paid_order_count").alias("orders"),
            pl.mean("discount_rate_pct").alias("avg_discount_pct"),
        )
    ).with_columns(
        pl.when(pl.col("orders") > 0)
        .then(pl.col("gmv") / pl.col("orders"))
        .otherwise(0.0)
        .alias("avg_ticket")
    ).sort("gmv", descending=True)

    channel_df = orders_df.group_by("channel").agg(
        pl.sum("recognized_revenue").alias("gmv"),
        pl.sum("gross_profit").alias("net_profit"),
        pl.sum("recognized_cogs").alias("cogs"),
    ).sort("net_profit", descending=True)

    category_df = orders_df.group_by("category").agg(
        pl.sum("recognized_revenue").alias("gmv"),
        pl.sum("gross_profit").alias("net_profit"),
        pl.sum("recognized_cogs").alias("cogs"),
    ).sort("net_profit", descending=True)

    status_df = orders_df.group_by("status").agg(
        pl.len().alias("events"),
        pl.sum("net_revenue").alias("listed_revenue"),
        pl.sum("discount_amount").alias("discount_amount"),
    ).sort("events", descending=True)

    trend_df = orders_df.group_by("event_minute").agg(
        pl.sum("recognized_revenue").alias("gmv"),
        pl.sum("paid_order_count").alias("orders"),
    ).sort("event_minute").tail(60)

    return {
        "generated_at": generated_at,
        "row_count": orders_df.height,
        "totals": totals,
        "sales_by_seller": _to_records(seller_df),
        "sales_by_store": _to_records(store_df),
        "sales_by_channel": _to_records(channel_df),
        "sales_by_category": _to_records(category_df),
        "status_summary": _to_records(status_df),
        "recent_gmv": _to_records(trend_df),
        "top_performers": _to_records(seller_df.head(5)),
        "bottom_performers": _to_records(seller_df.sort("gmv").head(5)),
    }