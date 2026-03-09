from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from minio_utils import build_s3_client, get_json
from settings import SETTINGS


def _format_currency(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _format_pct(value: float) -> str:
    return f"{value:.2f}%"


def _load_snapshot() -> dict[str, Any] | None:
    local_path = Path(SETTINGS.dashboard_metrics_local_path)
    if local_path.exists():
        return json.loads(local_path.read_text(encoding="utf-8"))

    client = build_s3_client()
    return get_json(client, SETTINGS.dashboard_metrics_key)


def _to_df(items: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(items or [])


def _render_empty_state() -> None:
    st.title("Black Friday Dashboard")
    st.info("Aguardando o primeiro snapshot do processor.")


def main() -> None:
    st.set_page_config(page_title="Black Friday Dashboard", page_icon="sales", layout="wide")

    refresh_seconds = st.sidebar.slider("Atualizacao automatica (s)", min_value=3, max_value=30, value=5)
    auto_refresh = st.sidebar.toggle("Atualizacao automatica", value=True)

    snapshot = _load_snapshot()
    if not snapshot:
        _render_empty_state()
        if auto_refresh:
            time.sleep(refresh_seconds)
            st.rerun()
        return

    totals = snapshot["totals"]
    seller_df = _to_df(snapshot["sales_by_seller"])
    store_df = _to_df(snapshot["sales_by_store"])
    channel_df = _to_df(snapshot["sales_by_channel"])
    category_df = _to_df(snapshot["sales_by_category"])
    status_df = _to_df(snapshot["status_summary"])
    trend_df = _to_df(snapshot["recent_gmv"])
    top_df = _to_df(snapshot["top_performers"])
    bottom_df = _to_df(snapshot["bottom_performers"])

    st.title("Black Friday Dashboard")
    st.caption(
        f"Snapshot: {snapshot['generated_at']} | eventos processados: {snapshot['row_count']}"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("GMV", _format_currency(float(totals["gmv"])))
    c2.metric("Lucro Liquido", _format_currency(float(totals["net_profit"])))
    c3.metric("Ticket Medio", _format_currency(float(totals["avg_ticket"])))

    if not trend_df.empty:
        trend_chart = px.line(
            trend_df,
            x="event_minute",
            y="gmv",
            title="GMV em janela recente",
            markers=True,
        )
        trend_chart.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=320)
        st.plotly_chart(trend_chart, use_container_width=True)

    st.header("Vendas")
    v1, v2, v3 = st.columns(3)
    v1.metric("Pedidos Confirmados", f"{int(totals['confirmed_orders'])}")
    v2.metric("Desconto Medio", _format_pct(float(seller_df["avg_discount_pct"].mean()) if not seller_df.empty else 0.0))
    v3.metric("Cancelamentos + Devolucoes", f"{int(totals['cancelled_orders']) + int(totals['returned_orders'])}")

    if not seller_df.empty:
        seller_chart = px.bar(
            seller_df.head(10),
            x="seller_name",
            y="gmv",
            title="GMV por vendedor",
            color="target_gmv_pct",
            color_continuous_scale="Blues",
        )
        seller_chart.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=360)
        st.plotly_chart(seller_chart, use_container_width=True)

        target_chart = px.bar(
            seller_df.sort_values("target_gmv_pct", ascending=False).head(10),
            x="target_gmv_pct",
            y="seller_name",
            title="Meta vs realizado por vendedor",
            orientation="h",
            text="target_gmv_pct",
        )
        target_chart.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        target_chart.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=360)
        st.plotly_chart(target_chart, use_container_width=True)

    if not store_df.empty:
        store_chart = px.bar(
            store_df.head(10),
            x="store_name",
            y="gmv",
            title="GMV por loja",
            color="avg_discount_pct",
            color_continuous_scale="Tealgrn",
        )
        store_chart.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=360)
        st.plotly_chart(store_chart, use_container_width=True)

    col_top, col_bottom = st.columns(2)
    with col_top:
        st.subheader("Top performers")
        st.dataframe(top_df[["seller_name", "gmv", "orders", "avg_ticket", "target_gmv_pct"]], use_container_width=True)
    with col_bottom:
        st.subheader("Bottom performers")
        st.dataframe(bottom_df[["seller_name", "gmv", "orders", "avg_ticket", "target_gmv_pct"]], use_container_width=True)

    st.header("Controladoria")
    k1, k2, k3 = st.columns(3)
    k1.metric("Margem Bruta", _format_pct(float(totals["gross_margin_pct"])))
    k2.metric("Custo", _format_currency(float(totals["cogs"])))
    k3.metric("Impacto de Descontos", _format_currency(float(totals["discount_impact"])))

    if not category_df.empty:
        category_chart = px.bar(
            category_df,
            x="category",
            y="net_profit",
            title="Lucro por categoria",
            color="gmv",
            color_continuous_scale="Aggrnyl",
        )
        category_chart.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=340)
        st.plotly_chart(category_chart, use_container_width=True)

    if not channel_df.empty:
        channel_chart = px.bar(
            channel_df,
            x="channel",
            y="net_profit",
            title="Lucro por canal",
            color="gmv",
            color_continuous_scale="Sunsetdark",
        )
        channel_chart.update_layout(margin=dict(l=10, r=10, t=50, b=10), height=340)
        st.plotly_chart(channel_chart, use_container_width=True)

    st.subheader("Cancelamentos e devolucoes")
    st.dataframe(status_df, use_container_width=True)

    if auto_refresh:
        time.sleep(refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()