
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ERCOT LMP Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Dark Theme CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0d0d1a; }
    .stMetric { background: #111125; border: 1px solid #1e1e3a; border-radius: 10px; padding: 10px; }
    h1, h2, h3 { color: #00d4ff; }
    .stSelectbox label, .stDateInput label, .stMultiSelect label { color: #aaa; }
</style>
""", unsafe_allow_html=True)

# ── ERCOT API Config ───────────────────────────────────────────────────────────
# ERCOT API endpoints to try in order
ENDPOINTS = [
    "https://api.ercot.com/api/public-reports/np6-785-er/spp_node_zone_hub_da_lmp",
    "https://api.ercot.com/api/public-reports/np6-788-cd/spp_hrly_actual_fcast_geo",
    "https://api.ercot.com/api/public-reports/np4-190-cd/lmp_electrical_bus",
]
HEADERS = {
    "Accept": "application/json",
    "Ocp-Apim-Subscription-Key": st.secrets.get("ERCOT_API_KEY", ""),
    "Authorization": f"Bearer {st.secrets.get('ERCOT_TOKEN', '')}"
}

# ── Available Nodes ────────────────────────────────────────────────────────────
NODES = [
    "HB_HOUSTON", "HB_NORTH", "HB_SOUTH", "HB_WEST",
    "LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST",
    "LZ_AEN", "LZ_CPS", "LZ_LCRA", "LZ_RAYBN"
]

# ── Fetch from ERCOT API ───────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def fetch_ercot_data(node: str, date_from: str, date_to: str) -> pd.DataFrame:
    params = {
        "deliveryDateFrom": date_from,
        "deliveryDateTo": date_to,
        "settlementPoint": node,
        "busName": node,
        "size": 9999
    }
    last_err = None
    for endpoint in ENDPOINTS:
        try:
            resp = requests.get(endpoint, headers=HEADERS, params=params, timeout=15)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            raw = resp.json()
            records = raw.get("data", raw.get("items", raw.get("_items", [])))
            if not records:
                continue
            df = pd.DataFrame(records)
            df.columns = [c.lower() for c in df.columns]
            date_col = next((c for c in df.columns if "date" in c or "time" in c), None)
            price_col = next((c for c in df.columns if "price" in c or "lmp" in c or "spp" in c), None)
            if not date_col or not price_col:
                continue
            df = df.rename(columns={date_col: "datetime", price_col: "lmp"})
            df["datetime"] = pd.to_datetime(df["datetime"])
            df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")
            df["node"] = node
            st.success(f"Live data loaded from ERCOT API for {node}!")
            return df[["datetime", "lmp", "node"]].dropna()
        except Exception as e:
            last_err = e
            continue
    st.info(f"ℹ️ Using demo data for **{node}** — [Get a free ERCOT API key](https://apiexplorer.ercot.com) and add it to Streamlit Secrets to load live data.")
    return generate_demo_data(node, date_from, date_to)

# ── Demo Data Fallback ─────────────────────────────────────────────────────────
def generate_demo_data(node: str, date_from: str, date_to: str) -> pd.DataFrame:
    import numpy as np
    seed = sum(ord(c) for c in node)
    np.random.seed(seed % 1000)
    dates = pd.date_range(start=date_from, end=date_to, freq="D")
    base = 30 + (seed % 40)
    n = len(dates)
    seasonal = np.sin(np.linspace(0, 4 * np.pi, n)) * 15
    noise = np.random.normal(0, 8, n)
    spikes = np.where(np.random.random(n) < 0.03, np.random.uniform(50, 150, n), 0)
    lmp = np.maximum(0, base + seasonal + noise + spikes)
    return pd.DataFrame({"datetime": dates, "lmp": lmp.round(2), "node": node})

# ── Aggregate Data ─────────────────────────────────────────────────────────────
def aggregate(df: pd.DataFrame, agg: str) -> pd.DataFrame:
    if df.empty:
        return df
    if agg == "Hourly":
        return df
    elif agg == "Daily":
        df["period"] = df["datetime"].dt.date
    elif agg == "Monthly":
        df["period"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
    elif agg == "Yearly":
        df["period"] = df["datetime"].dt.to_period("Y").dt.to_timestamp()
    return df.groupby(["period", "node"])["lmp"].mean().reset_index().rename(columns={"period": "datetime"})

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚡ ERCOT LMP")
    st.markdown("---")

    selected_nodes = st.multiselect(
        "Select Nodes", NODES,
        default=["HB_HOUSTON"],
        max_selections=6
    )

    st.markdown("### Date Range")
    preset = st.selectbox("Quick Range", ["1 Day","1 Week","1 Month","3 Months","1 Year","3 Years","5 Years","10 Years"])
    preset_map = {
        "1 Day": 1, "1 Week": 7, "1 Month": 30, "3 Months": 90,
        "1 Year": 365, "3 Years": 1095, "5 Years": 1825, "10 Years": 3650
    }
    default_from = datetime.today() - timedelta(days=preset_map[preset])
    date_from = st.date_input("From", value=default_from)
    date_to = st.date_input("To", value=datetime.today())

    agg = st.selectbox("Aggregation", ["Daily", "Monthly", "Yearly", "Hourly"])
    chart_type = st.selectbox("Chart Type", ["Area", "Line", "Bar"])
    show_mavg = st.checkbox("Show 7-period Moving Avg", value=True)

    refresh = st.button("🔄 Refresh Data", use_container_width=True)

# ── Main ───────────────────────────────────────────────────────────────────────
st.title("⚡ ERCOT LMP Dashboard")
st.caption("Locational Marginal Pricing — Settlement Point Prices ($/MWh)")

if not selected_nodes:
    st.warning("Please select at least one node from the sidebar.")
    st.stop()

# ── Fetch Data ─────────────────────────────────────────────────────────────────
with st.spinner("Fetching ERCOT data..."):
    all_dfs = []
    for node in selected_nodes:
        df = fetch_ercot_data(node, str(date_from), str(date_to))
        if not df.empty:
            all_dfs.append(df)

if not all_dfs:
    st.error("No data returned. Check your date range or API key.")
    st.stop()

combined = pd.concat(all_dfs, ignore_index=True)
combined = aggregate(combined, agg)

# ── Stats Row ──────────────────────────────────────────────────────────────────
st.markdown("### Summary Statistics")
cols = st.columns(len(selected_nodes))
for i, node in enumerate(selected_nodes):
    node_df = combined[combined["node"] == node]
    if not node_df.empty:
        with cols[i]:
            st.metric(f"📍 {node}", f"${node_df['lmp'].mean():.2f} avg")
            st.caption(f"Max: ${node_df['lmp'].max():.2f} | Min: ${node_df['lmp'].min():.2f}")

st.markdown("---")

# ── Main Chart ─────────────────────────────────────────────────────────────────
st.markdown("### LMP Price Chart")
COLORS = ["#00d4ff","#ff6b6b","#51cf66","#ffd43b","#cc5de8","#ff922b"]
fig = go.Figure()

for i, node in enumerate(selected_nodes):
    node_df = combined[combined["node"] == node].sort_values("datetime")
    color = COLORS[i % len(COLORS)]

    if chart_type == "Area":
        fig.add_trace(go.Scatter(
            x=node_df["datetime"], y=node_df["lmp"],
            name=node, mode="lines", line=dict(color=color, width=2),
            fill="tozeroy", fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.15)",
            hovertemplate=f"<b>{node}</b><br>%{{x|%Y-%m-%d}}<br>${{y:.2f}}/MWh<extra></extra>"
        ))
    elif chart_type == "Line":
        fig.add_trace(go.Scatter(
            x=node_df["datetime"], y=node_df["lmp"],
            name=node, mode="lines", line=dict(color=color, width=2),
            hovertemplate=f"<b>{node}</b><br>%{{x|%Y-%m-%d}}<br>${{y:.2f}}/MWh<extra></extra>"
        ))
    elif chart_type == "Bar":
        fig.add_trace(go.Bar(
            x=node_df["datetime"], y=node_df["lmp"],
            name=node, marker_color=color, opacity=0.85,
            hovertemplate=f"<b>{node}</b><br>%{{x|%Y-%m-%d}}<br>${{y:.2f}}/MWh<extra></extra>"
        ))

    if show_mavg and chart_type != "Bar" and len(node_df) > 7:
        node_df["mavg"] = node_df["lmp"].rolling(7, min_periods=1).mean()
        fig.add_trace(go.Scatter(
            x=node_df["datetime"], y=node_df["mavg"],
            name=f"{node} (7-MA)", mode="lines",
            line=dict(color=color, width=1.5, dash="dot"),
            hovertemplate=f"<b>{node} MA</b><br>${{y:.2f}}/MWh<extra></extra>"
        ))

fig.update_layout(
    template="plotly_dark",
    paper_bgcolor="#0d0d1a",
    plot_bgcolor="#111125",
    height=480,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=dict(showgrid=True, gridcolor="#1e1e3a", rangeslider=dict(visible=True)),
    yaxis=dict(showgrid=True, gridcolor="#1e1e3a", tickprefix="$", title="$/MWh"),
    hovermode="x unified",
    margin=dict(l=50, r=20, t=40, b=40)
)
st.plotly_chart(fig, use_container_width=True)

# ── Node Comparison ────────────────────────────────────────────────────────────
if len(selected_nodes) > 1:
    st.markdown("### Node Comparison — Average LMP")
    comp_data = []
    for node in selected_nodes:
        node_df = combined[combined["node"] == node]
        comp_data.append({"Node": node, "Avg LMP ($/MWh)": round(node_df["lmp"].mean(), 2),
                          "Max": round(node_df["lmp"].max(), 2), "Min": round(node_df["lmp"].min(), 2)})
    comp_df = pd.DataFrame(comp_data)
    fig2 = go.Figure(go.Bar(
        x=comp_df["Node"], y=comp_df["Avg LMP ($/MWh)"],
        marker_color=COLORS[:len(selected_nodes)],
        text=comp_df["Avg LMP ($/MWh)"].apply(lambda x: f"${x:.2f}"),
        textposition="outside"
    ))
    fig2.update_layout(
        template="plotly_dark", paper_bgcolor="#0d0d1a", plot_bgcolor="#111125",
        height=300, yaxis=dict(tickprefix="$", title="$/MWh"),
        margin=dict(l=50, r=20, t=20, b=40)
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.dataframe(comp_df.set_index("Node"), use_container_width=True)

# ── Raw Data ───────────────────────────────────────────────────────────────────
with st.expander("📄 View Raw Data"):
    st.dataframe(combined.sort_values("datetime", ascending=False), use_container_width=True)
    csv = combined.to_csv(index=False).encode()
    st.download_button("⬇️ Download CSV", csv, "ercot_lmp.csv", "text/csv")

st.caption("Data: ERCOT Public API — np6-785-er/spp_node_zone_hub | Auto-refreshes every 5 min")
