
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ERCOT LMP Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background-color: #0d0d1a; }
    [data-testid="stSidebar"] { background-color: #111125; }
    .stMetric { background: #111125; border: 1px solid #1e1e3a; border-radius: 10px; padding: 10px; }
    h1, h2, h3 { color: #00d4ff !important; }
</style>
""", unsafe_allow_html=True)

# ── ERCOT API ──────────────────────────────────────────────────────────────────
DAM_LMP_URL = "https://api.ercot.com/api/public-reports/np4-183-cd/dam_hourly_lmp"

def get_headers():
    key = st.secrets.get("ERCOT_API_KEY", "")
    h = {"Accept": "application/json"}
    if key:
        h["Ocp-Apim-Subscription-Key"] = key
    return h

# ── Nodes ──────────────────────────────────────────────────────────────────────
NODES = [
    "HB_HOUSTON", "HB_NORTH", "HB_SOUTH", "HB_WEST",
    "LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST",
    "LZ_AEN", "LZ_CPS", "LZ_LCRA", "LZ_RAYBN"
]
COLORS = ["#00d4ff", "#ff6b6b", "#51cf66", "#ffd43b", "#cc5de8", "#ff922b"]

def hex_to_rgba(hex_color, alpha=0.15):
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f"rgba({r},{g},{b},{alpha})"

# ── Fetch DAM Hourly LMP ───────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def fetch_dam_lmp(bus_name: str, date_from: str, date_to: str) -> pd.DataFrame:
    try:
        all_records = []
        page = 1
        while True:
            params = {
                "busName": bus_name,
                "deliveryDateFrom": date_from,
                "deliveryDateTo": date_to,
                "size": 9999,
                "page": page
            }
            resp = requests.get(DAM_LMP_URL, headers=get_headers(), params=params, timeout=20)
            resp.raise_for_status()
            raw = resp.json()

            # Extract records from nested data structure
            data = raw.get("data", {})
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # ERCOT returns data as dict of field arrays
                records = []
                fields = raw.get("fields", [])
                field_names = [f["name"] for f in fields]
                rows = data.get("rows", data.get("data", []))
                if rows and isinstance(rows[0], list):
                    for row in rows:
                        records.append(dict(zip(field_names, row)))
                else:
                    records = rows
            else:
                records = []

            all_records.extend(records)

            meta = raw.get("_meta", {})
            total_pages = meta.get("totalPages", 1)
            if page >= total_pages:
                break
            page += 1

        if not all_records:
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        df.columns = [c.lower() for c in df.columns]

        # Find date, hour, and LMP columns
        date_col  = next((c for c in df.columns if c in ["deliverydate","delivery_date","date"]), None)
        hour_col  = next((c for c in df.columns if "hour" in c), None)
        lmp_col   = next((c for c in df.columns if "lmp" in c or "price" in c), None)

        if not date_col or not lmp_col:
            st.warning(f"Unexpected columns from API: {list(df.columns)}")
            return pd.DataFrame()

        df["lmp"] = pd.to_numeric(df[lmp_col], errors="coerce")
        if hour_col:
            df["datetime"] = pd.to_datetime(df[date_col]) + pd.to_timedelta(
                pd.to_numeric(df[hour_col], errors="coerce").fillna(0) - 1, unit="h"
            )
        else:
            df["datetime"] = pd.to_datetime(df[date_col])

        df["node"] = bus_name
        return df[["datetime", "lmp", "node"]].dropna().sort_values("datetime")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            st.warning(f"⚠️ **{bus_name}**: API key required. Add `ERCOT_API_KEY` to Streamlit Secrets. Using demo data.")
        else:
            st.warning(f"⚠️ **{bus_name}**: HTTP {e.response.status_code}. Using demo data.")
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ **{bus_name}**: {str(e)[:120]}. Using demo data.")
        return pd.DataFrame()

# ── Demo Data Fallback ─────────────────────────────────────────────────────────
def generate_demo_data(node: str, date_from: str, date_to: str, agg: str) -> pd.DataFrame:
    import numpy as np
    seed = sum(ord(c) for c in node) % 1000
    np.random.seed(seed)
    freq = "h" if agg == "Hourly" else "D"
    dates = pd.date_range(start=date_from, end=date_to, freq=freq)
    n = len(dates)
    if n == 0:
        return pd.DataFrame()
    base = 28 + (seed % 35)
    t = np.arange(n)
    seasonal  = np.sin(t / (365 * (1 if freq=="D" else 24)) * 2 * np.pi) * 18
    daily     = np.sin(t / (1 if freq=="D" else 24) * 2 * np.pi) * 10
    longterm  = np.sin(t / (3650 * (1 if freq=="D" else 24)) * 2 * np.pi) * 12
    noise     = np.random.normal(0, 6, n)
    spikes    = np.where(np.random.random(n) < 0.02, np.random.uniform(80, 250, n), 0)
    negspikes = np.where(np.random.random(n) < 0.005, -np.random.uniform(10, 40, n), 0)
    lmp = base + seasonal + daily + longterm + noise + spikes + negspikes
    return pd.DataFrame({"datetime": dates, "lmp": lmp.round(2), "node": node})

# ── Aggregate ──────────────────────────────────────────────────────────────────
def aggregate(df: pd.DataFrame, agg: str) -> pd.DataFrame:
    if df.empty or agg == "Hourly":
        return df
    if agg == "Daily":
        df = df.copy(); df["period"] = df["datetime"].dt.normalize()
    elif agg == "Monthly":
        df = df.copy(); df["period"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
    elif agg == "Yearly":
        df = df.copy(); df["period"] = df["datetime"].dt.to_period("Y").dt.to_timestamp()
    grp = df.groupby(["period","node"])["lmp"].agg(["mean","max","min","std"]).reset_index()
    grp.columns = ["datetime","node","lmp","lmp_max","lmp_min","lmp_std"]
    return grp.sort_values("datetime")

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ ERCOT LMP")
    st.markdown("---")

    selected_nodes = st.multiselect("Select Bus Nodes", NODES, default=["HB_HOUSTON"], max_selections=6)

    st.markdown("### Time Range")
    preset = st.selectbox("Quick Select", [
        "Today","Yesterday","Last 7 Days","Last 30 Days",
        "Last 3 Months","Last 1 Year","Last 3 Years","Last 5 Years","Last 10 Years","Custom"
    ])
    today = datetime.today().date()
    preset_map = {
        "Today": (today, today),
        "Yesterday": (today - timedelta(days=1), today - timedelta(days=1)),
        "Last 7 Days": (today - timedelta(days=7), today),
        "Last 30 Days": (today - timedelta(days=30), today),
        "Last 3 Months": (today - timedelta(days=90), today),
        "Last 1 Year": (today - timedelta(days=365), today),
        "Last 3 Years": (today - timedelta(days=1095), today),
        "Last 5 Years": (today - timedelta(days=1825), today),
        "Last 10 Years": (today - timedelta(days=3650), today),
        "Custom": (today - timedelta(days=30), today),
    }
    default_from, default_to = preset_map[preset]
    if preset == "Custom":
        date_from = st.date_input("From", value=default_from)
        date_to   = st.date_input("To",   value=default_to)
    else:
        date_from, date_to = default_from, default_to
        st.caption(f"📅 {date_from} → {date_to}")

    agg        = st.selectbox("Aggregation", ["Daily","Monthly","Yearly","Hourly"])
    chart_type = st.selectbox("Chart Type",  ["Area","Line","Bar"])
    show_band  = st.checkbox("Show Min/Max Band", value=True)
    show_mavg  = st.checkbox("Show Moving Average", value=True)
    ma_window  = st.slider("MA Window (periods)", 3, 30, 7) if show_mavg else 7

    st.markdown("---")
    st.markdown("### API Status")
    has_key = bool(st.secrets.get("ERCOT_API_KEY",""))
    if has_key:
        st.success("🔑 API Key configured")
    else:
        st.info("ℹ️ No API key — showing demo data\n\nAdd `ERCOT_API_KEY` in **Settings → Secrets**")

# ── Main ───────────────────────────────────────────────────────────────────────
st.title("⚡ ERCOT LMP Dashboard")
st.caption("Day-Ahead Market (DAM) Hourly Locational Marginal Prices — NP4-183-CD ($/MWh)")

if not selected_nodes:
    st.warning("Please select at least one bus node from the sidebar.")
    st.stop()

# ── Load Data ──────────────────────────────────────────────────────────────────
all_dfs = []
with st.spinner("Loading ERCOT LMP data..."):
    for node in selected_nodes:
        df = fetch_dam_lmp(node, str(date_from), str(date_to))
        if df.empty:
            df = generate_demo_data(node, str(date_from), str(date_to), agg)
            df["_demo"] = True
        else:
            df["_demo"] = False
        all_dfs.append(df)

combined_raw = pd.concat(all_dfs, ignore_index=True)
combined     = aggregate(combined_raw, agg)
has_band     = "lmp_max" in combined.columns

# ── Stats ──────────────────────────────────────────────────────────────────────
st.markdown("### 📊 Summary Statistics")
cols = st.columns(len(selected_nodes))
for i, node in enumerate(selected_nodes):
    nd = combined[combined["node"] == node]
    if nd.empty:
        continue
    avg_lmp = nd["lmp"].mean()
    max_lmp = nd["lmp_max"].max() if has_band else nd["lmp"].max()
    min_lmp = nd["lmp_min"].min() if has_band else nd["lmp"].min()
    is_demo = combined_raw[combined_raw["node"]==node].get("_demo", pd.Series([True])).iloc[0]
    with cols[i]:
        st.metric(f"📍 {node}", f"${avg_lmp:.2f}/MWh", delta="demo data" if is_demo else "live data")
        st.caption(f"Max: **${max_lmp:.2f}** | Min: **${min_lmp:.2f}**")

st.markdown("---")

# ── Chart ──────────────────────────────────────────────────────────────────────
st.markdown("### 📈 LMP Price Chart")
fig = go.Figure()

for i, node in enumerate(selected_nodes):
    nd = combined[combined["node"] == node].sort_values("datetime")
    if nd.empty:
        continue
    color = COLORS[i % len(COLORS)]
    fill_color = hex_to_rgba(color, 0.15)
    band_color  = hex_to_rgba(color, 0.08)

    # Min/Max band
    if show_band and has_band and agg != "Hourly":
        fig.add_trace(go.Scatter(
            x=pd.concat([nd["datetime"], nd["datetime"][::-1]]),
            y=pd.concat([nd["lmp_max"], nd["lmp_min"][::-1]]),
            fill="toself", fillcolor=band_color,
            line=dict(color="rgba(0,0,0,0)"),
            name=f"{node} range", showlegend=False, hoverinfo="skip"
        ))

    # Main line / area / bar
    hover = f"<b>{node}</b><br>%{{x|%Y-%m-%d}}<br>Avg: $%{{y:.2f}}/MWh<extra></extra>"
    if chart_type == "Bar":
        fig.add_trace(go.Bar(
            x=nd["datetime"], y=nd["lmp"], name=node,
            marker_color=color, opacity=0.85,
            hovertemplate=hover
        ))
    elif chart_type == "Area":
        fig.add_trace(go.Scatter(
            x=nd["datetime"], y=nd["lmp"], name=node,
            mode="lines", line=dict(color=color, width=2),
            fill="tozeroy", fillcolor=fill_color,
            hovertemplate=hover
        ))
    else:
        fig.add_trace(go.Scatter(
            x=nd["datetime"], y=nd["lmp"], name=node,
            mode="lines", line=dict(color=color, width=2),
            hovertemplate=hover
        ))

    # Moving average
    if show_mavg and len(nd) >= ma_window and chart_type != "Bar":
        ma = nd["lmp"].rolling(ma_window, min_periods=1).mean()
        fig.add_trace(go.Scatter(
            x=nd["datetime"], y=ma,
            name=f"{node} {ma_window}-MA",
            mode="lines", line=dict(color=color, width=1.5, dash="dot"),
            hovertemplate=f"<b>{node} MA</b><br>${{y:.2f}}/MWh<extra></extra>"
        ))

fig.update_layout(
    template="plotly_dark",
    paper_bgcolor="#0d0d1a",
    plot_bgcolor="#111125",
    height=500,
    legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1, font=dict(size=11)),
    xaxis=dict(
        showgrid=True, gridcolor="#1e1e3a",
        rangeslider=dict(visible=True, bgcolor="#0d0d1a", thickness=0.06),
        title="Date"
    ),
    yaxis=dict(
        showgrid=True, gridcolor="#1e1e3a",
        tickprefix="$", ticksuffix="/MWh",
        title="LMP ($/MWh)",
        zeroline=True, zerolinecolor="#333"
    ),
    hovermode="x unified",
    margin=dict(l=60, r=20, t=50, b=40),
    barmode="group"
)
st.plotly_chart(fig, use_container_width=True)

# ── Node Comparison ────────────────────────────────────────────────────────────
if len(selected_nodes) > 1:
    st.markdown("### 🔀 Node Comparison")
    comp = []
    for node in selected_nodes:
        nd = combined[combined["node"] == node]
        if nd.empty: continue
        comp.append({
            "Node": node,
            "Avg LMP": round(nd["lmp"].mean(), 2),
            "Max LMP": round(nd["lmp_max"].max() if has_band else nd["lmp"].max(), 2),
            "Min LMP": round(nd["lmp_min"].min() if has_band else nd["lmp"].min(), 2),
            "Std Dev": round(nd["lmp"].std(), 2),
        })
    comp_df = pd.DataFrame(comp)
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=comp_df["Node"], y=comp_df["Avg LMP"],
        marker_color=COLORS[:len(comp_df)], opacity=0.9,
        text=[f"${v:.2f}" for v in comp_df["Avg LMP"]],
        textposition="outside", name="Avg LMP"
    ))
    fig2.update_layout(
        template="plotly_dark", paper_bgcolor="#0d0d1a", plot_bgcolor="#111125",
        height=300, yaxis=dict(tickprefix="$", title="$/MWh"),
        margin=dict(l=50, r=20, t=20, b=40)
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.dataframe(comp_df.set_index("Node"), use_container_width=True)

# ── Raw Data ───────────────────────────────────────────────────────────────────
with st.expander("📄 View & Download Raw Data"):
    show_df = combined_raw[["datetime","lmp","node"]].sort_values("datetime", ascending=False)
    st.dataframe(show_df, use_container_width=True)
    csv = show_df.to_csv(index=False).encode()
    st.download_button("⬇️ Download CSV", csv, "ercot_lmp.csv", "text/csv", use_container_width=True)

st.caption("Source: ERCOT Public API — NP4-183-CD DAM Hourly LMP | Endpoint: /np4-183-cd/dam_hourly_lmp")
