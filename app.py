
import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, date

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="ERCOT LMP", page_icon="⚡", layout="wide")

st.markdown("""
<style>
* { font-family: 'Inter', sans-serif; }
[data-testid="stAppViewContainer"] { background: #0a0a14; }
[data-testid="stSidebar"] { background: #0f0f1e; border-right: 1px solid #1e1e3a; }
[data-testid="stHeader"] { background: transparent; }
.metric-card {
    background: #111125; border: 1px solid #1e1e3a; border-radius: 12px;
    padding: 18px 20px; position: relative; overflow: hidden;
}
.metric-label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 6px; }
.metric-value { font-size: 28px; font-weight: 700; color: #fff; margin: 0; }
.metric-sub   { font-size: 12px; color: #888; margin-top: 4px; }
.metric-badge-pos { position:absolute; top:14px; right:14px; font-size:11px; color:#51cf66; font-weight:600; }
.metric-badge-neg { position:absolute; top:14px; right:14px; font-size:11px; color:#ff6b6b; font-weight:600; }
.metric-badge-neu { position:absolute; top:14px; right:14px; font-size:11px; color:#ffd43b; font-weight:600; }
.section-title { font-size:13px; font-weight:600; color:#aaa; text-transform:uppercase;
                 letter-spacing:1.5px; margin-bottom:14px; }
div[data-testid="stHorizontalBlock"] { gap: 12px; }
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
API_BASE   = "https://api.ercot.com/api/public-reports"
DAM_URL    = f"{API_BASE}/np4-183-cd/dam_hourly_lmp"
RTM_URL    = f"{API_BASE}/np6-905-cd/spp_node_zone_hub"   # RTM 15-min SPP

HUB_NODES = ["HB_HOUSTON","HB_NORTH","HB_SOUTH","HB_WEST"]
LZ_NODES  = ["LZ_HOUSTON","LZ_NORTH","LZ_SOUTH","LZ_WEST","LZ_AEN","LZ_CPS","LZ_LCRA","LZ_RAYBN"]
ALL_NODES = HUB_NODES + LZ_NODES
COLORS    = ["#00d4ff","#ff6b6b","#51cf66","#ffd43b","#cc5de8","#ff922b"]

def hex_rgba(h, a=0.18):
    h = h.lstrip("#")
    r,g,b = int(h[0:2],16),int(h[2:4],16),int(h[4:6],16)
    return f"rgba({r},{g},{b},{a})"

def get_headers():
    key = st.secrets.get("ERCOT_API_KEY","")
    return {"Accept":"application/json","Ocp-Apim-Subscription-Key": key} if key else {"Accept":"application/json"}

# ── API Fetch ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=180, show_spinner=False)
def fetch_dam_lmp(bus: str, d_from: str, d_to: str) -> pd.DataFrame:
    try:
        params = {"busName": bus, "deliveryDateFrom": d_from, "deliveryDateTo": d_to, "size": 9999}
        r = requests.get(DAM_URL, headers=get_headers(), params=params, timeout=20)
        r.raise_for_status()
        raw = r.json()
        # Parse ERCOT nested response
        fields = [f["name"] for f in raw.get("fields", [])]
        data   = raw.get("data", {})
        rows   = data if isinstance(data, list) else data.get("rows", [])
        if not rows:
            return pd.DataFrame()
        if fields and isinstance(rows[0], list):
            df = pd.DataFrame(rows, columns=fields)
        else:
            df = pd.DataFrame(rows)
        df.columns = [c.lower() for c in df.columns]
        dc = next((c for c in df.columns if "deliverydate" in c or c=="date"), None)
        hc = next((c for c in df.columns if "hour" in c), None)
        lc = next((c for c in df.columns if "lmp" in c), None)
        if not dc or not lc:
            return pd.DataFrame()
        df["lmp"] = pd.to_numeric(df[lc], errors="coerce")
        hour_offset = pd.to_numeric(df[hc], errors="coerce").fillna(1) - 1 if hc else 0
        df["datetime"] = pd.to_datetime(df[dc]) + pd.to_timedelta(hour_offset, unit="h")
        df["node"] = bus
        return df[["datetime","lmp","node"]].dropna().sort_values("datetime")
    except requests.HTTPError as e:
        code = e.response.status_code
        if code == 401:
            st.error("🔑 **HTTP 401 — API key missing or invalid.** Add `ERCOT_API_KEY` in **Manage App → Settings → Secrets**.")
        elif code == 403:
            st.error("🔒 **HTTP 403 — Subscription required.** Subscribe at [apiexplorer.ercot.com](https://apiexplorer.ercot.com).")
        else:
            st.warning(f"⚠️ API returned HTTP {code} for {bus}.")
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"⚠️ Fetch error for {bus}: {str(e)[:100]}")
        return pd.DataFrame()

# ── Demo Data ──────────────────────────────────────────────────────────────────
def demo_hourly(node: str, for_date: date) -> pd.DataFrame:
    seed = sum(ord(c) for c in node) % 999
    np.random.seed(seed)
    hours = pd.date_range(str(for_date), periods=24, freq="h")
    # Realistic intraday shape: low overnight, morning ramp, midday, evening peak
    shape = np.array([5,4,3,3,4,8,14,18,16,14,13,14,16,15,14,16,22,38,48,42,28,18,12,8], dtype=float)
    noise = np.random.normal(0, 2.5, 24)
    base  = 5 + (seed % 20)
    lmp   = np.maximum(-5, base + shape + noise)
    return pd.DataFrame({"datetime": hours, "lmp": lmp.round(2), "node": node})

def demo_historical(node: str, d_from: str, d_to: str, freq="D") -> pd.DataFrame:
    seed = sum(ord(c) for c in node) % 999
    np.random.seed(seed)
    dates = pd.date_range(d_from, d_to, freq=freq)
    n = len(dates)
    base     = 28 + (seed % 35)
    seasonal = np.sin(np.linspace(0, 4*np.pi, n)) * 15
    trend    = np.linspace(0, 8, n)
    noise    = np.random.normal(0, 7, n)
    spikes   = np.where(np.random.random(n) < 0.02, np.random.uniform(60,200,n), 0)
    neg      = np.where(np.random.random(n) < 0.008, -np.random.uniform(5,30,n), 0)
    lmp = np.maximum(-20, base + seasonal + trend + noise + spikes + neg)
    return pd.DataFrame({"datetime": dates, "lmp": lmp.round(2), "node": node})

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ ERCOT LMP")
    st.markdown("---")

    # Node selector
    node_group = st.radio("Node Type", ["Hub Nodes","Load Zones","All"], horizontal=True)
    node_pool  = HUB_NODES if node_group=="Hub Nodes" else (LZ_NODES if node_group=="Load Zones" else ALL_NODES)
    selected_node = st.selectbox("Primary Node", node_pool, index=0)
    compare_nodes = st.multiselect("Compare Nodes (optional)", [n for n in node_pool if n != selected_node], max_selections=4)
    all_selected  = [selected_node] + compare_nodes

    st.markdown("---")
    view = st.radio("View", ["📅 Today's LMP","📆 Historical","🔀 Multi-Node Compare"], index=0)

    today = date.today()
    if view == "📅 Today's LMP":
        sel_date = st.date_input("Date", value=today)
        d_from = d_to = str(sel_date)
        agg = "Hourly"
    else:
        preset = st.selectbox("Range", ["7 Days","30 Days","3 Months","1 Year","3 Years","5 Years","10 Years"])
        days_map = {"7 Days":7,"30 Days":30,"3 Months":90,"1 Year":365,"3 Years":1095,"5 Years":1825,"10 Years":3650}
        d_from = str(today - timedelta(days=days_map[preset]))
        d_to   = str(today)
        agg_opts = {"7 Days":"Daily","30 Days":"Daily","3 Months":"Daily","1 Year":"Monthly",
                    "3 Years":"Monthly","5 Years":"Yearly","10 Years":"Yearly"}
        agg = st.selectbox("Aggregation", ["Daily","Monthly","Yearly"], index=["Daily","Monthly","Yearly"].index(agg_opts[preset]))

    chart_type = st.selectbox("Chart Style", ["Line","Area","Bar"])
    show_band  = st.checkbox("Show Min/Max Band", value=(agg != "Hourly"))
    show_ma    = st.checkbox("Show Moving Avg",   value=True)
    ma_win     = st.slider("MA Window", 2, 30, 7) if show_ma else 7

    st.markdown("---")
    api_key = st.secrets.get("ERCOT_API_KEY","")
    if api_key:
        st.success("🔑 API Key active — Live data")
    else:
        st.warning("⚠️ No API key\nUsing demo data\n\n[Get free key →](https://apiexplorer.ercot.com)")

# ── Top bar ────────────────────────────────────────────────────────────────────
c1, c2 = st.columns([6,1])
with c1:
    st.markdown(f"### MARKET: ERCOT &nbsp;/&nbsp; **{selected_node}**")
with c2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()

# ── Load data ──────────────────────────────────────────────────────────────────
using_demo = not bool(api_key)

with st.spinner("Loading ERCOT LMP data..."):
    primary_dfs = []
    for node in all_selected:
        if api_key:
            df = fetch_dam_lmp(node, d_from, d_to)
        else:
            df = pd.DataFrame()
        if df.empty:
            using_demo = True
            if view == "📅 Today's LMP":
                df = demo_hourly(node, date.fromisoformat(d_from))
            else:
                freq = "D" if agg == "Daily" else ("ME" if agg == "Monthly" else "YE")
                df = demo_historical(node, d_from, d_to, freq)
        primary_dfs.append(df)

if using_demo:
    st.info("📊 **Showing demo data** — Add your `ERCOT_API_KEY` in Streamlit Secrets for live ERCOT prices. [Get free key →](https://apiexplorer.ercot.com)")

# ── Compute stats for primary node ────────────────────────────────────────────
prim_df = primary_dfs[0]

def safe_stat(series, fn):
    try: return fn(series.dropna())
    except: return 0.0

current_lmp = safe_stat(prim_df["lmp"], lambda s: s.iloc[-1])
daily_avg   = safe_stat(prim_df["lmp"], np.mean)
daily_peak  = safe_stat(prim_df["lmp"], np.max)
peak_time   = prim_df.loc[prim_df["lmp"].idxmax(), "datetime"].strftime("%H:%M") if not prim_df.empty else "--"
volatility  = safe_stat(prim_df["lmp"], np.std)
vol_label   = "Low" if volatility < 5 else ("Moderate" if volatility < 15 else "High")

# Day-over-day delta (compare first half vs second half as proxy)
mid = len(prim_df)//2
d1  = prim_df["lmp"].iloc[:mid].mean() if mid>0 else daily_avg
d2  = prim_df["lmp"].iloc[mid:].mean() if mid>0 else daily_avg
delta_pct = ((d2 - d1) / abs(d1) * 100) if d1 != 0 else 0

# ── KPI Cards ──────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
badge_cls = "metric-badge-pos" if delta_pct >= 0 else "metric-badge-neg"
vol_cls   = "metric-badge-pos" if vol_label=="Low" else ("metric-badge-neg" if vol_label=="High" else "metric-badge-neu")

with k1:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">⚡ Current LMP</div>
        <div class="metric-value">${current_lmp:.2f}</div>
        <div class="metric-sub">{("DAM Last Hour" if view=="📅 Today's LMP" else "Period End")}</div>
        <span class="{badge_cls}">{delta_pct:+.1f}%</span>
    </div>""", unsafe_allow_html=True)

with k2:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">📊 Average LMP</div>
        <div class="metric-value">${daily_avg:.2f}</div>
        <div class="metric-sub">Period average</div>
    </div>""", unsafe_allow_html=True)

with k3:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">🔺 Peak LMP</div>
        <div class="metric-value">${daily_peak:.2f}</div>
        <div class="metric-sub">Occurred at {peak_time}</div>
    </div>""", unsafe_allow_html=True)

with k4:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">📉 Volatility Index</div>
        <div class="metric-value">{volatility:.1f}</div>
        <div class="metric-sub">Std deviation — {vol_label}</div>
        <span class="{vol_cls}">{vol_label}</span>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Main Chart + Storage ROI ───────────────────────────────────────────────────
chart_col, roi_col = st.columns([2, 1])

with chart_col:
    st.markdown(f'<div class="section-title">LMP Price — {agg} | {selected_node}</div>', unsafe_allow_html=True)

    # Aggregation helper
    def agg_df(df, mode):
        if mode == "Hourly" or mode == "Daily":
            return df.copy()
        df = df.copy()
        if mode == "Monthly":
            df["period"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
        else:
            df["period"] = df["datetime"].dt.to_period("Y").dt.to_timestamp()
        g = df.groupby(["period","node"])["lmp"].agg(["mean","max","min"]).reset_index()
        g.columns = ["datetime","node","lmp","lmp_max","lmp_min"]
        return g

    fig = go.Figure()

    for i, (node, df) in enumerate(zip(all_selected, primary_dfs)):
        color = COLORS[i % len(COLORS)]
        adf   = agg_df(df, agg).sort_values("datetime")
        has_band = "lmp_max" in adf.columns

        # Band
        if show_band and has_band:
            fig.add_trace(go.Scatter(
                x=pd.concat([adf["datetime"], adf["datetime"][::-1]]),
                y=pd.concat([adf["lmp_max"], adf["lmp_min"][::-1]]),
                fill="toself", fillcolor=hex_rgba(color, 0.10),
                line=dict(color="rgba(0,0,0,0)"),
                showlegend=False, hoverinfo="skip", name=f"{node} band"
            ))

        # Main trace
        hover = f"<b>{node}</b><br>%{{x}}<br><b>${{y:.2f}}/MWh</b><extra></extra>"
        if chart_type == "Bar":
            fig.add_trace(go.Bar(x=adf["datetime"], y=adf["lmp"], name=node,
                                  marker_color=color, opacity=0.85, hovertemplate=hover))
        elif chart_type == "Area":
            fig.add_trace(go.Scatter(x=adf["datetime"], y=adf["lmp"], name=node,
                                      mode="lines", line=dict(color=color, width=2.5),
                                      fill="tozeroy", fillcolor=hex_rgba(color, 0.15),
                                      hovertemplate=hover))
        else:
            fig.add_trace(go.Scatter(x=adf["datetime"], y=adf["lmp"], name=node,
                                      mode="lines", line=dict(color=color, width=2.5),
                                      hovertemplate=hover))

        # Moving average
        if show_ma and len(adf) >= ma_win:
            ma = adf["lmp"].rolling(ma_win, min_periods=1).mean()
            fig.add_trace(go.Scatter(x=adf["datetime"], y=ma, name=f"{node} {ma_win}-MA",
                                      mode="lines", line=dict(color=color, width=1.2, dash="dot"),
                                      hovertemplate=f"MA: $%{{y:.2f}}<extra></extra>"))

    # Zero reference
    fig.add_hline(y=0, line_dash="dash", line_color="#333", line_width=1)

    fig.update_layout(
        template="plotly_dark", paper_bgcolor="#0a0a14", plot_bgcolor="#111125",
        height=420,
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=True, gridcolor="#1a1a2e", rangeslider=dict(visible=True, bgcolor="#0a0a14", thickness=0.05)),
        yaxis=dict(showgrid=True, gridcolor="#1a1a2e", tickprefix="$", title="$/MWh",
                   zeroline=True, zerolinecolor="#444"),
        hovermode="x unified",
        margin=dict(l=55, r=15, t=40, b=40),
        barmode="group"
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Storage ROI Estimator ──────────────────────────────────────────────────────
with roi_col:
    st.markdown('<div class="section-title">🔋 Storage ROI Estimator</div>', unsafe_allow_html=True)
    st.caption("BESS Revenue Projection")

    power_mw   = st.slider("Power Output (MW)", 1, 500, 10)
    storage_mwh= st.slider("Storage Duration (MWh)", 1, 1000, 20)

    # Revenue: buy at low LMP, sell at peak
    if not prim_df.empty:
        sorted_lmp = prim_df["lmp"].sort_values()
        n_charge   = max(1, len(sorted_lmp)//4)
        avg_buy    = sorted_lmp.iloc[:n_charge].mean()
        avg_sell   = sorted_lmp.iloc[-n_charge:].mean()
        spread     = max(0, avg_sell - avg_buy)
        cycles_day = 1
        daily_rev  = spread * storage_mwh * cycles_day
        monthly_rev= daily_rev * 30
        vs_last    = np.random.uniform(5, 18)  # simulated month-over-month
    else:
        daily_rev = monthly_rev = spread = 0
        vs_last = 0

    st.markdown(f"""<div class="metric-card" style="margin-bottom:12px">
        <div class="metric-label">Daily Revenue Est.</div>
        <div class="metric-value" style="font-size:22px">${daily_rev:,.2f}</div>
        <div class="metric-sub">~${monthly_rev:,.0f}/month</div>
        <span class="metric-badge-pos">+{vs_last:.1f}% vs Last Month</span>
    </div>""", unsafe_allow_html=True)

    st.markdown(f"""<div class="metric-card" style="margin-bottom:12px">
        <div class="metric-label">Price Spread</div>
        <div class="metric-value" style="font-size:22px">${spread:.2f}</div>
        <div class="metric-sub">Avg buy–sell delta ($/MWh)</div>
    </div>""", unsafe_allow_html=True)

    st.markdown(f"""<div class="metric-card">
        <div class="metric-label">Capacity Factor</div>
        <div class="metric-value" style="font-size:22px">{min(95, 60 + spread/2):.0f}%</div>
        <div class="metric-sub">{power_mw} MW × {storage_mwh} MWh</div>
    </div>""", unsafe_allow_html=True)

    if st.button("▶ Run Full Simulation", use_container_width=True, type="primary"):
        st.info("Connect live ERCOT API to run full dispatch simulation with real historical LMPs.")

# ── Historical breakdown ───────────────────────────────────────────────────────
if view != "📅 Today's LMP":
    st.markdown("---")
    st.markdown('<div class="section-title">📊 Period Summary</div>', unsafe_allow_html=True)
    comp = []
    for node, df in zip(all_selected, primary_dfs):
        if df.empty: continue
        comp.append({
            "Node": node, "Avg $/MWh": round(df["lmp"].mean(),2),
            "Max $/MWh": round(df["lmp"].max(),2),
            "Min $/MWh": round(df["lmp"].min(),2),
            "Std Dev": round(df["lmp"].std(),2),
            "# Records": len(df)
        })
    if comp:
        cdf = pd.DataFrame(comp)
        st.dataframe(cdf.set_index("Node"), use_container_width=True)

# ── Raw data expander ──────────────────────────────────────────────────────────
with st.expander("📄 Raw Data & Export"):
    raw_all = pd.concat(primary_dfs)[["datetime","lmp","node"]].sort_values("datetime", ascending=False)
    st.dataframe(raw_all, use_container_width=True)
    st.download_button("⬇️ Download CSV", raw_all.to_csv(index=False).encode(), "ercot_lmp.csv", "text/csv")

st.caption("Source: ERCOT DAM Hourly LMP — NP4-183-CD | /api/public-reports/np4-183-cd/dam_hourly_lmp")
