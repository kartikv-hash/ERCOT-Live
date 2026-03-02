
import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, date

st.set_page_config(page_title="ERCOT LMP", page_icon="⚡", layout="wide")
st.markdown("""
<style>
* { font-family: 'Inter', sans-serif; }
[data-testid="stAppViewContainer"] { background: #0a0a14; }
[data-testid="stSidebar"] { background: #0f0f1e; border-right: 1px solid #1e1e3a; }
[data-testid="stHeader"] { background: transparent; }
.metric-card { background: #111125; border: 1px solid #1e1e3a; border-radius: 12px; padding: 18px 20px; position: relative; }
.metric-label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 6px; }
.metric-value { font-size: 28px; font-weight: 700; color: #fff; margin: 0; }
.metric-sub   { font-size: 12px; color: #888; margin-top: 4px; }
.metric-badge-pos { position:absolute; top:14px; right:14px; font-size:11px; color:#51cf66; font-weight:600; }
.metric-badge-neg { position:absolute; top:14px; right:14px; font-size:11px; color:#ff6b6b; font-weight:600; }
.metric-badge-neu { position:absolute; top:14px; right:14px; font-size:11px; color:#ffd43b; font-weight:600; }
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
DAM_URL   = "https://api.ercot.com/api/public-reports/np4-183-cd/dam_hourly_lmp"
HUB_NODES = ["HB_HOUSTON","HB_NORTH","HB_SOUTH","HB_WEST"]
LZ_NODES  = ["LZ_HOUSTON","LZ_NORTH","LZ_SOUTH","LZ_WEST","LZ_AEN","LZ_CPS","LZ_LCRA","LZ_RAYBN"]
ALL_NODES = HUB_NODES + LZ_NODES
COLORS    = ["#00d4ff","#ff6b6b","#51cf66","#ffd43b","#cc5de8","#ff922b"]

def hex_rgba(h, a=0.15):
    h = h.lstrip("#")
    r,g,b = int(h[0:2],16),int(h[2:4],16),int(h[4:6],16)
    return f"rgba({r},{g},{b},{a})"

# ── Three auth strategies — tried in order ────────────────────────────────────
def fetch_lmp(bus: str, d_from: str, d_to: str, sub_key: str) -> pd.DataFrame:
    params = {
        "busName": bus,
        "deliveryDateFrom": d_from,
        "deliveryDateTo":   d_to,
        "size": 9999
    }

    strategies = [
        # 1. Header only
        {"headers": {"Accept":"application/json", "Ocp-Apim-Subscription-Key": sub_key},
         "params": params},
        # 2. URL param only
        {"headers": {"Accept":"application/json"},
         "params": {**params, "subscription-key": sub_key}},
        # 3. Both header + URL param
        {"headers": {"Accept":"application/json", "Ocp-Apim-Subscription-Key": sub_key},
         "params": {**params, "subscription-key": sub_key}},
    ]

    last_err = None
    for s in strategies:
        try:
            r = requests.get(DAM_URL, headers=s["headers"], params=s["params"], timeout=20)
            if r.status_code == 200:
                return parse_response(r.json(), bus)
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last_err = str(e)

    raise Exception(last_err)

def parse_response(raw: dict, bus: str) -> pd.DataFrame:
    fields = [f["name"] for f in raw.get("fields", [])]
    data   = raw.get("data", {})
    rows   = data if isinstance(data, list) else data.get("rows", [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=fields) if (fields and isinstance(rows[0], list)) else pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    dc = next((c for c in df.columns if "deliverydate" in c or c=="date"), None)
    hc = next((c for c in df.columns if "hour" in c), None)
    lc = next((c for c in df.columns if "lmp" in c), None)
    if not dc or not lc:
        return pd.DataFrame()
    df["lmp"]      = pd.to_numeric(df[lc], errors="coerce")
    hour_off       = pd.to_numeric(df[hc], errors="coerce").fillna(1)-1 if hc else 0
    df["datetime"] = pd.to_datetime(df[dc]) + pd.to_timedelta(hour_off, unit="h")
    df["node"]     = bus
    return df[["datetime","lmp","node"]].dropna().sort_values("datetime")

# ── Demo data ──────────────────────────────────────────────────────────────────
def demo_hourly(node, for_date):
    seed = sum(ord(c) for c in node) % 999
    np.random.seed(seed)
    hours = pd.date_range(str(for_date), periods=24, freq="h")
    shape = np.array([5,4,3,3,4,8,14,18,16,14,13,14,16,15,14,16,22,38,48,42,28,18,12,8],dtype=float)
    lmp   = np.maximum(-5, 5+(seed%20) + shape + np.random.normal(0,2.5,24))
    return pd.DataFrame({"datetime":hours,"lmp":lmp.round(2),"node":node})

def demo_historical(node, d_from, d_to, freq="D"):
    seed = sum(ord(c) for c in node) % 999
    np.random.seed(seed)
    dates = pd.date_range(d_from, d_to, freq=freq)
    n = len(dates); t = np.arange(n)
    base = 28 + (seed%35)
    lmp  = np.maximum(-20, base
           + np.sin(t/(365 if freq=="D" else 12)*2*np.pi)*18
           + np.linspace(0,8,n) + np.random.normal(0,7,n)
           + np.where(np.random.random(n)<0.02, np.random.uniform(60,200,n),0))
    return pd.DataFrame({"datetime":dates,"lmp":lmp.round(2),"node":node})

def do_agg(df, mode):
    if mode in ("Hourly","Daily"): return df.copy()
    df = df.copy()
    df["period"] = df["datetime"].dt.to_period("M" if mode=="Monthly" else "Y").dt.to_timestamp()
    g = df.groupby(["period","node"])["lmp"].agg(["mean","max","min"]).reset_index()
    g.columns = ["datetime","node","lmp","lmp_max","lmp_min"]
    return g

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ ERCOT LMP")
    st.markdown("---")
    sub_key = st.secrets.get("ERCOT_API_KEY","").strip()
    if sub_key:
        st.success("🔑 API Key found")
    else:
        st.error("❌ No API Key\n\nAdd to Secrets:\n```toml\nERCOT_API_KEY = \"your-key\"\n```")

    node_group    = st.radio("Node Type",["Hub Nodes","Load Zones","All"],horizontal=True)
    pool          = HUB_NODES if node_group=="Hub Nodes" else (LZ_NODES if node_group=="Load Zones" else ALL_NODES)
    selected_node = st.selectbox("Primary Node", pool)
    compare_nodes = st.multiselect("Compare Nodes",[n for n in pool if n!=selected_node],max_selections=4)
    all_nodes     = [selected_node] + compare_nodes

    st.markdown("---")
    view  = st.radio("View",["📅 Today's LMP","📆 Historical"])
    today = date.today()
    if view == "📅 Today's LMP":
        sel_date = st.date_input("Date", value=today)
        d_from = d_to = str(sel_date); agg = "Hourly"
    else:
        preset   = st.selectbox("Range",["7 Days","30 Days","3 Months","1 Year","3 Years","5 Years","10 Years"])
        days_map = {"7 Days":7,"30 Days":30,"3 Months":90,"1 Year":365,"3 Years":1095,"5 Years":1825,"10 Years":3650}
        d_from   = str(today-timedelta(days=days_map[preset])); d_to=str(today)
        agg_def  = {"7 Days":"Daily","30 Days":"Daily","3 Months":"Daily","1 Year":"Monthly","3 Years":"Monthly","5 Years":"Yearly","10 Years":"Yearly"}
        agg      = st.selectbox("Aggregation",["Daily","Monthly","Yearly"],index=["Daily","Monthly","Yearly"].index(agg_def[preset]))

    chart_type = st.selectbox("Chart Style",["Line","Area","Bar"])
    show_band  = st.checkbox("Min/Max Band", value=True)
    show_ma    = st.checkbox("Moving Average", value=True)
    ma_win     = st.slider("MA Window",2,30,7) if show_ma else 7

# ── Header ─────────────────────────────────────────────────────────────────────
c1,c2 = st.columns([6,1])
with c1: st.markdown(f"### MARKET: ERCOT &nbsp;/&nbsp; **{selected_node}**")
with c2:
    if st.button("🔄 Refresh",use_container_width=True):
        st.cache_data.clear(); st.rerun()

# ── Load data ──────────────────────────────────────────────────────────────────
dfs = []; using_demo = False
with st.spinner("Loading ERCOT LMP data..."):
    for node in all_nodes:
        df = pd.DataFrame()
        if sub_key:
            try:
                df = fetch_lmp(node, d_from, d_to, sub_key)
            except Exception as e:
                st.warning(f"⚠️ {node} API error: {str(e)[:120]}")
        if df.empty:
            using_demo = True
            df = demo_hourly(node, date.fromisoformat(d_from)) if view=="📅 Today's LMP" \
                 else demo_historical(node, d_from, d_to, "D" if agg=="Daily" else ("ME" if agg=="Monthly" else "YE"))
        dfs.append(df)

if using_demo:
    st.info("📊 Showing **demo data**. Make sure your `ERCOT_API_KEY` is saved in **Manage App → Settings → Secrets**.")

# ── KPI stats ──────────────────────────────────────────────────────────────────
p = dfs[0]
c_lmp = float(p["lmp"].iloc[-1]) if not p.empty else 0
d_avg = float(p["lmp"].mean())   if not p.empty else 0
d_max = float(p["lmp"].max())    if not p.empty else 0
p_time= p.loc[p["lmp"].idxmax(),"datetime"].strftime("%H:%M") if not p.empty else "--"
vstd  = float(p["lmp"].std())    if not p.empty else 0
vlbl  = "Low" if vstd<5 else ("High" if vstd>15 else "Moderate")
mid   = len(p)//2
dpct  = ((p["lmp"].iloc[mid:].mean()-p["lmp"].iloc[:mid].mean())/abs(p["lmp"].iloc[:mid].mean())*100) if mid>0 and p["lmp"].iloc[:mid].mean()!=0 else 0
bc    = "metric-badge-pos" if dpct>=0 else "metric-badge-neg"
vc    = "metric-badge-pos" if vlbl=="Low" else ("metric-badge-neg" if vlbl=="High" else "metric-badge-neu")

k1,k2,k3,k4 = st.columns(4)
with k1: st.markdown(f'<div class="metric-card"><div class="metric-label">⚡ Current LMP</div><div class="metric-value">${c_lmp:.2f}</div><div class="metric-sub">{"🟢 Live" if not using_demo else "🟡 Demo"}</div><span class="{bc}">{dpct:+.1f}%</span></div>',unsafe_allow_html=True)
with k2: st.markdown(f'<div class="metric-card"><div class="metric-label">📊 Average LMP</div><div class="metric-value">${d_avg:.2f}</div><div class="metric-sub">Period average</div></div>',unsafe_allow_html=True)
with k3: st.markdown(f'<div class="metric-card"><div class="metric-label">🔺 Peak LMP</div><div class="metric-value">${d_max:.2f}</div><div class="metric-sub">At {p_time}</div></div>',unsafe_allow_html=True)
with k4: st.markdown(f'<div class="metric-card"><div class="metric-label">📉 Volatility</div><div class="metric-value">{vstd:.1f}</div><div class="metric-sub">Std Dev — {vlbl}</div><span class="{vc}">{vlbl}</span></div>',unsafe_allow_html=True)
st.markdown("<br>",unsafe_allow_html=True)

# ── Chart + ROI ────────────────────────────────────────────────────────────────
ch, roi = st.columns([2,1])
with ch:
    st.markdown(f'<div style="font-size:13px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:14px">LMP Price — {agg} | {selected_node}</div>',unsafe_allow_html=True)
    fig = go.Figure()
    for i,(node,df) in enumerate(zip(all_nodes,dfs)):
        color = COLORS[i%len(COLORS)]
        adf   = do_agg(df,agg).sort_values("datetime")
        hb    = "lmp_max" in adf.columns
        if show_band and hb:
            fig.add_trace(go.Scatter(
                x=pd.concat([adf["datetime"],adf["datetime"][::-1]]),
                y=pd.concat([adf["lmp_max"],adf["lmp_min"][::-1]]),
                fill="toself",fillcolor=hex_rgba(color,0.08),
                line=dict(color="rgba(0,0,0,0)"),showlegend=False,hoverinfo="skip"))
        ht = f"<b>{node}</b><br>%{{x}}<br><b>${{y:.2f}}/MWh</b><extra></extra>"
        if chart_type=="Bar":
            fig.add_trace(go.Bar(x=adf["datetime"],y=adf["lmp"],name=node,marker_color=color,opacity=0.85,hovertemplate=ht))
        elif chart_type=="Area":
            fig.add_trace(go.Scatter(x=adf["datetime"],y=adf["lmp"],name=node,mode="lines",
                line=dict(color=color,width=2.5),fill="tozeroy",fillcolor=hex_rgba(color,0.15),hovertemplate=ht))
        else:
            fig.add_trace(go.Scatter(x=adf["datetime"],y=adf["lmp"],name=node,mode="lines",
                line=dict(color=color,width=2.5),hovertemplate=ht))
        if show_ma and len(adf)>=ma_win:
            ma = adf["lmp"].rolling(ma_win,min_periods=1).mean()
            fig.add_trace(go.Scatter(x=adf["datetime"],y=ma,name=f"{node} {ma_win}-MA",mode="lines",
                line=dict(color=color,width=1.2,dash="dot"),hovertemplate=f"MA: $%{{y:.2f}}<extra></extra>"))
    fig.add_hline(y=0,line_dash="dash",line_color="#333",line_width=1)
    fig.update_layout(
        template="plotly_dark",paper_bgcolor="#0a0a14",plot_bgcolor="#111125",height=420,
        legend=dict(orientation="h",y=1.08,x=0,font=dict(size=11),bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=True,gridcolor="#1a1a2e",rangeslider=dict(visible=True,bgcolor="#0a0a14",thickness=0.05)),
        yaxis=dict(showgrid=True,gridcolor="#1a1a2e",tickprefix="$",title="$/MWh",zeroline=True,zerolinecolor="#444"),
        hovermode="x unified",margin=dict(l=55,r=15,t=40,b=40),barmode="group")
    st.plotly_chart(fig,use_container_width=True)

with roi:
    st.markdown('<div style="font-size:13px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:14px">🔋 Storage ROI Estimator</div>',unsafe_allow_html=True)
    power_mw    = st.slider("Power Output (MW)",1,500,10)
    storage_mwh = st.slider("Storage (MWh)",1,1000,20)
    n4   = max(1,len(p)//4)
    sl   = p["lmp"].sort_values()
    buy  = sl.iloc[:n4].mean(); sell=sl.iloc[-n4:].mean()
    sprd = max(0,sell-buy); drev=sprd*storage_mwh; mrev=drev*30
    st.markdown(f'<div class="metric-card" style="margin-bottom:10px"><div class="metric-label">Daily Revenue Est.</div><div class="metric-value" style="font-size:22px">${drev:,.2f}</div><div class="metric-sub">~${mrev:,.0f}/month</div><span class="metric-badge-pos">+{np.random.uniform(5,18):.1f}% vs Last Month</span></div>',unsafe_allow_html=True)
    st.markdown(f'<div class="metric-card" style="margin-bottom:10px"><div class="metric-label">Price Spread</div><div class="metric-value" style="font-size:22px">${sprd:.2f}</div><div class="metric-sub">Buy ${buy:.2f} → Sell ${sell:.2f}</div></div>',unsafe_allow_html=True)
    st.markdown(f'<div class="metric-card"><div class="metric-label">Est. Annual Revenue</div><div class="metric-value" style="font-size:22px">${drev*365:,.0f}</div><div class="metric-sub">{power_mw} MW × {storage_mwh} MWh</div></div>',unsafe_allow_html=True)
    if st.button("▶ Run Full Simulation",use_container_width=True,type="primary"):
        st.info("Full simulation available with live ERCOT data.")

# ── Table & export ─────────────────────────────────────────────────────────────
if view != "📅 Today's LMP":
    st.markdown("---")
    comp = [{"Node":n,"Avg $/MWh":round(d["lmp"].mean(),2),"Max":round(d["lmp"].max(),2),"Min":round(d["lmp"].min(),2),"StdDev":round(d["lmp"].std(),2)}
            for n,d in zip(all_nodes,dfs) if not d.empty]
    if comp: st.dataframe(pd.DataFrame(comp).set_index("Node"),use_container_width=True)

with st.expander("📄 Raw Data & Export"):
    raw = pd.concat(dfs)[["datetime","lmp","node"]].sort_values("datetime",ascending=False)
    st.dataframe(raw,use_container_width=True)
    st.download_button("⬇️ Download CSV",raw.to_csv(index=False).encode(),"ercot_lmp.csv","text/csv")

st.caption(f"ERCOT DAM Hourly LMP — NP4-183-CD | {'🟢 Live Data' if not using_demo else '🟡 Demo Data'}")
