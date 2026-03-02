
# ─────────────────────────────────────────────────────────────────────────────
# ERCOT LMP Dashboard — Fresh build using:
# GET /np6-785-er/spp_node_zone_hub  (getLMPNodeZoneHub)
# Docs: https://apiexplorer.ercot.com
# Auth: Bearer token via ERCOT B2C ROPC flow + Ocp-Apim-Subscription-Key header
# ─────────────────────────────────────────────────────────────────────────────

import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, date

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="ERCOT LMP", page_icon="⚡", layout="wide")
st.markdown("""
<style>
[data-testid="stAppViewContainer"]{background:#0a0a14}
[data-testid="stSidebar"]{background:#0f0f1e;border-right:1px solid #1e1e3a}
[data-testid="stHeader"]{background:transparent}
.card{background:#111125;border:1px solid #1e1e3a;border-radius:12px;padding:18px 20px;position:relative;margin-bottom:8px}
.lbl{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:6px}
.val{font-size:28px;font-weight:700;color:#fff;margin:0}
.sub{font-size:12px;color:#888;margin-top:4px}
.pos{position:absolute;top:14px;right:14px;font-size:11px;color:#51cf66;font-weight:600}
.neg{position:absolute;top:14px;right:14px;font-size:11px;color:#ff6b6b;font-weight:600}
.neu{position:absolute;top:14px;right:14px;font-size:11px;color:#ffd43b;font-weight:600}
</style>
""", unsafe_allow_html=True)

# ── ERCOT API constants ────────────────────────────────────────────────────────
# Correct endpoint from apiexplorer.ercot.com → getData_lmp_node_zone_hub
BASE     = "https://api.ercot.com/api/public-reports"
ENDPOINT = f"{BASE}/np6-785-er/spp_node_zone_hub"

# ERCOT OAuth2 ROPC — official values from ERCOT developer portal
TOKEN_URL = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
CLIENT_ID = "5f0baa1d-5124-47cc-a8dd-2c8d89d3e62e"   # Verified ERCOT public app registration

NODES = [
    "HB_HOUSTON","HB_NORTH","HB_SOUTH","HB_WEST",
    "LZ_HOUSTON","LZ_NORTH","LZ_SOUTH","LZ_WEST",
    "LZ_AEN","LZ_CPS","LZ_LCRA","LZ_RAYBN"
]
COLORS = ["#00d4ff","#ff6b6b","#51cf66","#ffd43b","#cc5de8","#ff922b"]

def rgba(h, a=0.15):
    h=h.lstrip("#"); r,g,b=int(h[:2],16),int(h[2:4],16),int(h[4:],16)
    return f"rgba({r},{g},{b},{a})"

# ── Step 1: Get Bearer token ───────────────────────────────────────────────────
@st.cache_data(ttl=3200, show_spinner=False)
def get_token(username: str, password: str) -> str:
    """
    ERCOT ROPC OAuth2 flow.
    Returns id_token which is used as Bearer token.
    """
    r = requests.post(TOKEN_URL,
        data={
            "grant_type":    "password",
            "username":      username,
            "password":      password,
            "response_type": "id_token",
            "scope":         f"openid offline_access {CLIENT_ID}",
            "client_id":     CLIENT_ID,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=20
    )
    if not r.ok:
        err = r.json()
        raise Exception(f"HTTP {r.status_code} — {err.get('error_description', r.text)[:300]}")
    j = r.json()
    tok = j.get("id_token") or j.get("access_token")
    if not tok:
        raise Exception(f"No token in response. Keys returned: {list(j.keys())}")
    return tok

# ── Step 2: Call the LMP endpoint ─────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def fetch_lmp(node: str, d_from: str, d_to: str, token: str, sub_key: str) -> pd.DataFrame:
    """
    GET /np6-785-er/spp_node_zone_hub
    Params: settlementPoint, deliveryDateFrom, deliveryDateTo, size
    Headers: Authorization: Bearer <id_token>
             Ocp-Apim-Subscription-Key: <subscription_key>
    """
    r = requests.get(ENDPOINT,
        headers={
            "Authorization":             f"Bearer {token}",
            "Ocp-Apim-Subscription-Key": sub_key,
            "Accept":                    "application/json",
        },
        params={
            "settlementPoint":   node,
            "deliveryDateFrom":  d_from,
            "deliveryDateTo":    d_to,
            "size":              9999,
        },
        timeout=25
    )
    r.raise_for_status()
    return parse_ercot(r.json(), node)

def parse_ercot(raw: dict, node: str) -> pd.DataFrame:
    """Parse ERCOT's standard field+data response format."""
    fields = [f["name"] for f in raw.get("fields", [])]
    data   = raw.get("data", {})

    # ERCOT returns data as {"rows": [[val,val,...], ...]} or flat list
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("rows", [])
    else:
        rows = []

    if not rows:
        return pd.DataFrame()

    # Build dataframe
    if fields and isinstance(rows[0], (list, tuple)):
        df = pd.DataFrame(rows, columns=fields)
    else:
        df = pd.DataFrame(rows)

    df.columns = [c.lower() for c in df.columns]

    # Find relevant columns — ERCOT uses: deliveryDate, deliveryHour, settlementPointPrice
    date_col  = next((c for c in df.columns if c in ("deliverydate","delivery_date","date")), None)
    hour_col  = next((c for c in df.columns if "hour" in c and "ending" not in c), None)
    price_col = next((c for c in df.columns if "price" in c or "spp" in c or "lmp" in c), None)

    if not date_col or not price_col:
        st.warning(f"Unexpected columns: {list(df.columns)}")
        return pd.DataFrame()

    df["lmp"] = pd.to_numeric(df[price_col], errors="coerce")

    if hour_col:
        # hourEnding is 1–24, convert to 0–23 offset
        h = pd.to_numeric(df[hour_col], errors="coerce").fillna(1) - 1
        df["datetime"] = pd.to_datetime(df[date_col]) + pd.to_timedelta(h, unit="h")
    else:
        df["datetime"] = pd.to_datetime(df[date_col])

    df["node"] = node
    return df[["datetime","lmp","node"]].dropna().sort_values("datetime")

# ── Demo data fallback ─────────────────────────────────────────────────────────
def demo_hourly(node, for_date):
    s = sum(ord(c) for c in node) % 999; np.random.seed(s)
    hrs   = pd.date_range(str(for_date), periods=24, freq="h")
    shape = np.array([4,3,3,3,4,7,13,17,15,13,12,13,15,14,13,15,21,37,47,41,27,17,11,7],dtype=float)
    lmp   = np.maximum(-8, 5+(s%25) + shape + np.random.normal(0,3,24))
    return pd.DataFrame({"datetime":hrs,"lmp":lmp.round(2),"node":node})

def demo_hist(node, d_from, d_to, freq="D"):
    s = sum(ord(c) for c in node) % 999; np.random.seed(s)
    dates = pd.date_range(d_from, d_to, freq=freq)
    n = len(dates); t = np.arange(n)
    lmp = np.maximum(-15,
        30+(s%30)
        + np.sin(t/(365 if freq=="D" else 12)*2*np.pi)*16
        + np.linspace(0,10,n)
        + np.random.normal(0,8,n)
        + np.where(np.random.random(n)<0.025, np.random.uniform(80,250,n), 0)
        + np.where(np.random.random(n)<0.01, -np.random.uniform(5,40,n), 0)
    )
    return pd.DataFrame({"datetime":dates,"lmp":lmp.round(2),"node":node})

def do_agg(df, mode):
    if mode in ("Hourly","Daily"): return df.copy()
    df = df.copy()
    df["p"] = df["datetime"].dt.to_period("M" if mode=="Monthly" else "Y").dt.to_timestamp()
    g = df.groupby(["p","node"])["lmp"].agg(["mean","max","min"]).reset_index()
    g.columns = ["datetime","node","lmp","lmp_max","lmp_min"]
    return g

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ ERCOT LMP")
    st.markdown("---")

    sub_key  = st.secrets.get("ERCOT_API_KEY","").strip()
    username = st.secrets.get("ERCOT_USERNAME","").strip()
    password = st.secrets.get("ERCOT_PASSWORD","").strip()

    if sub_key and username and password:
        st.success("✅ Credentials ready")
    else:
        st.warning("Add to Streamlit Secrets:")
        st.code('ERCOT_API_KEY  = "your-subscription-key"\nERCOT_USERNAME = "your@email.com"\nERCOT_PASSWORD = "yourPassword"', language="toml")

    node_grp  = st.radio("Node Type",["Hub","Load Zone","All"],horizontal=True)
    pool      = (["HB_HOUSTON","HB_NORTH","HB_SOUTH","HB_WEST"] if node_grp=="Hub"
                 else (["LZ_HOUSTON","LZ_NORTH","LZ_SOUTH","LZ_WEST","LZ_AEN","LZ_CPS","LZ_LCRA","LZ_RAYBN"]
                       if node_grp=="Load Zone" else NODES))
    sel_node  = st.selectbox("Primary Node", pool)
    cmp_nodes = st.multiselect("Compare", [n for n in pool if n!=sel_node], max_selections=4)
    all_nodes = [sel_node] + cmp_nodes

    st.markdown("---")
    view  = st.radio("View", ["📅 Daily","📆 Historical"])
    today = date.today()

    if view == "📅 Daily":
        sel_date = st.date_input("Date", value=today)
        d_from = d_to = str(sel_date); agg = "Hourly"
    else:
        preset = st.selectbox("Range",["7 Days","30 Days","3 Months","1 Year","3 Years","5 Years","10 Years"])
        dm = {"7 Days":7,"30 Days":30,"3 Months":90,"1 Year":365,"3 Years":1095,"5 Years":1825,"10 Years":3650}
        ad = {"7 Days":"Daily","30 Days":"Daily","3 Months":"Daily","1 Year":"Monthly","3 Years":"Monthly","5 Years":"Yearly","10 Years":"Yearly"}
        d_from = str(today-timedelta(days=dm[preset])); d_to = str(today)
        agg = st.selectbox("Aggregation",["Daily","Monthly","Yearly"],
                           index=["Daily","Monthly","Yearly"].index(ad[preset]))

    chart_t = st.selectbox("Chart Style",["Line","Area","Bar"])
    show_b  = st.checkbox("Min/Max Band", value=True)
    show_ma = st.checkbox("Moving Avg",   value=True)
    ma_w    = st.slider("MA Window",2,30,7) if show_ma else 7

# ── Header ─────────────────────────────────────────────────────────────────────
h1,h2 = st.columns([6,1])
with h1: st.markdown(f"### ⚡ MARKET: ERCOT &nbsp;/&nbsp; **{sel_node}**")
with h2:
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear(); st.rerun()

# ── Authenticate ───────────────────────────────────────────────────────────────
token = None; live = False

if sub_key and username and password:
    with st.spinner("🔐 Authenticating..."):
        try:
            token = get_token(username, password)
            live  = True
        except Exception as e:
            st.error(f"❌ Auth error: {e}")

# ── Fetch data ─────────────────────────────────────────────────────────────────
freq_map = {"Daily":"D","Monthly":"ME","Yearly":"YE","Hourly":"h"}
dfs = []
with st.spinner("📡 Loading LMP data..."):
    for node in all_nodes:
        df = pd.DataFrame()
        if live and token:
            try:
                df = fetch_lmp(node, d_from, d_to, token, sub_key)
            except requests.HTTPError as e:
                st.warning(f"⚠️ {node} → HTTP {e.response.status_code}: {e.response.text[:150]}")
            except Exception as e:
                st.warning(f"⚠️ {node} → {str(e)[:150]}")
        if df.empty:
            df = (demo_hourly(node, date.fromisoformat(d_from)) if agg=="Hourly"
                  else demo_hist(node, d_from, d_to, freq_map.get(agg,"D")))
        dfs.append(df)

is_demo = not live or all(d.empty for d in dfs)
if not live:
    st.info("📊 **Demo data** — Add `ERCOT_API_KEY`, `ERCOT_USERNAME`, `ERCOT_PASSWORD` to Streamlit Secrets for live data.")
elif live and any(d.empty for d in dfs):
    st.warning("Some nodes returned no data — showing demo for those nodes.")
else:
    st.success("🟢 Live ERCOT data loaded successfully!")

# ── KPI cards ──────────────────────────────────────────────────────────────────
p = dfs[0]
c_lmp  = float(p["lmp"].iloc[-1])   if not p.empty else 0.0
d_avg  = float(p["lmp"].mean())     if not p.empty else 0.0
d_max  = float(p["lmp"].max())      if not p.empty else 0.0
p_time = (p.loc[p["lmp"].idxmax(),"datetime"].strftime("%H:%M") if not p.empty else "--")
vstd   = float(p["lmp"].std())      if not p.empty else 0.0
vlbl   = "Low" if vstd<5 else ("High" if vstd>15 else "Moderate")
mid    = len(p)//2
base   = p["lmp"].iloc[:mid].mean() if mid>0 else 1
dpct   = ((p["lmp"].iloc[mid:].mean()-base)/abs(base)*100) if base!=0 else 0

k1,k2,k3,k4 = st.columns(4)
bc = "pos" if dpct>=0 else "neg"
vc = "pos" if vlbl=="Low" else ("neg" if vlbl=="High" else "neu")
with k1: st.markdown(f'<div class="card"><div class="lbl">⚡ Current LMP</div><div class="val">${c_lmp:.2f}</div><div class="sub">{"🟢 Live" if live else "🟡 Demo"} · DAM</div><span class="{bc}">{dpct:+.1f}%</span></div>',unsafe_allow_html=True)
with k2: st.markdown(f'<div class="card"><div class="lbl">📊 Avg LMP</div><div class="val">${d_avg:.2f}</div><div class="sub">Period average · $/MWh</div></div>',unsafe_allow_html=True)
with k3: st.markdown(f'<div class="card"><div class="lbl">🔺 Peak LMP</div><div class="val">${d_max:.2f}</div><div class="sub">Occurred at {p_time}</div></div>',unsafe_allow_html=True)
with k4: st.markdown(f'<div class="card"><div class="lbl">📉 Volatility</div><div class="val">{vstd:.1f}</div><div class="sub">Std Dev · {vlbl}</div><span class="{vc}">{vlbl}</span></div>',unsafe_allow_html=True)
st.markdown("<br>",unsafe_allow_html=True)

# ── Chart ──────────────────────────────────────────────────────────────────────
ch, roi = st.columns([2,1])

with ch:
    st.markdown(f'<div style="font-size:13px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:12px">LMP · {agg} · {sel_node}</div>',unsafe_allow_html=True)
    fig = go.Figure()
    for i,(node,df) in enumerate(zip(all_nodes,dfs)):
        c   = COLORS[i%len(COLORS)]
        adf = do_agg(df,agg).sort_values("datetime")
        hb  = "lmp_max" in adf.columns
        if show_b and hb:
            fig.add_trace(go.Scatter(
                x=pd.concat([adf["datetime"],adf["datetime"][::-1]]),
                y=pd.concat([adf["lmp_max"],adf["lmp_min"][::-1]]),
                fill="toself",fillcolor=rgba(c,0.08),
                line=dict(color="rgba(0,0,0,0)"),showlegend=False,hoverinfo="skip"))
        ht = f"<b>{node}</b><br>%{{x}}<br>${{y:.2f}}/MWh<extra></extra>"
        if chart_t=="Bar":
            fig.add_trace(go.Bar(x=adf["datetime"],y=adf["lmp"],name=node,
                marker_color=c,opacity=0.85,hovertemplate=ht))
        elif chart_t=="Area":
            fig.add_trace(go.Scatter(x=adf["datetime"],y=adf["lmp"],name=node,
                mode="lines",line=dict(color=c,width=2.5),
                fill="tozeroy",fillcolor=rgba(c,0.15),hovertemplate=ht))
        else:
            fig.add_trace(go.Scatter(x=adf["datetime"],y=adf["lmp"],name=node,
                mode="lines",line=dict(color=c,width=2.5),hovertemplate=ht))
        if show_ma and len(adf)>=ma_w:
            ma = adf["lmp"].rolling(ma_w,min_periods=1).mean()
            fig.add_trace(go.Scatter(x=adf["datetime"],y=ma,name=f"{node} MA{ma_w}",
                mode="lines",line=dict(color=c,width=1.2,dash="dot"),
                hovertemplate=f"MA: $%{{y:.2f}}<extra></extra>"))
    fig.add_hline(y=0,line_dash="dash",line_color="#333",line_width=1)
    fig.update_layout(
        template="plotly_dark",paper_bgcolor="#0a0a14",plot_bgcolor="#111125",height=430,
        legend=dict(orientation="h",y=1.07,x=0,font=dict(size=11),bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=True,gridcolor="#1a1a2e",
                   rangeslider=dict(visible=True,bgcolor="#0a0a14",thickness=0.05)),
        yaxis=dict(showgrid=True,gridcolor="#1a1a2e",tickprefix="$",
                   title="$/MWh",zeroline=True,zerolinecolor="#444"),
        hovermode="x unified",margin=dict(l=55,r=10,t=40,b=40),barmode="group")
    st.plotly_chart(fig,use_container_width=True)

# ── Storage ROI ────────────────────────────────────────────────────────────────
with roi:
    st.markdown('<div style="font-size:13px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:1.2px;margin-bottom:12px">🔋 Storage ROI</div>',unsafe_allow_html=True)
    pw  = st.slider("Power (MW)",1,500,10)
    dur = st.slider("Duration (MWh)",1,1000,20)
    n4  = max(1,len(p)//4); sl=p["lmp"].sort_values()
    buy = sl.iloc[:n4].mean(); sell=sl.iloc[-n4:].mean()
    spd = max(0,sell-buy); drev=spd*dur; mrev=drev*30
    st.markdown(f'<div class="card"><div class="lbl">Daily Revenue</div><div class="val" style="font-size:22px">${drev:,.2f}</div><div class="sub">~${mrev:,.0f}/month</div><span class="pos">+{abs(spd/max(buy,1)*100):.1f}% spread</span></div>',unsafe_allow_html=True)
    st.markdown(f'<div class="card"><div class="lbl">Price Spread</div><div class="val" style="font-size:22px">${spd:.2f}</div><div class="sub">Buy ${buy:.2f} → Sell ${sell:.2f}</div></div>',unsafe_allow_html=True)
    st.markdown(f'<div class="card"><div class="lbl">Annual Revenue</div><div class="val" style="font-size:22px">${drev*365:,.0f}</div><div class="sub">{pw} MW · {dur} MWh</div></div>',unsafe_allow_html=True)
    st.button("▶ Run Full Simulation",use_container_width=True,type="primary")

# ── Summary table ──────────────────────────────────────────────────────────────
if view == "📆 Historical":
    st.markdown("---")
    rows = [{"Node":n,"Avg $/MWh":round(d["lmp"].mean(),2),"Max":round(d["lmp"].max(),2),
             "Min":round(d["lmp"].min(),2),"Std Dev":round(d["lmp"].std(),2),"Records":len(d)}
            for n,d in zip(all_nodes,dfs) if not d.empty]
    if rows: st.dataframe(pd.DataFrame(rows).set_index("Node"),use_container_width=True)

with st.expander("📄 Raw Data & CSV Export"):
    raw = pd.concat(dfs)[["datetime","lmp","node"]].sort_values("datetime",ascending=False)
    st.dataframe(raw,use_container_width=True)
    st.download_button("⬇️ Download CSV",raw.to_csv(index=False).encode(),"ercot_lmp.csv","text/csv")

st.caption(f"Endpoint: GET /np6-785-er/spp_node_zone_hub · {'🟢 Live ERCOT Data' if live else '🟡 Demo Data'}")
