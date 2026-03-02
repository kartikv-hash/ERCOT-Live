
import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
from io import BytesIO
import zipfile
import calendar
import re
import time

st.set_page_config(page_title="ERCOT LMP", page_icon="⚡", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;400;600&display=swap');
* { font-family: 'Rajdhani', sans-serif; }
[data-testid="stAppViewContainer"] {
    background: radial-gradient(ellipse at 20% 50%, #0a0015 0%, #000510 50%, #000a05 100%);
}
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #05001a 0%, #000d1a 100%);
    border-right: 1px solid #00ff9920;
}
[data-testid="stHeader"] { background: transparent; }
.title-glow { font-family:'Orbitron',monospace; font-size:26px; font-weight:900;
    color:#00ff99; text-shadow:0 0 20px #00ff9980,0 0 40px #00ff9940; letter-spacing:3px; }
.subtitle { font-size:12px; color:#00ff9950; letter-spacing:4px;
    text-transform:uppercase; margin-top:-6px; margin-bottom:20px; }
.card { background:linear-gradient(135deg,#050520 0%,#0a0a2e 100%);
    border:1px solid #00ff9930; border-radius:4px; padding:16px 20px;
    position:relative; margin-bottom:8px;
    box-shadow:0 0 20px #00ff9908,inset 0 0 20px #00000040;
    clip-path:polygon(0 0,calc(100% - 12px) 0,100% 12px,100% 100%,12px 100%,0 calc(100% - 12px)); }
.card::before { content:''; position:absolute; top:0; left:0; right:0; height:1px;
    background:linear-gradient(90deg,transparent,#00ff99,transparent); }
.lbl { font-size:10px; color:#00ff9970; text-transform:uppercase;
    letter-spacing:2px; margin-bottom:5px; font-family:'Orbitron',monospace; }
.val { font-size:30px; font-weight:700; color:#fff;
    text-shadow:0 0 10px #00ff9940; font-family:'Orbitron',monospace; margin:0; }
.sub { font-size:12px; color:#00ff9950; margin-top:4px; }
.bp { position:absolute; top:12px; right:14px; font-size:11px; color:#00ff99; font-weight:700; }
.bn { position:absolute; top:12px; right:14px; font-size:11px; color:#ff3366; font-weight:700; }
.bm { position:absolute; top:12px; right:14px; font-size:11px; color:#ffaa00; font-weight:700; }
.shdr { font-family:'Orbitron',monospace; font-size:10px; color:#00ff9970;
    text-transform:uppercase; letter-spacing:3px;
    border-left:2px solid #00ff99; padding-left:10px; margin:16px 0 12px; }
.stSelectbox label,.stMultiSelect label,.stDateInput label,
.stSlider label,.stRadio label { color:#00ff9970 !important; font-size:11px !important; }
.stButton>button { background:linear-gradient(135deg,#00ff9915,#00aaff15) !important;
    border:1px solid #00ff9950 !important; color:#00ff99 !important;
    font-family:'Orbitron',monospace !important; font-size:10px !important;
    letter-spacing:2px !important; border-radius:2px !important; }
.stProgress>div>div { background:linear-gradient(90deg,#00ff99,#00aaff) !important; }
</style>
""", unsafe_allow_html=True)

COLORS = ["#00ff99","#00aaff","#ff3366","#ffaa00","#aa44ff","#ff6600"]

def rgba(h, a=0.15):
    h = h.lstrip("#")
    return f"rgba({int(h[:2],16)},{int(h[2:4],16)},{int(h[4:],16)},{a})"

# ─────────────────────────────────────────────────────────────────────────────
# ERCOT MIS — Get report doc IDs for NP4-183-CD by date
# URL: https://www.ercot.com/misapp/GetReports.do?reportTypeId=12300&reportTitle=DAM+Settlement+Point+Prices&showHTMLView=&mimicHash=
# ─────────────────────────────────────────────────────────────────────────────

MIS_REPORT_URL   = "https://www.ercot.com/misapp/GetReports.do"
MIS_DOWNLOAD_URL = "https://www.ercot.com/misdownload/servlets/mirDownload"
REPORT_TYPE_ID   = "12300"  # NP4-183-CD DAM Settlement Point Prices

@st.cache_data(ttl=3600, show_spinner=False)
def get_doc_ids(target_date: date) -> list:
    """
    Scrape ERCOT MIS report listing page to get doc IDs for a specific date.
    Returns list of (doc_id, filename) tuples.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer":    "https://www.ercot.com/",
    }
    params = {
        "reportTypeId":  REPORT_TYPE_ID,
        "reportTitle":   "DAM Settlement Point Prices",
        "showHTMLView":  "",
        "mimicHash":     "",
    }
    r = requests.get(MIS_REPORT_URL, params=params, headers=headers, timeout=20)
    r.raise_for_status()

    # Parse HTML for doc IDs and filenames matching our date
    ds      = target_date.strftime("%Y%m%d")
    pattern = rf'doclookupId=(\d+)[^>]*>([^<]*{ds}[^<]*\.zip)'
    matches = re.findall(pattern, r.text, re.IGNORECASE)

    if not matches:
        # Broader search — find any zip for this date
        all_docs  = re.findall(r'doclookupId=(\d+)', r.text)
        all_files = re.findall(r'>(DAM[^<]*' + ds + r'[^<]*\.zip)<', r.text, re.IGNORECASE)
        if all_docs and all_files:
            matches = list(zip(all_docs[:len(all_files)], all_files))

    # Also try direct date pattern in href
    if not matches:
        href_matches = re.findall(
            r'doclookupId=(\d+)["\s][^>]*>[^<]*(' + ds + r'[^<]*)',
            r.text, re.IGNORECASE
        )
        matches = [(m[0], m[1].strip()) for m in href_matches if m[1].strip()]

    return matches


@st.cache_data(ttl=86400, show_spinner=False)
def download_mis_zip(doc_id: str) -> bytes:
    """Download zip from ERCOT MIS by document lookup ID."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":    "https://www.ercot.com/",
    }
    r = requests.get(MIS_DOWNLOAD_URL,
                     params={"mimic_duns": "", "doclookupId": doc_id},
                     headers=headers, timeout=60)
    r.raise_for_status()
    return r.content


def parse_zip_bytes(content: bytes, date_str: str) -> pd.DataFrame:
    """Parse ERCOT zip bytes into a clean DataFrame."""
    with zipfile.ZipFile(BytesIO(content)) as z:
        csvs = [f for f in z.namelist() if f.lower().endswith(".csv")]
        if not csvs:
            raise ValueError(f"No CSV found. Contents: {z.namelist()}")
        with z.open(csvs[0]) as f:
            raw   = f.read().decode("utf-8", errors="replace")
            lines = [l for l in raw.splitlines() if not l.startswith("#")]
            df    = pd.read_csv(BytesIO("\n".join(lines).encode()), low_memory=False)

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    rename = {}
    for c in df.columns:
        cl = c.lower()
        if any(x in cl for x in ["busname","bus_name","settlementpoint","settlement_point"]): rename[c] = "bus"
        elif "deliverydate" in cl or "delivery_date" in cl:                                   rename[c] = "date"
        elif any(x in cl for x in ["hourending","hour_ending","hourendingcst","deliveryhour"]): rename[c] = "hour"
        elif cl == "lmp":                                                                       rename[c] = "lmp"
        elif any(x in cl for x in ["settlementpointprice","spp","price"]) and "lmp" not in rename.values(): rename[c] = "lmp"
    df = df.rename(columns=rename)

    if "lmp" not in df.columns or "bus" not in df.columns:
        raise ValueError(f"Could not find LMP/bus columns. Found: {list(df.columns)}")

    df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")

    if "date" in df.columns and "hour" in df.columns:
        hr = pd.to_numeric(
            df["hour"].astype(str).str.replace(":00","").str.strip(),
            errors="coerce").fillna(1)
        df["datetime"] = pd.to_datetime(df["date"], errors="coerce") + \
                         pd.to_timedelta(hr - 1, unit="h")
    elif "date" in df.columns:
        df["datetime"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        df["datetime"] = pd.to_datetime(date_str)

    return df[["datetime","bus","lmp"]].dropna().sort_values("datetime")


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="title-glow" style="font-size:16px">⚡ ERCOT LMP</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:9px;color:#00ff9930;letter-spacing:3px;margin-bottom:16px">NP4-183-CD // DAM PRICES</div>', unsafe_allow_html=True)

    view  = st.radio("MODE", ["📅 Single Day — 24H", "📆 Monthly Average"])
    today = date.today()

    if view == "📅 Single Day — 24H":
        sel_date = st.date_input("DATE",
                                  value=today - timedelta(days=2),
                                  max_value=today - timedelta(days=1))
        d_from = d_to = sel_date
        yr, mo = sel_date.year, sel_date.month
    else:
        c1,c2 = st.columns(2)
        with c1: yr = st.selectbox("YEAR", list(range(today.year, 2015, -1)))
        with c2: mo = st.selectbox("MONTH", list(range(1,13)),
                                    index=max(0, today.month-2),
                                    format_func=lambda m: datetime(2000,m,1).strftime("%b"))
        _, last = calendar.monthrange(yr, mo)
        d_from  = date(yr, mo, 1)
        d_to    = min(date(yr, mo, last), today - timedelta(days=1))

    chart_t = st.selectbox("CHART", ["Line","Area","Bar"])
    show_ma = st.checkbox("Moving Average", value=True)
    ma_w    = st.slider("MA Window", 2, 14, 3) if show_ma else 3

    if st.button("⟳ CLEAR CACHE", use_container_width=True):
        st.cache_data.clear(); st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="title-glow">⚡ ERCOT LMP COMMAND CENTER</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Day-Ahead Market // Locational Marginal Prices // $/MWh</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# BUILD DATE LIST
# ─────────────────────────────────────────────────────────────────────────────
all_dates = pd.date_range(d_from, d_to, freq="D").date.tolist()

# ─────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────────────────────────────
all_dfs = []
errors  = []
pb      = st.progress(0, text="⚡ Initializing...")

for i, d in enumerate(all_dates):
    pb.progress((i+1)/len(all_dates), text=f"⚡ Loading {d}...")
    try:
        # Step 1: get doc IDs for this date
        docs = get_doc_ids(d)
        if not docs:
            errors.append(f"{d}: No document found on MIS")
            continue

        # Step 2: download first matching zip
        doc_id  = docs[0][0]
        content = download_mis_zip(doc_id)

        # Step 3: parse
        df = parse_zip_bytes(content, str(d))
        if not df.empty:
            all_dfs.append(df)

    except Exception as e:
        errors.append(f"{d}: {str(e)[:100]}")

pb.empty()

# ─────────────────────────────────────────────────────────────────────────────
# FALLBACK — Manual upload if MIS scraping fails
# ─────────────────────────────────────────────────────────────────────────────
if not all_dfs:
    st.error("⚠️ Could not auto-download from ERCOT MIS.")
    if errors:
        with st.expander("Error details"):
            for e in errors: st.text(e)

    st.markdown("### 📂 Manual Upload")
    st.markdown("Download zip(s) from [data.ercot.com/data-product-archive/NP4-183-CD](https://data.ercot.com/data-product-archive/NP4-183-CD) and upload here:")
    ups = st.file_uploader("Upload NP4-183-CD zip file(s)", type="zip", accept_multiple_files=True)
    if ups:
        for up in ups:
            try:
                df = parse_zip_bytes(up.read(), "uploaded")
                all_dfs.append(df)
                st.success(f"✅ {up.name} — {len(df):,} records, {df['bus'].nunique()} buses")
            except Exception as e:
                st.error(f"❌ {up.name}: {e}")
    if not all_dfs:
        st.stop()

data = pd.concat(all_dfs, ignore_index=True)
st.success(f"✅ {len(data):,} records loaded across {len(all_dfs)} day(s)")

if errors:
    with st.expander(f"⚠️ {len(errors)} day(s) had issues"):
        for e in errors: st.text(e)

# ─────────────────────────────────────────────────────────────────────────────
# BUS SELECTOR
# ─────────────────────────────────────────────────────────────────────────────
all_buses = sorted(data["bus"].dropna().unique().tolist())
pref      = [b for b in ["HB_HOUSTON","HB_NORTH","HB_SOUTH","HB_WEST",
                          "LZ_HOUSTON","LZ_NORTH"] if b in all_buses]
default   = pref[:1] if pref else all_buses[:1]

st.markdown('<div class="shdr">SELECT BUS / SETTLEMENT POINT</div>', unsafe_allow_html=True)
sel = st.multiselect("", all_buses, default=default, max_selections=6,
                      label_visibility="collapsed")
if not sel:
    st.warning("Select at least one bus.")
    st.stop()

filt = data[data["bus"].isin(sel)].copy()

# ─────────────────────────────────────────────────────────────────────────────
# KPI CARDS
# ─────────────────────────────────────────────────────────────────────────────
p     = filt[filt["bus"]==sel[0]].sort_values("datetime")
c_lmp = float(p["lmp"].iloc[-1])  if not p.empty else 0
d_avg = float(p["lmp"].mean())    if not p.empty else 0
d_max = float(p["lmp"].max())     if not p.empty else 0
vstd  = float(p["lmp"].std())     if not p.empty else 0
vlbl  = "LOW" if vstd<5 else ("HIGH" if vstd>15 else "MOD")
pt    = p.loc[p["lmp"].idxmax(),"datetime"].strftime("%m-%d %H:%M") if not p.empty else "--"
mid   = len(p)//2
base  = p["lmp"].iloc[:mid].mean() if mid>0 else 1
dpct  = ((p["lmp"].iloc[mid:].mean()-base)/abs(base)*100) if base!=0 else 0
bc    = "bp" if dpct>=0 else "bn"
vc    = "bp" if vlbl=="LOW" else ("bn" if vlbl=="HIGH" else "bm")

k1,k2,k3,k4 = st.columns(4)
with k1: st.markdown(f'<div class="card"><div class="lbl">⚡ CURRENT LMP</div><div class="val">${c_lmp:.2f}</div><div class="sub">LAST HOUR · $/MWh</div><span class="{bc}">{dpct:+.1f}%</span></div>',unsafe_allow_html=True)
with k2: st.markdown(f'<div class="card"><div class="lbl">◈ AVG LMP</div><div class="val">${d_avg:.2f}</div><div class="sub">PERIOD AVERAGE · $/MWh</div></div>',unsafe_allow_html=True)
with k3: st.markdown(f'<div class="card"><div class="lbl">▲ PEAK LMP</div><div class="val">${d_max:.2f}</div><div class="sub">AT {pt}</div><span class="bn">MAX</span></div>',unsafe_allow_html=True)
with k4: st.markdown(f'<div class="card"><div class="lbl">≈ VOLATILITY</div><div class="val">{vstd:.1f}</div><div class="sub">STD DEV — {vlbl}</div><span class="{vc}">{vlbl}</span></div>',unsafe_allow_html=True)
st.markdown("<br>",unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CHART
# ─────────────────────────────────────────────────────────────────────────────
if view == "📆 Monthly Average":
    filt2       = filt.copy()
    filt2["day"]= filt2["datetime"].dt.normalize()
    plot_df     = filt2.groupby(["day","bus"])["lmp"].agg(
        lmp="mean", lmp_max="max", lmp_min="min"
    ).reset_index().rename(columns={"day":"datetime"})
    x_lbl = "Date"
    t_sfx = f"{datetime(yr,mo,1).strftime('%B %Y')} — Daily Avg"
else:
    plot_df = filt.copy()
    x_lbl   = "Hour (CST)"
    t_sfx   = f"{sel_date} — 24H LMP"

st.markdown(f'<div class="shdr">LMP PRICE CHART — {t_sfx}</div>', unsafe_allow_html=True)

fig = go.Figure()
for i, bus in enumerate(sel):
    c   = COLORS[i % len(COLORS)]
    bdf = plot_df[plot_df["bus"]==bus].sort_values("datetime")
    if bdf.empty: continue

    if "lmp_max" in bdf.columns:
        fig.add_trace(go.Scatter(
            x=pd.concat([bdf["datetime"], bdf["datetime"][::-1]]),
            y=pd.concat([bdf["lmp_max"],  bdf["lmp_min"][::-1]]),
            fill="toself", fillcolor=rgba(c, 0.07),
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=False, hoverinfo="skip"))

    ht = f"<b>{bus}</b><br>%{{x}}<br><b>${{y:.2f}}/MWh</b><extra></extra>"
    if chart_t == "Bar":
        fig.add_trace(go.Bar(x=bdf["datetime"], y=bdf["lmp"], name=bus,
            marker_color=c, opacity=0.8, hovertemplate=ht))
    elif chart_t == "Area":
        fig.add_trace(go.Scatter(x=bdf["datetime"], y=bdf["lmp"], name=bus,
            mode="lines", line=dict(color=c, width=2),
            fill="tozeroy", fillcolor=rgba(c, 0.12), hovertemplate=ht))
    else:
        fig.add_trace(go.Scatter(x=bdf["datetime"], y=bdf["lmp"], name=bus,
            mode="lines", line=dict(color=c, width=2), hovertemplate=ht))

    if show_ma and len(bdf) >= ma_w:
        ma = bdf["lmp"].rolling(ma_w, min_periods=1).mean()
        fig.add_trace(go.Scatter(x=bdf["datetime"], y=ma, name=f"{bus} MA{ma_w}",
            mode="lines", line=dict(color=c, width=1, dash="dot"),
            hovertemplate=f"MA $%{{y:.2f}}<extra></extra>"))

fig.add_hline(y=0, line_dash="dash", line_color="#00ff9920", line_width=1)
fig.update_layout(
    template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#020818", height=480,
    font=dict(family="Rajdhani", color="#00ff9970"),
    legend=dict(orientation="h", y=1.06, x=0,
                font=dict(size=12, color="#00ff99"), bgcolor="rgba(0,0,0,0)"),
    xaxis=dict(title=x_lbl, showgrid=True, gridcolor="#00ff9910",
               tickfont=dict(color="#00ff9970"),
               rangeslider=dict(visible=True, bgcolor="#020818", thickness=0.04),
               showline=True, linecolor="#00ff9920"),
    yaxis=dict(title="LMP ($/MWh)", showgrid=True, gridcolor="#00ff9910",
               tickprefix="$", tickfont=dict(color="#00ff9970"),
               zeroline=True, zerolinecolor="#00ff9920",
               showline=True, linecolor="#00ff9920"),
    hovermode="x unified",
    margin=dict(l=60,r=15,t=50,b=40), barmode="group")
st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# HOURLY TABLE
# ─────────────────────────────────────────────────────────────────────────────
if view == "📅 Single Day — 24H":
    with st.expander("🕐 HOURLY PRICE TABLE"):
        pivot = filt.pivot_table(index="datetime", columns="bus", values="lmp").round(2)
        pivot.index = pivot.index.strftime("%H:%M")
        st.dataframe(pivot.style.background_gradient(cmap="RdYlGn_r", axis=None),
                     use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# MONTHLY SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
if view == "📆 Monthly Average":
    st.markdown('<div class="shdr" style="margin-top:20px">MONTHLY SUMMARY</div>', unsafe_allow_html=True)
    rows = [{"BUS": n,
             "AVG $/MWh": round(data[data["bus"]==n]["lmp"].mean(), 2),
             "MAX":       round(data[data["bus"]==n]["lmp"].max(),  2),
             "MIN":       round(data[data["bus"]==n]["lmp"].min(),  2),
             "STD DEV":   round(data[data["bus"]==n]["lmp"].std(),  2),
             "HOURS":     len(data[data["bus"]==n])}
            for n in sel if not data[data["bus"]==n].empty]
    if rows:
        st.dataframe(pd.DataFrame(rows).set_index("BUS"), use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────────────────────────────────
with st.expander("◈ RAW DATA EXPORT"):
    show = data[data["bus"].isin(sel)].sort_values("datetime", ascending=False)
    st.dataframe(show, use_container_width=True)
    st.download_button("⬇ DOWNLOAD CSV",
        show.to_csv(index=False).encode(), "ercot_lmp.csv", "text/csv",
        use_container_width=True)

st.markdown('<div style="font-size:10px;color:#00ff9915;text-align:center;margin-top:20px;letter-spacing:2px">ERCOT NP4-183-CD // DAM HOURLY LMP // www.ercot.com/misapp</div>', unsafe_allow_html=True)
