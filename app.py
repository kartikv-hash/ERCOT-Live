
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
from io import BytesIO
import zipfile
import calendar
import re

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
.upload-box { background:linear-gradient(135deg,#050520,#0a0a2e);
    border:2px dashed #00ff9940; border-radius:8px; padding:30px;
    text-align:center; margin:20px 0; }
.upload-title { font-family:'Orbitron',monospace; font-size:18px;
    color:#00ff99; text-shadow:0 0 15px #00ff9980; margin-bottom:10px; }
.upload-sub { font-size:13px; color:#00ff9960; letter-spacing:1px; }
.step-box { background:#050520; border:1px solid #00ff9920; border-radius:6px;
    padding:14px 18px; margin:8px 0; display:flex; align-items:center; gap:12px; }
.step-num { font-family:'Orbitron',monospace; font-size:20px; color:#00ff99;
    font-weight:900; text-shadow:0 0 10px #00ff99; min-width:32px; }
.step-txt { font-size:13px; color:#aaa; letter-spacing:0.5px; }
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
# PARSE ZIP → DATAFRAME
# ─────────────────────────────────────────────────────────────────────────────
def parse_zip(file_bytes: bytes) -> tuple[pd.DataFrame, list]:
    """Parse ERCOT NP4-183-CD zip into clean DataFrame. Returns (df, raw_columns)."""
    with zipfile.ZipFile(BytesIO(file_bytes)) as z:
        csvs = [f for f in z.namelist() if f.lower().endswith(".csv")]
        if not csvs:
            raise ValueError(f"No CSV in zip. Files: {z.namelist()}")
        with z.open(csvs[0]) as f:
            raw   = f.read().decode("utf-8", errors="replace")
            # Skip comment/header lines starting with #
            lines = [l for l in raw.splitlines() if not l.startswith("#")]
            df    = pd.read_csv(BytesIO("\n".join(lines).encode()), low_memory=False)

    raw_cols = list(df.columns)

    # ── Normalize column names ─────────────────────────────────────────────
    df.columns = [c.strip() for c in df.columns]
    col_lower  = {c: c.lower().replace(" ","").replace("_","") for c in df.columns}

    rename = {}
    for orig, cl in col_lower.items():
        if   any(x in cl for x in ["busname","settlementpoint","nodename","bus"]):
            if "bus" not in rename.values(): rename[orig] = "bus"
        elif any(x in cl for x in ["deliverydate","date"]):
            if "date" not in rename.values(): rename[orig] = "date"
        elif any(x in cl for x in ["hourending","hourendingcst","deliveryhour","hour"]):
            if "hour" not in rename.values(): rename[orig] = "hour"
        elif cl == "lmp":
            rename[orig] = "lmp"
        elif any(x in cl for x in ["settlementpointprice","settlepointprice","spp","price","lmp"]):
            if "lmp" not in rename.values(): rename[orig] = "lmp"

    df = df.rename(columns=rename)

    if "lmp" not in df.columns:
        raise ValueError(f"Cannot find LMP price column. Columns found: {raw_cols}")
    if "bus" not in df.columns:
        raise ValueError(f"Cannot find Bus/Node column. Columns found: {raw_cols}")

    df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")

    # ── Build datetime ─────────────────────────────────────────────────────
    if "date" in df.columns and "hour" in df.columns:
        # HourEnding is 1-24 in ERCOT → offset 0-23
        hr_str = df["hour"].astype(str).str.replace(":00","").str.strip()
        hr     = pd.to_numeric(hr_str, errors="coerce").fillna(1)
        df["datetime"] = pd.to_datetime(df["date"], errors="coerce") + \
                         pd.to_timedelta(hr - 1, unit="h")
    elif "date" in df.columns:
        df["datetime"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        raise ValueError(f"Cannot find date column. Columns: {raw_cols}")

    return df[["datetime","bus","lmp"]].dropna().sort_values("datetime"), raw_cols


# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────
if "data" not in st.session_state:
    st.session_state.data = None

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<div class="title-glow">⚡ ERCOT LMP COMMAND CENTER</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">Day-Ahead Market // Locational Marginal Prices // $/MWh</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# UPLOAD SECTION — shown until data is loaded
# ─────────────────────────────────────────────────────────────────────────────
if st.session_state.data is None:
    st.markdown("""
    <div class="upload-box">
        <div class="upload-title">⚡ LOAD ERCOT DATA</div>
        <div class="upload-sub">Upload one or more NP4-183-CD zip files to begin</div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([1,1])
    with col1:
        st.markdown("#### How to download from ERCOT:")
        st.markdown("""
        <div class="step-box"><div class="step-num">1</div>
        <div class="step-txt">Go to <a href="https://data.ercot.com/data-product-archive/NP4-183-CD"
        style="color:#00ff99" target="_blank">data.ercot.com/data-product-archive/NP4-183-CD</a></div></div>
        <div class="step-box"><div class="step-num">2</div>
        <div class="step-txt">Select your date(s) and click the download icon ⬇</div></div>
        <div class="step-box"><div class="step-num">3</div>
        <div class="step-txt">Upload the zip file(s) below — supports multiple files for monthly view</div></div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("#### Tips:")
        st.markdown("""
        <div class="step-box"><div class="step-num">📅</div>
        <div class="step-txt"><b>Single day:</b> Upload 1 zip file → plots 24-hour LMP</div></div>
        <div class="step-box"><div class="step-num">📆</div>
        <div class="step-txt"><b>Monthly view:</b> Upload multiple zips → plots daily average LMP</div></div>
        <div class="step-box"><div class="step-num">💡</div>
        <div class="step-txt">Data is cached in session — no need to re-upload on refresh</div></div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    ups = st.file_uploader(
        "DROP ZIP FILE(S) HERE",
        type="zip",
        accept_multiple_files=True,
        help="Download from data.ercot.com/data-product-archive/NP4-183-CD"
    )

    if ups:
        all_dfs = []; errors = []
        pb = st.progress(0, text="⚡ Parsing files...")
        for i, up in enumerate(ups):
            pb.progress((i+1)/len(ups), text=f"⚡ Parsing {up.name}...")
            try:
                df, raw_cols = parse_zip(up.read())
                all_dfs.append(df)
                st.success(f"✅ **{up.name}** — {len(df):,} records | {df['bus'].nunique()} buses | {df['datetime'].min().date()} → {df['datetime'].max().date()}")
            except Exception as e:
                errors.append(f"❌ {up.name}: {e}")
                st.error(f"❌ **{up.name}**: {e}")
        pb.empty()

        if all_dfs:
            st.session_state.data = pd.concat(all_dfs, ignore_index=True)
            st.rerun()

    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADED — MAIN DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────
data = st.session_state.data

# Date range in loaded data
min_date = data["datetime"].dt.date.min()
max_date = data["datetime"].dt.date.max()
n_days   = (max_date - min_date).days + 1

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="title-glow" style="font-size:16px">⚡ ERCOT LMP</div>', unsafe_allow_html=True)
    st.markdown(f'<div style="font-size:9px;color:#00ff9930;letter-spacing:2px;margin-bottom:12px">DATA: {min_date} → {max_date}<br>{len(data):,} RECORDS LOADED</div>', unsafe_allow_html=True)

    # View mode
    if n_days == 1:
        view = "📅 Single Day — 24H"
        st.markdown(f'<div style="font-size:11px;color:#00ff9960">MODE: 24H VIEW — {min_date}</div>', unsafe_allow_html=True)
    else:
        view = st.radio("MODE", ["📅 Single Day — 24H", "📆 Monthly Average"])

    # Date selector for single day
    if view == "📅 Single Day — 24H" and n_days > 1:
        available = sorted(data["datetime"].dt.date.unique())
        sel_date  = st.selectbox("SELECT DATE", available,
                                  index=len(available)-1,
                                  format_func=lambda d: d.strftime("%Y-%m-%d"))
    else:
        sel_date = min_date

    chart_t = st.selectbox("CHART STYLE", ["Line","Area","Bar"])
    show_ma = st.checkbox("Moving Average", value=True)
    ma_w    = st.slider("MA Window", 2, 14, 3) if show_ma else 3

    st.markdown("---")
    if st.button("📂 LOAD NEW FILES", use_container_width=True):
        st.session_state.data = None; st.rerun()
    if st.button("⟳ CLEAR CACHE", use_container_width=True):
        st.cache_data.clear(); st.session_state.data = None; st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# FILTER BY VIEW
# ─────────────────────────────────────────────────────────────────────────────
if view == "📅 Single Day — 24H":
    view_data = data[data["datetime"].dt.date == sel_date].copy()
    t_sfx     = f"{sel_date} — 24H LMP"
    x_lbl     = "Hour (CST)"
else:
    view_data = data.copy()
    t_sfx     = f"{min_date.strftime('%b %Y')} {'→ '+str(max_date) if n_days>1 else ''} — Daily Avg"
    x_lbl     = "Date"

# ─────────────────────────────────────────────────────────────────────────────
# BUS SELECTOR
# ─────────────────────────────────────────────────────────────────────────────
all_buses = sorted(data["bus"].dropna().unique().tolist())
pref      = [b for b in ["HB_HOUSTON","HB_NORTH","HB_SOUTH","HB_WEST",
                          "LZ_HOUSTON","LZ_NORTH","LZ_SOUTH","LZ_WEST"] if b in all_buses]
default   = pref[:1] if pref else all_buses[:1]

st.markdown('<div class="shdr">SELECT BUS / SETTLEMENT POINT</div>', unsafe_allow_html=True)

c1, c2 = st.columns([3,1])
with c1:
    sel = st.multiselect("", all_buses, default=default, max_selections=6,
                          label_visibility="collapsed")
with c2:
    st.markdown(f'<div style="font-size:10px;color:#00ff9950;padding-top:8px">{len(all_buses)} buses available<br>{min_date} → {max_date}</div>', unsafe_allow_html=True)

if not sel:
    st.warning("Select at least one bus.")
    st.stop()

filt = view_data[view_data["bus"].isin(sel)].copy()

# ─────────────────────────────────────────────────────────────────────────────
# KPI CARDS
# ─────────────────────────────────────────────────────────────────────────────
p     = filt[filt["bus"]==sel[0]].sort_values("datetime")
c_lmp = float(p["lmp"].iloc[-1])  if not p.empty else 0
d_avg = float(p["lmp"].mean())    if not p.empty else 0
d_max = float(p["lmp"].max())     if not p.empty else 0
d_min = float(p["lmp"].min())     if not p.empty else 0
vstd  = float(p["lmp"].std())     if not p.empty else 0
vlbl  = "LOW" if vstd<5 else ("HIGH" if vstd>15 else "MOD")
pt    = p.loc[p["lmp"].idxmax(),"datetime"].strftime("%m-%d %H:%M") if not p.empty else "--"
mid   = len(p)//2
base  = p["lmp"].iloc[:mid].mean() if mid>0 and p["lmp"].iloc[:mid].mean()!=0 else 1
dpct  = ((p["lmp"].iloc[mid:].mean()-base)/abs(base)*100) if mid>0 else 0
bc = "bp" if dpct>=0 else "bn"
vc = "bp" if vlbl=="LOW" else ("bn" if vlbl=="HIGH" else "bm")

k1,k2,k3,k4 = st.columns(4)
with k1: st.markdown(f'<div class="card"><div class="lbl">⚡ LAST LMP</div><div class="val">${c_lmp:.2f}</div><div class="sub">MOST RECENT HOUR · $/MWh</div><span class="{bc}">{dpct:+.1f}%</span></div>',unsafe_allow_html=True)
with k2: st.markdown(f'<div class="card"><div class="lbl">◈ AVG LMP</div><div class="val">${d_avg:.2f}</div><div class="sub">PERIOD AVERAGE · $/MWh</div></div>',unsafe_allow_html=True)
with k3: st.markdown(f'<div class="card"><div class="lbl">▲ PEAK LMP</div><div class="val">${d_max:.2f}</div><div class="sub">AT {pt}</div><span class="bn">MAX</span></div>',unsafe_allow_html=True)
with k4: st.markdown(f'<div class="card"><div class="lbl">≈ VOLATILITY</div><div class="val">{vstd:.1f}</div><div class="sub">STD DEV — {vlbl}</div><span class="{vc}">{vlbl}</span></div>',unsafe_allow_html=True)
st.markdown("<br>",unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATE FOR MONTHLY VIEW
# ─────────────────────────────────────────────────────────────────────────────
if view == "📆 Monthly Average":
    filt["day"] = filt["datetime"].dt.normalize()
    plot_df = filt.groupby(["day","bus"])["lmp"].agg(
        lmp="mean", lmp_max="max", lmp_min="min"
    ).reset_index().rename(columns={"day":"datetime"})
else:
    plot_df = filt.copy()

# ─────────────────────────────────────────────────────────────────────────────
# CHART
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f'<div class="shdr">LMP PRICE CHART — {t_sfx}</div>', unsafe_allow_html=True)

fig = go.Figure()
for i, bus in enumerate(sel):
    c   = COLORS[i % len(COLORS)]
    bdf = plot_df[plot_df["bus"]==bus].sort_values("datetime")
    if bdf.empty: continue

    # Min/max shaded band
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
            fill="tozeroy", fillcolor=rgba(c,0.12), hovertemplate=ht))
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
    plot_bgcolor="#020818", height=500,
    font=dict(family="Rajdhani", color="#00ff9970"),
    legend=dict(orientation="h", y=1.06, x=0,
                font=dict(size=12,color="#00ff99"), bgcolor="rgba(0,0,0,0)"),
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
# HOURLY TABLE — single day
# ─────────────────────────────────────────────────────────────────────────────
if view == "📅 Single Day — 24H":
    with st.expander("🕐 HOURLY PRICE TABLE"):
        pivot = filt.pivot_table(index="datetime", columns="bus", values="lmp").round(2)
        pivot.index = pivot.index.strftime("%H:%M")
        st.dataframe(pivot.style.background_gradient(cmap="RdYlGn_r", axis=None),
                     use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY TABLE — monthly
# ─────────────────────────────────────────────────────────────────────────────
if view == "📆 Monthly Average":
    st.markdown('<div class="shdr" style="margin-top:20px">PERIOD SUMMARY</div>', unsafe_allow_html=True)
    rows = [{"BUS":n,
             "AVG $/MWh": round(filt[filt["bus"]==n]["lmp"].mean(),2),
             "MAX":       round(filt[filt["bus"]==n]["lmp"].max(), 2),
             "MIN":       round(filt[filt["bus"]==n]["lmp"].min(), 2),
             "STD DEV":   round(filt[filt["bus"]==n]["lmp"].std(), 2),
             "HOURS":     len(filt[filt["bus"]==n])}
            for n in sel if not filt[filt["bus"]==n].empty]
    if rows:
        st.dataframe(pd.DataFrame(rows).set_index("BUS"), use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────────────────────────────────
with st.expander("◈ RAW DATA EXPORT"):
    show = filt.sort_values("datetime", ascending=False)
    st.dataframe(show, use_container_width=True)
    st.download_button("⬇ DOWNLOAD CSV",
        show.to_csv(index=False).encode(),
        "ercot_lmp.csv", "text/csv", use_container_width=True)

st.markdown('<div style="font-size:10px;color:#00ff9915;text-align:center;margin-top:20px;letter-spacing:2px">ERCOT NP4-183-CD // DAM HOURLY LMP // data.ercot.com</div>', unsafe_allow_html=True)
