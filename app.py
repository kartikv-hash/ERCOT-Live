
# ─────────────────────────────────────────────────────────────────────────────
# ERCOT LMP Dashboard
# Data source: https://data.ercot.com/data-product-archive/NP4-183-CD
# No authentication required — public zip file downloads
# ─────────────────────────────────────────────────────────────────────────────

import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
from io import BytesIO
import zipfile
import re

st.set_page_config(page_title="ERCOT LMP", page_icon="⚡", layout="wide")
st.markdown("""
<style>
[data-testid="stAppViewContainer"]{background:#0a0a14}
[data-testid="stSidebar"]{background:#0f0f1e;border-right:1px solid #1e1e3a}
[data-testid="stHeader"]{background:transparent}
.card{background:#111125;border:1px solid #1e1e3a;border-radius:12px;
      padding:18px 20px;position:relative;margin-bottom:8px}
.lbl{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:6px}
.val{font-size:28px;font-weight:700;color:#fff;margin:0}
.sub{font-size:12px;color:#888;margin-top:4px}
.pos{position:absolute;top:14px;right:14px;font-size:11px;color:#51cf66;font-weight:600}
.neg{position:absolute;top:14px;right:14px;font-size:11px;color:#ff6b6b;font-weight:600}
.neu{position:absolute;top:14px;right:14px;font-size:11px;color:#ffd43b;font-weight:600}
</style>
""", unsafe_allow_html=True)

COLORS = ["#00d4ff","#ff6b6b","#51cf66","#ffd43b","#cc5de8","#ff922b"]

def rgba(h, a=0.15):
    h = h.lstrip("#")
    return f"rgba({int(h[:2],16)},{int(h[2:4],16)},{int(h[4:],16)},{a})"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Get list of available zip files from ERCOT archive page
# ─────────────────────────────────────────────────────────────────────────────
ARCHIVE_URL = "https://data.ercot.com/data-product-archive/NP4-183-CD"

@st.cache_data(ttl=3600, show_spinner=False)
def get_file_list() -> pd.DataFrame:
    """
    Scrape the ERCOT archive page to get all available zip file links.
    Returns DataFrame with columns: date, filename, url
    """
    r = requests.get(ARCHIVE_URL, timeout=20)
    r.raise_for_status()

    # Find all zip file links — pattern: .../NP4-183-CD_YYYYMMDD.zip or similar
    urls  = re.findall(r'href="([^"]*NP4-183[^"]*\.zip)"', r.text, re.IGNORECASE)
    # Also try JSON/API endpoint that ERCOT might use
    if not urls:
        urls = re.findall(r'(https?://[^\s"<>]*NP4-183[^\s"<>]*\.zip)', r.text, re.IGNORECASE)

    rows = []
    for u in urls:
        full = u if u.startswith("http") else "https://data.ercot.com" + u
        # Extract date from filename
        m = re.search(r'(\d{8})', full)
        if m:
            try:
                d = datetime.strptime(m.group(1), "%Y%m%d").date()
                rows.append({"date": d, "url": full, "label": d.strftime("%Y-%m-%d")})
            except:
                pass

    if rows:
        df = pd.DataFrame(rows).sort_values("date", ascending=False).drop_duplicates("date")
        return df

    # Fallback — try ERCOT's data API endpoint
    try:
        api = "https://data.ercot.com/api/1/services/search/archive/NP4-183-CD"
        jr  = requests.get(api, timeout=10).json()
        rows2 = []
        for item in jr.get("data", jr.get("items", [])):
            u = item.get("url","") or item.get("downloadUrl","")
            n = item.get("filename","") or item.get("name","")
            m = re.search(r'(\d{8})', u+n)
            if m and u:
                d = datetime.strptime(m.group(1), "%Y%m%d").date()
                rows2.append({"date":d,"url":u,"label":d.strftime("%Y-%m-%d")})
        if rows2:
            return pd.DataFrame(rows2).sort_values("date",ascending=False).drop_duplicates("date")
    except:
        pass

    return pd.DataFrame(columns=["date","url","label"])


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Download & parse a single zip file
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def load_zip(url: str) -> pd.DataFrame:
    """
    Download a zip from ERCOT, extract CSV, return clean DataFrame.
    ERCOT DAM LMP CSV columns (NP4-183-CD):
      DeliveryDate, HourEnding, BusName, LMP, MCC, MLC (or similar)
    """
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    with zipfile.ZipFile(BytesIO(r.content)) as z:
        # Pick the first CSV file in the zip
        csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
        if not csv_files:
            return pd.DataFrame()
        with z.open(csv_files[0]) as f:
            # ERCOT CSVs sometimes have a header comment row — skip lines starting with #
            raw = f.read().decode("utf-8", errors="replace")
            lines = [l for l in raw.splitlines() if not l.startswith("#")]
            df = pd.read_csv(BytesIO("\n".join(lines).encode()), low_memory=False)

    # Normalize columns
    df.columns = [c.strip().lower().replace(" ","_") for c in df.columns]

    # Map to standard names
    col_map = {}
    for c in df.columns:
        if "busname" in c or "bus_name" in c or c == "bus":
            col_map[c] = "bus"
        elif "deliverydate" in c or "delivery_date" in c:
            col_map[c] = "date"
        elif "hourending" in c or "hour_ending" in c or "hourendingcst" in c:
            col_map[c] = "hour"
        elif c == "lmp" or "lmp" in c:
            col_map[c] = "lmp"
    df = df.rename(columns=col_map)

    needed = [c for c in ["date","hour","bus","lmp"] if c in df.columns]
    if "lmp" not in df.columns or "bus" not in df.columns:
        return pd.DataFrame()

    df = df[needed].copy()
    df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")

    # Build datetime
    if "date" in df.columns and "hour" in df.columns:
        df["hour_num"] = pd.to_numeric(
            df["hour"].astype(str).str.replace(":00","").str.strip(),
            errors="coerce").fillna(1)
        df["datetime"] = pd.to_datetime(df["date"], errors="coerce") + \
                         pd.to_timedelta(df["hour_num"] - 1, unit="h")
    elif "date" in df.columns:
        df["datetime"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        return pd.DataFrame()

    return df[["datetime","bus","lmp"]].dropna().sort_values("datetime")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Load multiple days (for monthly view)
# ─────────────────────────────────────────────────────────────────────────────
def load_date_range(file_df: pd.DataFrame, d_from: date, d_to: date,
                    progress_bar) -> pd.DataFrame:
    mask  = (file_df["date"] >= d_from) & (file_df["date"] <= d_to)
    files = file_df[mask].sort_values("date")
    if files.empty:
        return pd.DataFrame()
    dfs = []
    for i,(_, row) in enumerate(files.iterrows()):
        progress_bar.progress((i+1)/len(files), text=f"Loading {row['label']}...")
        try:
            dfs.append(load_zip(row["url"]))
        except Exception as e:
            st.warning(f"⚠️ Could not load {row['label']}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ ERCOT LMP")
    st.caption("Source: data.ercot.com / NP4-183-CD")
    st.markdown("---")

    view = st.radio("Mode", ["📅 Single Day (24h)", "📆 Monthly Average"], index=0)

    today = date.today()
    if view == "📅 Single Day (24h)":
        sel_date = st.date_input("Select Date",
                                  value=today - timedelta(days=2),
                                  max_value=today - timedelta(days=1))
        d_from = d_to = sel_date
    else:
        col1, col2 = st.columns(2)
        with col1:
            sel_year  = st.selectbox("Year",  list(range(today.year, 2010, -1)))
        with col2:
            sel_month = st.selectbox("Month",
                list(range(1,13)),
                index=today.month - 2 if today.month > 1 else 0,
                format_func=lambda m: datetime(2000,m,1).strftime("%b"))
        import calendar
        _, last_day = calendar.monthrange(sel_year, sel_month)
        d_from = date(sel_year, sel_month, 1)
        d_to   = date(sel_year, sel_month, last_day)
        st.caption(f"📅 {d_from} → {d_to}")

    chart_t = st.selectbox("Chart Style", ["Line","Area","Bar"])
    show_ma = st.checkbox("Moving Average", value=True)
    ma_w    = st.slider("MA Window", 2, 12, 3) if show_ma else 3

    if st.button("🔄 Clear Cache & Reload", use_container_width=True):
        st.cache_data.clear(); st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("### ⚡ ERCOT DAM LMP Dashboard")
st.caption("Day-Ahead Market Locational Marginal Prices — NP4-183-CD ($/MWh)")

# Load file index
with st.spinner("📋 Loading ERCOT file index..."):
    file_df = get_file_list()

if file_df.empty:
    st.error("""
❌ Could not load file list from data.ercot.com.

**Workaround — Upload a file manually:**
1. Go to 👉 https://data.ercot.com/data-product-archive/NP4-183-CD
2. Download a zip file for any date
3. Upload it below
""")
    uploaded = st.file_uploader("Upload ERCOT NP4-183-CD zip file", type="zip")
    if uploaded:
        with st.spinner("Parsing uploaded file..."):
            try:
                with zipfile.ZipFile(BytesIO(uploaded.read())) as z:
                    csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
                    if csv_files:
                        with z.open(csv_files[0]) as f:
                            raw = f.read().decode("utf-8", errors="replace")
                            lines = [l for l in raw.splitlines() if not l.startswith("#")]
                            df_raw = pd.read_csv(BytesIO("\n".join(lines).encode()), low_memory=False)
                            df_raw.columns = [c.strip().lower().replace(" ","_") for c in df_raw.columns]
                            st.success(f"✅ Loaded! Columns: {list(df_raw.columns)}")
                            st.dataframe(df_raw.head(20), use_container_width=True)
                            # Store for use below
                            st.session_state["uploaded_df"] = df_raw
            except Exception as e:
                st.error(f"Parse error: {e}")
    st.stop()

# Show available date range
min_d = file_df["date"].min(); max_d = file_df["date"].max()
st.info(f"📦 {len(file_df)} files available — {min_d} → {max_d}")

# Load data
pb   = st.progress(0, text="Loading...")
data = load_date_range(file_df, d_from, d_to, pb)
pb.empty()

if data.empty:
    st.error(f"No data found for {d_from} → {d_to}. Try a different date.")
    st.stop()

# Bus selector — populated from actual data
all_buses = sorted(data["bus"].dropna().unique().tolist())
default_buses = [b for b in ["HB_HOUSTON","HB_NORTH","HB_SOUTH","HB_WEST"] if b in all_buses]
if not default_buses and all_buses:
    default_buses = all_buses[:1]

selected_buses = st.multiselect(
    "🔌 Select Bus / Settlement Point",
    options=all_buses,
    default=default_buses[:1],
    max_selections=6
)

if not selected_buses:
    st.warning("Please select at least one bus.")
    st.stop()

filt = data[data["bus"].isin(selected_buses)].copy()

# ─────────────────────────────────────────────────────────────────────────────
# KPI CARDS
# ─────────────────────────────────────────────────────────────────────────────
p    = filt[filt["bus"] == selected_buses[0]]
c_lmp = float(p["lmp"].iloc[-1])  if not p.empty else 0
d_avg = float(p["lmp"].mean())    if not p.empty else 0
d_max = float(p["lmp"].max())     if not p.empty else 0
d_min = float(p["lmp"].min())     if not p.empty else 0
vstd  = float(p["lmp"].std())     if not p.empty else 0
vlbl  = "Low" if vstd<5 else ("High" if vstd>15 else "Moderate")
p_t   = p.loc[p["lmp"].idxmax(),"datetime"].strftime("%m-%d %H:%M") if not p.empty else "--"
vc    = "pos" if vlbl=="Low" else ("neg" if vlbl=="High" else "neu")

k1,k2,k3,k4 = st.columns(4)
with k1: st.markdown(f'<div class="card"><div class="lbl">⚡ Last LMP</div><div class="val">${c_lmp:.2f}</div><div class="sub">Most recent hour · $/MWh</div></div>',unsafe_allow_html=True)
with k2: st.markdown(f'<div class="card"><div class="lbl">📊 Average LMP</div><div class="val">${d_avg:.2f}</div><div class="sub">Period average · $/MWh</div></div>',unsafe_allow_html=True)
with k3: st.markdown(f'<div class="card"><div class="lbl">🔺 Peak LMP</div><div class="val">${d_max:.2f}</div><div class="sub">At {p_t}</div><span class="neg">▲ peak</span></div>',unsafe_allow_html=True)
with k4: st.markdown(f'<div class="card"><div class="lbl">📉 Volatility</div><div class="val">{vstd:.1f}</div><div class="sub">Std Dev — {vlbl}</div><span class="{vc}">{vlbl}</span></div>',unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# CHART
# ─────────────────────────────────────────────────────────────────────────────
if view == "📅 Single Day (24h)":
    title = f"24-Hour LMP — {sel_date} — {', '.join(selected_buses)}"
    x_title = "Hour"
else:
    title = f"Monthly Average LMP — {d_from.strftime('%B %Y')} — {', '.join(selected_buses)}"
    # Aggregate to daily average for monthly view
    filt["day"] = filt["datetime"].dt.normalize()
    filt = filt.groupby(["day","bus"])["lmp"].agg(
        lmp="mean", lmp_max="max", lmp_min="min"
    ).reset_index().rename(columns={"day":"datetime"})
    x_title = "Date"

fig = go.Figure()
for i, bus in enumerate(selected_buses):
    c   = COLORS[i % len(COLORS)]
    bdf = filt[filt["bus"] == bus].sort_values("datetime")
    if bdf.empty: continue

    # Min/max band for monthly view
    if view == "📆 Monthly Average" and "lmp_max" in bdf.columns:
        fig.add_trace(go.Scatter(
            x=pd.concat([bdf["datetime"], bdf["datetime"][::-1]]),
            y=pd.concat([bdf["lmp_max"], bdf["lmp_min"][::-1]]),
            fill="toself", fillcolor=rgba(c, 0.08),
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=False, hoverinfo="skip", name=f"{bus} range"
        ))

    ht = f"<b>{bus}</b><br>%{{x}}<br><b>${{y:.2f}}/MWh</b><extra></extra>"
    if chart_t == "Bar":
        fig.add_trace(go.Bar(
            x=bdf["datetime"], y=bdf["lmp"], name=bus,
            marker_color=c, opacity=0.85, hovertemplate=ht))
    elif chart_t == "Area":
        fig.add_trace(go.Scatter(
            x=bdf["datetime"], y=bdf["lmp"], name=bus,
            mode="lines", line=dict(color=c, width=2.5),
            fill="tozeroy", fillcolor=rgba(c, 0.15), hovertemplate=ht))
    else:
        fig.add_trace(go.Scatter(
            x=bdf["datetime"], y=bdf["lmp"], name=bus,
            mode="lines", line=dict(color=c, width=2.5), hovertemplate=ht))

    # Moving average
    if show_ma and len(bdf) >= ma_w:
        ma = bdf["lmp"].rolling(ma_w, min_periods=1).mean()
        fig.add_trace(go.Scatter(
            x=bdf["datetime"], y=ma, name=f"{bus} MA{ma_w}",
            mode="lines", line=dict(color=c, width=1.2, dash="dot"),
            hovertemplate=f"MA: $%{{y:.2f}}<extra></extra>"))

fig.add_hline(y=0, line_dash="dash", line_color="#333", line_width=1)
fig.update_layout(
    title=dict(text=title, font=dict(color="#aaa", size=13)),
    template="plotly_dark", paper_bgcolor="#0a0a14", plot_bgcolor="#111125",
    height=480,
    legend=dict(orientation="h", y=1.06, x=0, font=dict(size=11), bgcolor="rgba(0,0,0,0)"),
    xaxis=dict(title=x_title, showgrid=True, gridcolor="#1a1a2e",
               rangeslider=dict(visible=True, bgcolor="#0a0a14", thickness=0.04)),
    yaxis=dict(title="LMP ($/MWh)", showgrid=True, gridcolor="#1a1a2e",
               tickprefix="$", zeroline=True, zerolinecolor="#444"),
    hovermode="x unified", margin=dict(l=60,r=15,t=50,b=40), barmode="group"
)
st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# HOURLY PROFILE TABLE (single day)
# ─────────────────────────────────────────────────────────────────────────────
if view == "📅 Single Day (24h)":
    with st.expander("🕐 Hourly LMP Table"):
        pivot = filt.pivot_table(index="datetime", columns="bus", values="lmp").round(2)
        pivot.index = pivot.index.strftime("%H:%M")
        st.dataframe(pivot, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# MONTHLY SUMMARY TABLE
# ─────────────────────────────────────────────────────────────────────────────
if view == "📆 Monthly Average":
    st.markdown("---")
    st.markdown("#### 📋 Monthly Summary")
    rows = []
    for bus in selected_buses:
        bd = data[data["bus"]==bus]["lmp"]
        if bd.empty: continue
        rows.append({"Bus":bus, "Avg $/MWh":round(bd.mean(),2),
                     "Max":round(bd.max(),2), "Min":round(bd.min(),2),
                     "Std Dev":round(bd.std(),2), "Hours":len(bd)})
    if rows:
        st.dataframe(pd.DataFrame(rows).set_index("Bus"), use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────────────────────────────────
with st.expander("📄 Raw Data & Export"):
    show = data[data["bus"].isin(selected_buses)].sort_values("datetime", ascending=False)
    st.dataframe(show, use_container_width=True)
    st.download_button("⬇️ Download CSV",
                       show.to_csv(index=False).encode(),
                       "ercot_lmp.csv", "text/csv",
                       use_container_width=True)

st.caption("Data: ERCOT NP4-183-CD DAM Hourly LMP | Source: data.ercot.com")
