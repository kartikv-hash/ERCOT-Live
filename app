
import { useState, useEffect, useCallback, useRef } from "react";
import {
  LineChart, Line, AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine, Brush
} from "recharts";

// ─── Constants ────────────────────────────────────────────────────────────────
const ERCOT_API = "https://api.ercot.com/api/public-reports";
const NODES = [
  "HB_HOUSTON","HB_NORTH","HB_SOUTH","HB_WEST",
  "LZ_HOUSTON","LZ_NORTH","LZ_SOUTH","LZ_WEST",
  "LZ_AEN","LZ_CPS","LZ_LCRA","LZ_RAYBN",
  "HOUSTON","NORTH","SOUTH","WEST"
];
const COLORS = ["#00d4ff","#ff6b6b","#51cf66","#ffd43b","#cc5de8","#ff922b"];
const AGGREGATIONS = ["Hourly","Daily","Monthly","Yearly"];

// ─── Helper ───────────────────────────────────────────────────────────────────
const fmt = (d) => new Date(d).toLocaleDateString("en-US",{month:"short",day:"numeric"});
const fmtM = (d) => new Date(d).toLocaleDateString("en-US",{year:"numeric",month:"short"});
const fmtY = (d) => new Date(d).getFullYear().toString();
const avg = (arr) => arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : 0;
const toISO = (d) => d.toISOString().split("T")[0];

function addDays(d, n) { const x=new Date(d); x.setDate(x.getDate()+n); return x; }
function addMonths(d, n) { const x=new Date(d); x.setMonth(x.getMonth()+n); return x; }
function addYears(d, n) { const x=new Date(d); x.setFullYear(x.getFullYear()+n); return x; }

// ─── Fetch ERCOT via Anthropic API (bypass CORS) ─────────────────────────────
async function fetchViaAnthropic(prompt) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "claude-sonnet-4-20250514",
      max_tokens: 4000,
      tools: [{ type: "web_search_20250305", name: "web_search" }],
      system: `You are an ERCOT data retrieval agent. When asked for LMP data, search ERCOT public API or web for the data and return ONLY a valid JSON array. No markdown, no explanation. Just raw JSON array.`,
      messages: [{ role: "user", content: prompt }]
    })
  });
  const data = await res.json();
  const text = data.content.filter(c=>c.type==="text").map(c=>c.text).join("");
  const clean = text.replace(/```json|```/g,"").trim();
  try { return JSON.parse(clean); } catch { return null; }
}

// ─── Direct ERCOT API Fetch ───────────────────────────────────────────────────
async function fetchERCOTDirect(endpoint, params) {
  const url = new URL(`${ERCOT_API}/${endpoint}`);
  Object.entries(params).forEach(([k,v]) => url.searchParams.set(k,v));
  url.searchParams.set("size","9999");
  const res = await fetch(url.toString(), {
    headers: { "Accept": "application/json", "Ocp-Apim-Subscription-Key": "" }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// ─── Generate Demo Data (fallback) ───────────────────────────────────────────
function genDemoData(node, from, to, agg) {
  const data = [];
  let d = new Date(from);
  const end = new Date(to);
  const seed = node.split("").reduce((a,c)=>a+c.charCodeAt(0),0);
  const basePrice = 30 + (seed % 40);
  let idx = 0;
  while (d <= end) {
    const t = d.getTime()/1000;
    const seasonal = Math.sin(t/5184000)*15;
    const daily = Math.sin(t/43200)*8;
    const spike = Math.random()<0.03 ? Math.random()*120 : 0;
    const noise = (Math.random()-0.5)*12;
    const lmp = Math.max(0, basePrice + seasonal + daily + spike + noise);
    let label;
    if (agg==="Hourly") label = new Date(d).toISOString();
    else if (agg==="Daily") label = toISO(d);
    else if (agg==="Monthly") label = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}`;
    else label = d.getFullYear().toString();
    data.push({ date: label, lmp: +lmp.toFixed(2), node });
    if (agg==="Hourly") d = addDays(d,1/24);
    else if (agg==="Daily") d = addDays(d,1);
    else if (agg==="Monthly") d = addMonths(d,1);
    else d = addYears(d,1);
    idx++;
    if (idx > 3000) break;
  }
  return data;
}

// ─── Aggregate raw hourly records ────────────────────────────────────────────
function aggregate(records, agg) {
  if (!records.length) return [];
  const grouped = {};
  records.forEach(r => {
    let key;
    const d = new Date(r.date || r.deliveryDate || r.timestamp);
    if (agg==="Daily") key = toISO(d);
    else if (agg==="Monthly") key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}`;
    else if (agg==="Yearly") key = d.getFullYear().toString();
    else key = r.date;
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(r.lmp);
  });
  return Object.entries(grouped)
    .map(([k,v]) => ({ date: k, lmp: +avg(v).toFixed(2) }))
    .sort((a,b) => a.date.localeCompare(b.date));
}

// ─── Custom Tooltip ───────────────────────────────────────────────────────────
const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{background:"#1a1a2e",border:"1px solid #333",borderRadius:8,padding:"10px 14px"}}>
      <p style={{color:"#aaa",margin:0,fontSize:12}}>{label}</p>
      {payload.map((p,i) => (
        <p key={i} style={{color:p.color,margin:"4px 0 0",fontSize:13,fontWeight:600}}>
          {p.name}: <span style={{color:"#fff"}}>${p.value?.toFixed(2)}/MWh</span>
        </p>
      ))}
    </div>
  );
};

// ─── Main Component ───────────────────────────────────────────────────────────
export default function ERCOTDashboard() {
  const [selectedNodes, setSelectedNodes] = useState(["HB_HOUSTON"]);
  const [aggregation, setAggregation] = useState("Daily");
  const [chartType, setChartType] = useState("area");
  const [dateFrom, setDateFrom] = useState(toISO(addYears(new Date(),-1)));
  const [dateTo, setDateTo] = useState(toISO(new Date()));
  const [chartData, setChartData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [dataSource, setDataSource] = useState("");
  const [stats, setStats] = useState({});
  const [nodeSearch, setNodeSearch] = useState("");
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const abortRef = useRef(null);

  const fetchData = useCallback(async () => {
    setLoading(true); setError(""); setDataSource("");
    if (abortRef.current) abortRef.current = false;
    const current = {};
    abortRef.current = current;

    try {
      let allData = [];
      // Try direct ERCOT API first
      try {
        const results = await Promise.all(selectedNodes.map(async node => {
          const raw = await fetchERCOTDirect("np6-785-er/spp_node_zone_hub", {
            deliveryDateFrom: dateFrom,
            deliveryDateTo: dateTo,
            settlementPoint: node
          });
          const records = (raw?.data || raw?.items || []).map(r => ({
            date: r.deliveryDate || r.deliveryDatetime,
            lmp: parseFloat(r.settlementPointPrice || r.lmp || 0),
            node
          }));
          return records;
        }));
        results.forEach(r => allData.push(...r));
        if (allData.length > 0) setDataSource("✅ Live ERCOT API");
      } catch (e) {
        // CORS fallback — use demo data
        setDataSource("📊 Demo Data (CORS restricted — deploy with backend proxy for live data)");
        selectedNodes.forEach(node => {
          allData.push(...genDemoData(node, dateFrom, dateTo, aggregation));
        });
      }

      if (current !== abortRef.current) return;

      // Build combined chart data
      const byNode = {};
      selectedNodes.forEach(n => {
        const nodeRecords = allData.filter(r => r.node === n);
        const agged = aggregate(nodeRecords, aggregation);
        agged.forEach(r => {
          if (!byNode[r.date]) byNode[r.date] = { date: r.date };
          byNode[r.date][n] = r.lmp;
        });
      });

      const combined = Object.values(byNode).sort((a,b) => a.date.localeCompare(b.date));
      setChartData(combined);

      // Stats
      const s = {};
      selectedNodes.forEach(n => {
        const vals = combined.map(r=>r[n]).filter(Boolean);
        s[n] = {
          avg: avg(vals).toFixed(2),
          max: Math.max(...vals).toFixed(2),
          min: Math.min(...vals).toFixed(2),
          count: vals.length
        };
      });
      setStats(s);
    } catch(e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [selectedNodes, dateFrom, dateTo, aggregation]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const toggleNode = (n) => {
    setSelectedNodes(prev =>
      prev.includes(n)
        ? prev.filter(x => x !== n)
        : [...prev.slice(-5), n]
    );
  };

  const filteredNodes = NODES.filter(n => n.toLowerCase().includes(nodeSearch.toLowerCase()));

  const fmtLabel = (v) => {
    if (aggregation==="Monthly") return fmtM(v+"-01");
    if (aggregation==="Yearly") return v;
    return fmt(v);
  };

  // ─── Preset date ranges ────────────────────────────────────────────────────
  const presets = [
    {label:"1D", from:toISO(addDays(new Date(),-1)), agg:"Hourly"},
    {label:"1W", from:toISO(addDays(new Date(),-7)), agg:"Hourly"},
    {label:"1M", from:toISO(addMonths(new Date(),-1)), agg:"Daily"},
    {label:"3M", from:toISO(addMonths(new Date(),-3)), agg:"Daily"},
    {label:"1Y", from:toISO(addYears(new Date(),-1)), agg:"Monthly"},
    {label:"3Y", from:toISO(addYears(new Date(),-3)), agg:"Monthly"},
    {label:"5Y", from:toISO(addYears(new Date(),-5)), agg:"Yearly"},
    {label:"10Y", from:toISO(addYears(new Date(),-10)), agg:"Yearly"},
  ];

  const s = {
    app: {display:"flex",height:"100vh",background:"#0d0d1a",color:"#e0e0e0",fontFamily:"'Inter',sans-serif",overflow:"hidden"},
    sidebar: {width:sidebarOpen?240:0,minWidth:sidebarOpen?240:0,overflow:"hidden",transition:"all .3s",background:"#111125",borderRight:"1px solid #1e1e3a",display:"flex",flexDirection:"column"},
    sideInner: {padding:16,display:"flex",flexDirection:"column",gap:12,overflowY:"auto",flex:1},
    main: {flex:1,display:"flex",flexDirection:"column",overflow:"hidden"},
    header: {padding:"14px 20px",background:"#111125",borderBottom:"1px solid #1e1e3a",display:"flex",alignItems:"center",gap:12},
    body: {flex:1,overflowY:"auto",padding:20,display:"flex",flexDirection:"column",gap:16},
    card: {background:"#111125",border:"1px solid #1e1e3a",borderRadius:12,padding:16},
    label: {fontSize:11,color:"#666",textTransform:"uppercase",letterSpacing:1,marginBottom:6},
    btn: (active) => ({padding:"5px 12px",borderRadius:6,border:"1px solid "+(active?"#00d4ff":"#333"),background:active?"rgba(0,212,255,.15)":"transparent",color:active?"#00d4ff":"#888",cursor:"pointer",fontSize:12,fontWeight:active?600:400}),
    input: {background:"#0d0d1a",border:"1px solid #2a2a4a",borderRadius:6,color:"#e0e0e0",padding:"6px 10px",fontSize:12,width:"100%",boxSizing:"border-box"},
    nodeBtn: (active) => ({padding:"4px 10px",borderRadius:6,border:"1px solid "+(active?"#00d4ff":"#222"),background:active?"rgba(0,212,255,.1)":"#0d0d1a",color:active?"#00d4ff":"#888",cursor:"pointer",fontSize:11,width:"100%",textAlign:"left",marginBottom:4}),
    statCard: (c) => ({background:`rgba(${c},0.08)`,border:`1px solid rgba(${c},0.25)`,borderRadius:8,padding:"10px 14px",flex:1,minWidth:100}),
  };

  return (
    <div style={s.app}>
      {/* Sidebar */}
      <div style={s.sidebar}>
        <div style={s.sideInner}>
          <div>
            <div style={s.label}>Nodes</div>
            <input
              style={s.input} placeholder="Search node..."
              value={nodeSearch} onChange={e=>setNodeSearch(e.target.value)}
            />
            <div style={{marginTop:8}}>
              {filteredNodes.map(n => (
                <button key={n} style={s.nodeBtn(selectedNodes.includes(n))} onClick={()=>toggleNode(n)}>
                  {selectedNodes.includes(n)?"✓ ":""}{n}
                </button>
              ))}
            </div>
          </div>

          <div>
            <div style={s.label}>Aggregation</div>
            <div style={{display:"flex",flexWrap:"wrap",gap:4}}>
              {AGGREGATIONS.map(a=>(
                <button key={a} style={s.btn(aggregation===a)} onClick={()=>setAggregation(a)}>{a}</button>
              ))}
            </div>
          </div>

          <div>
            <div style={s.label}>Chart Type</div>
            <div style={{display:"flex",gap:4}}>
              {["area","line","bar"].map(t=>(
                <button key={t} style={s.btn(chartType===t)} onClick={()=>setChartType(t)}>{t}</button>
              ))}
            </div>
          </div>

          <div>
            <div style={s.label}>Date Range</div>
            <div style={{display:"flex",flexDirection:"column",gap:6}}>
              <input type="date" style={s.input} value={dateFrom} onChange={e=>setDateFrom(e.target.value)} />
              <input type="date" style={s.input} value={dateTo} onChange={e=>setDateTo(e.target.value)} />
            </div>
          </div>

          <div>
            <div style={s.label}>Quick Ranges</div>
            <div style={{display:"flex",flexWrap:"wrap",gap:4}}>
              {presets.map(p=>(
                <button key={p.label} style={s.btn(dateFrom===p.from)} onClick={()=>{setDateFrom(p.from);setDateTo(toISO(new Date()));setAggregation(p.agg);}}>
                  {p.label}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Main */}
      <div style={s.main}>
        {/* Header */}
        <div style={s.header}>
          <button onClick={()=>setSidebarOpen(o=>!o)} style={{background:"none",border:"none",color:"#888",cursor:"pointer",fontSize:18,padding:0}}>☰</button>
          <div>
            <div style={{fontWeight:700,fontSize:16,color:"#fff"}}>⚡ ERCOT LMP Dashboard</div>
            <div style={{fontSize:11,color:"#555"}}>Locational Marginal Pricing — Electrical Bus</div>
          </div>
          <div style={{flex:1}}/>
          {dataSource && <div style={{fontSize:11,color:"#666",background:"#0d0d1a",padding:"4px 10px",borderRadius:6,border:"1px solid #222"}}>{dataSource}</div>}
          <button onClick={fetchData} style={{padding:"6px 14px",background:"rgba(0,212,255,.15)",border:"1px solid #00d4ff",color:"#00d4ff",borderRadius:6,cursor:"pointer",fontSize:12,fontWeight:600}}>
            {loading ? "⏳ Loading..." : "🔄 Refresh"}
          </button>
        </div>

        <div style={s.body}>
          {/* Stats */}
          {Object.keys(stats).length > 0 && (
            <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
              {selectedNodes.map((n,i) => {
                const st = stats[n];
                if (!st) return null;
                const c = i===0?"0,212,255":i===1?"255,107,107":i===2?"81,207,102":"255,212,59";
                return (
                  <div key={n} style={s.statCard(c)}>
                    <div style={{fontSize:10,color:"#888",marginBottom:4}}>{n}</div>
                    <div style={{display:"flex",gap:12}}>
                      <div><div style={{fontSize:10,color:"#666"}}>AVG</div><div style={{fontSize:15,fontWeight:700,color:`rgb(${c})`}}>${st.avg}</div></div>
                      <div><div style={{fontSize:10,color:"#666"}}>MAX</div><div style={{fontSize:15,fontWeight:700,color:"#ff6b6b"}}>${st.max}</div></div>
                      <div><div style={{fontSize:10,color:"#666"}}>MIN</div><div style={{fontSize:15,fontWeight:700,color:"#51cf66"}}>${st.min}</div></div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {/* Main Chart */}
          <div style={{...s.card, flex:1, minHeight:320}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
              <div style={{fontWeight:600,fontSize:14,color:"#fff"}}>
                LMP Price — {aggregation} Average &nbsp;
                <span style={{fontSize:11,color:"#555",fontWeight:400}}>$/MWh</span>
              </div>
              <div style={{fontSize:11,color:"#444"}}>{chartData.length} data points</div>
            </div>

            {loading ? (
              <div style={{height:280,display:"flex",alignItems:"center",justifyContent:"center"}}>
                <div style={{textAlign:"center"}}>
                  <div style={{fontSize:32,marginBottom:8}}>⏳</div>
                  <div style={{color:"#555",fontSize:13}}>Fetching ERCOT data...</div>
                </div>
              </div>
            ) : error ? (
              <div style={{height:280,display:"flex",alignItems:"center",justifyContent:"center",color:"#ff6b6b"}}>{error}</div>
            ) : chartData.length===0 ? (
              <div style={{height:280,display:"flex",alignItems:"center",justifyContent:"center",color:"#555"}}>No data</div>
            ) : (
              <ResponsiveContainer width="100%" height={300}>
                {chartType==="bar" ? (
                  <BarChart data={chartData} margin={{top:5,right:20,left:0,bottom:5}}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e1e3a" />
                    <XAxis dataKey="date" tickFormatter={fmtLabel} tick={{fill:"#555",fontSize:11}} />
                    <YAxis tick={{fill:"#555",fontSize:11}} tickFormatter={v=>`$${v}`} />
                    <Tooltip content={<CustomTooltip/>} />
                    <Legend wrapperStyle={{color:"#888",fontSize:12}} />
                    {selectedNodes.map((n,i)=>(
                      <Bar key={n} dataKey={n} fill={COLORS[i%COLORS.length]} opacity={0.85} radius={[2,2,0,0]} />
                    ))}
                  </BarChart>
                ) : chartType==="line" ? (
                  <LineChart data={chartData} margin={{top:5,right:20,left:0,bottom:5}}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e1e3a" />
                    <XAxis dataKey="date" tickFormatter={fmtLabel} tick={{fill:"#555",fontSize:11}} />
                    <YAxis tick={{fill:"#555",fontSize:11}} tickFormatter={v=>`$${v}`} />
                    <Tooltip content={<CustomTooltip/>} />
                    <Legend wrapperStyle={{color:"#888",fontSize:12}} />
                    <ReferenceLine y={0} stroke="#333" />
                    {selectedNodes.map((n,i)=>(
                      <Line key={n} type="monotone" dataKey={n} stroke={COLORS[i%COLORS.length]} strokeWidth={2} dot={false} activeDot={{r:4}} />
                    ))}
                    <Brush dataKey="date" height={20} stroke="#1e1e3a" fill="#0d0d1a" travellerWidth={6} />
                  </LineChart>
                ) : (
                  <AreaChart data={chartData} margin={{top:5,right:20,left:0,bottom:5}}>
                    <defs>
                      {selectedNodes.map((n,i)=>(
                        <linearGradient key={n} id={`grad${i}`} x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor={COLORS[i%COLORS.length]} stopOpacity={0.3}/>
                          <stop offset="95%" stopColor={COLORS[i%COLORS.length]} stopOpacity={0}/>
                        </linearGradient>
                      ))}
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e1e3a" />
                    <XAxis dataKey="date" tickFormatter={fmtLabel} tick={{fill:"#555",fontSize:11}} />
                    <YAxis tick={{fill:"#555",fontSize:11}} tickFormatter={v=>`$${v}`} />
                    <Tooltip content={<CustomTooltip/>} />
                    <Legend wrapperStyle={{color:"#888",fontSize:12}} />
                    {selectedNodes.map((n,i)=>(
                      <Area key={n} type="monotone" dataKey={n} stroke={COLORS[i%COLORS.length]} strokeWidth={2}
                        fill={`url(#grad${i})`} dot={false} activeDot={{r:4}} />
                    ))}
                    <Brush dataKey="date" height={20} stroke="#1e1e3a" fill="#0d0d1a" travellerWidth={6} />
                  </AreaChart>
                )}
              </ResponsiveContainer>
            )}
          </div>

          {/* Comparison bar — yearly avg if multi-node */}
          {selectedNodes.length > 1 && chartData.length > 0 && (
            <div style={{...s.card}}>
              <div style={{fontWeight:600,fontSize:13,color:"#fff",marginBottom:12}}>Node Comparison — Average LMP</div>
              <div style={{display:"flex",gap:8,alignItems:"flex-end",height:80}}>
                {selectedNodes.map((n,i)=>{
                  const vals = chartData.map(r=>r[n]).filter(Boolean);
                  const a = avg(vals);
                  const maxA = Math.max(...selectedNodes.map(nn=>{
                    const v = chartData.map(r=>r[nn]).filter(Boolean); return avg(v);
                  }));
                  const pct = maxA>0 ? (a/maxA)*100 : 0;
                  return (
                    <div key={n} style={{flex:1,textAlign:"center"}}>
                      <div style={{fontSize:11,color:COLORS[i%COLORS.length],marginBottom:4}}>${a.toFixed(2)}</div>
                      <div style={{height:`${Math.max(10,pct*0.6)}px`,background:COLORS[i%COLORS.length],borderRadius:"4px 4px 0 0",opacity:0.8}}/>
                      <div style={{fontSize:10,color:"#555",marginTop:4}}>{n}</div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Info footer */}
          <div style={{fontSize:11,color:"#444",textAlign:"center",paddingBottom:8}}>
            Data source: ERCOT Public API (np6-785-er/spp_node_zone_hub) • Settlement Point Prices •{" "}
            <span style={{color:"#555"}}>For live data deploy with a backend proxy to bypass browser CORS restrictions</span>
          </div>
        </div>
      </div>
    </div>
  );
}
