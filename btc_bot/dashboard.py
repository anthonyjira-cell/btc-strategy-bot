"""Web dashboard for the BTC strategy bot."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from btc_bot.strategy import BTCStrategy
    from btc_bot.btc_feed import BTCFeed

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BTC Strategy Dashboard</title>
<style>
  * { box-sizing:border-box; margin:0; padding:0 }
  body { background:#0d1117; color:#e6edf3; font-family:'Courier New',monospace; padding:24px }
  h1 { font-size:1.1rem; font-weight:bold; margin-bottom:4px }
  .sub { color:#8b949e; font-size:.85rem; margin-bottom:20px }
  .dot { display:inline-block; width:8px; height:8px; border-radius:50%;
         background:#3fb950; margin-right:6px; animation:pulse 2s infinite }
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .stats { display:flex; gap:32px; margin-bottom:20px; font-size:.9rem; color:#8b949e }
  .stats span { color:#e6edf3; font-weight:bold }
  .green { color:#3fb950 } .red { color:#f85149 } .yellow { color:#d29922 }
  table { width:100%; border-collapse:collapse; margin-bottom:20px }
  th { text-align:left; color:#8b949e; font-size:.8rem; padding:6px 10px;
       border-bottom:1px solid #21262d }
  td { padding:8px 10px; font-size:.85rem; border-bottom:1px solid #21262d }
  .badge { display:inline-block; padding:1px 6px; border-radius:3px; font-size:.75rem }
  .badge-arb { background:#1f6feb; color:#fff }
  .badge-dir { background:#388bfd33; color:#388bfd }
  .badge-hedge { background:#23863633; color:#3fb950 }
  .badge-stop { background:#f8514933; color:#f85149 }
  .mkt { display:flex; gap:16px; margin-bottom:20px; flex-wrap:wrap }
  .mkt-card { background:#161b22; border:1px solid #21262d; border-radius:6px;
              padding:12px 16px; min-width:200px; font-size:.85rem }
  .mkt-card .name { color:#8b949e; font-size:.75rem; margin-bottom:4px }
  .updated { color:#8b949e; font-size:.75rem; margin-top:16px }
</style>
</head>
<body>
<h1><span class="dot"></span>BTC Strategy <span id="mode-badge" style="color:#f85149;font-size:.8rem">LIVE</span></h1>
<div class="sub">5-min BTC Up/Down binary dislocation strategy on Polymarket</div>

<div class="stats">
  <div>BTC Price: <span id="btc">—</span> <span id="src" style="font-size:.7rem;color:#8b949e"></span></div>
  <div>Momentum: <span id="mom">—</span></div>
  <div>Cum PnL: <span id="pnl">—</span></div>
  <div>Trades: <span id="trades">—</span></div>
  <div>Open: <span id="open">—</span></div>
</div>

<div id="markets" class="mkt"></div>

<table>
  <thead><tr>
    <th>Time</th><th>Market</th><th>Type</th><th>Net</th><th>Cum PnL</th>
  </tr></thead>
  <tbody id="trade-rows"></tbody>
</table>

<div class="updated">Last updated: <span id="upd">—</span></div>

<script>
function cls(v){return v>0?'green':v<0?'red':''}
function fmt(v,d=2){return v==null?'--':(v>=0?'+':'')+v.toFixed(d)}
function momLabel(m){
  if(m>0.3) return '<span class="green">↑ Up</span>';
  if(m<-0.3) return '<span class="red">↓ Down</span>';
  return '<span class="yellow">→ Flat</span>';
}
function badgeType(t){
  const map={arb:'badge-arb',directional:'badge-dir','directional+hedge':'badge-hedge',
             delayed_hedge:'badge-hedge',stopped:'badge-stop'};
  return `<span class="badge ${map[t]||'badge-dir'}">${t}</span>`;
}

async function refresh(){
  try{
    const d=await fetch('/api/state').then(r=>r.json());
    document.getElementById('btc').textContent=d.btc_price?'$'+d.btc_price.toLocaleString():'—';
    document.getElementById('src').textContent=d.btc_source?`via ${d.btc_source}`:'';
    document.getElementById('mom').innerHTML=momLabel(d.momentum||0);
    const p=d.cum_pnl||0;
    document.getElementById('pnl').innerHTML=`<span class="${cls(p)}">${fmt(p,2)}</span>`;
    document.getElementById('trades').textContent=d.trade_count||0;
    document.getElementById('open').textContent=d.open_positions||0;

    document.getElementById('markets').innerHTML=(d.markets||[]).map(m=>`
      <div class="mkt-card" title="${m.question}">
        <div class="name">${m.label}</div>
        <div style="font-size:.75rem;color:#8b949e;margin-bottom:4px">${
          m.expires_in!=null
            ? (m.expires_in<2?`<span class="red">⏰ ${m.expires_in}h left</span>`
              :m.expires_in<24?`<span class="yellow">⏰ ${m.expires_in}h left</span>`
              :`<span style="color:#8b949e">⏰ ${m.expires_in}h left</span>`)
            : ''
        } &nbsp; Vol: $${(m.volume/1000).toFixed(0)}k</div>
        <div>YES <b>${m.yes_ask?.toFixed(3)||'--'}</b> &nbsp; NO <b>${m.no_ask?.toFixed(3)||'--'}</b></div>
        <div>Combined: <b class="${m.spread>0.03?'green':m.spread>0?'yellow':'red'}">${m.combined?.toFixed(4)||'--'}</b>
          &nbsp; Spread: <b class="${m.spread>0.03?'green':m.spread>0?'yellow':'red'}">${m.spread?.toFixed(4)||'--'}</b></div>
      </div>`).join('');

    document.getElementById('trade-rows').innerHTML=(d.recent_trades||[]).reverse().map(t=>`
      <tr>
        <td>${new Date(t.time*1000).toLocaleTimeString()}</td>
        <td>${t.market}</td>
        <td>${badgeType(t.type)}</td>
        <td class="${cls(t.net)}">${fmt(t.net,2)}</td>
        <td class="${cls(t.cum_pnl)}">${fmt(t.cum_pnl,2)}</td>
      </tr>`).join('');

    document.getElementById('upd').textContent=new Date().toLocaleTimeString();
  }catch(e){console.error(e)}
}
refresh();setInterval(refresh,3000);
</script>
</body>
</html>"""


def create_app(strategy: "BTCStrategy", feed: "BTCFeed",
               markets_holder: list) -> web.Application:
    routes = web.RouteTableDef()

    @routes.get("/")
    async def index(req):
        return web.Response(text=_HTML, content_type="text/html")

    @routes.get("/api/state")
    async def state(req):
        def hours_to_expiry(end_date: str):
            if not end_date:
                return None
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(end_date, fmt).replace(tzinfo=timezone.utc)
                    h = (dt - datetime.now(timezone.utc)).total_seconds() / 3600
                    return round(max(0.0, h), 1)
                except ValueError:
                    continue
            return None

        payload = {
            "btc_price":     feed.price,
            "btc_source":    feed.source,
            "momentum":      feed.momentum,
            "cum_pnl":       float(strategy.cumulative_pnl),
            "trade_count":   strategy.trade_count,
            "open_positions":strategy.open_positions,
            "markets": [
                {
                    "label":      m.label,
                    "question":   m.question,
                    "yes_ask":    float(m.yes_ask),
                    "no_ask":     float(m.no_ask),
                    "combined":   float(m.combined),
                    "spread":     float(m.spread),
                    "volume":     m.volume,
                    "expires_in": hours_to_expiry(m.end_date),
                }
                for m in markets_holder
            ],
            "recent_trades": strategy.recent_trades,
        }
        return web.Response(text=json.dumps(payload), content_type="application/json")

    app = web.Application()
    app.add_routes(routes)
    return app


async def start_dashboard(app: web.Application, port: int) -> web.AppRunner:
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    return runner
