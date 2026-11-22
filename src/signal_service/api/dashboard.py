"""Gold-only dashboard. Reads GOLD_* plus the PIPELINE_RUNS audit.

Chat cannot write warehouse rows. When agent launch is enabled, a confirmed
tool call can open a pull request through the Cursor Cloud Agents API.
"""

from __future__ import annotations

import json
from datetime import date, datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from signal_service.cloud_agents import (
    TOOL_DEFINITIONS,
    TOOL_SYSTEM,
    execute_tool_call,
    launch_cloud_agent,
    launch_enabled,
)
from signal_service.paths import repo_root
from signal_service.safety.cost import CircuitBreakerOpen
from signal_service.safety.redaction import RedactionError, assert_redacted

PERSONAS = ("atlas", "sentinel", "diagnostician", "builder", "narrator")


class ChatBody(BaseModel):
    persona: str = "narrator"
    message: str = Field(min_length=1, max_length=2000)
    confirm: bool = False
    launch_task: str | None = Field(default=None, max_length=2000)


def _cell(value):
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 6)
    return value


def _rows(warehouse, sql: str, params: list | None = None) -> list[dict]:
    return [{key: _cell(value) for key, value in row.items()} for row in warehouse.query(sql, params or [])]


def collect(warehouse) -> dict:
    """Read the gold marts. This function does not query earlier layers."""
    health = _rows(
        warehouse,
        """
        SELECT layer, row_count, fresh_at, latest_status, suspect_rows
        FROM gold_health
        """,
    )
    quality = _rows(
        warehouse,
        """
        SELECT batch_id, "table", "key", reason, first_seen
        FROM gold_data_quality
        ORDER BY first_seen DESC
        LIMIT 50
        """,
    )
    conversion = _rows(
        warehouse,
        """
        SELECT hour,
               SUM(sessions)::INTEGER AS sessions,
               SUM(converted)::INTEGER AS converted,
               SUM(converted)::DOUBLE / NULLIF(SUM(sessions), 0) AS conversion_rate,
               AVG(baseline_rate)::DOUBLE AS baseline_rate,
               MAX(CASE WHEN is_anomaly THEN 1 ELSE 0 END)::INTEGER AS is_anomaly,
               SUM(dq_suspect_rows)::INTEGER AS dq_suspect_rows
        FROM gold_hourly_conversion
        WHERE platform = 'android' AND country = 'IN'
        GROUP BY hour
        ORDER BY hour
        """,
    )
    anomalies = _rows(
        warehouse,
        """
        SELECT hour, platform, country, payment_method, app_version,
               sessions, conversion_rate, baseline_rate
        FROM gold_hourly_conversion
        WHERE is_anomaly
        ORDER BY hour DESC
        LIMIT 12
        """,
    )
    revenue_rows = _rows(
        warehouse,
        """
        SELECT hour,
               SUM(revenue_at_risk_usd)::DOUBLE AS revenue_at_risk_usd,
               SUM(dq_suspect_rows)::INTEGER AS dq_suspect_rows
        FROM gold_revenue_at_risk
        WHERE platform = 'android' AND country = 'IN'
        GROUP BY hour
        ORDER BY hour
        """,
    )
    revenue_total = _rows(
        warehouse,
        "SELECT COALESCE(SUM(revenue_at_risk_usd), 0)::DOUBLE AS revenue_at_risk_usd FROM gold_revenue_at_risk",
    )
    cases = _rows(
        warehouse,
        """
        SELECT case_id, metric, segment_json, status, severity, revenue_at_risk_usd, opened_at
        FROM gold_case_facts
        WHERE status IS NULL OR LOWER(status) <> 'closed'
        ORDER BY opened_at DESC
        """,
    )
    topics = _rows(
        warehouse,
        """
        SELECT topic,
               SUM(ticket_count)::INTEGER AS ticket_count,
               AVG(avg_confidence)::DOUBLE AS avg_confidence
        FROM gold_ticket_topics
        GROUP BY topic
        ORDER BY ticket_count DESC
        """,
    )
    spend = _rows(
        warehouse,
        """
        SELECT spend_date, spend_usd, cap_usd, headroom_usd, _batch_id
        FROM gold_daily_spend
        ORDER BY spend_date DESC
        LIMIT 14
        """,
    )
    lag = _rows(
        warehouse,
        """
        SELECT *
        FROM gold_pipeline_lag
        ORDER BY 2 DESC
        LIMIT 8
        """,
    )
    return {
        "health": health,
        "conversion": conversion,
        "anomalies": anomalies,
        "anomaly_count": len(anomalies),
        "revenue": revenue_rows,
        "revenue_at_risk_usd": revenue_total[0]["revenue_at_risk_usd"] if revenue_total else 0,
        "cases": cases,
        "topics": topics,
        "spend": spend,
        "lag": lag,
        "quality": quality,
        "suspect_rows": sum(int(row.get("suspect_rows") or 0) for row in health if row.get("layer") == "gold"),
    }


def list_runs(warehouse, batch_id: str | None = None) -> list[dict]:
    if batch_id:
        return _rows(
            warehouse,
            """
            SELECT run_id, batch_id, layer, started_at, finished_at, status,
                   rows_in, rows_out, rows_rejected, error
            FROM pipeline_runs
            WHERE batch_id = ?
            ORDER BY started_at
            """,
            [batch_id],
        )
    return _rows(
        warehouse,
        """
        SELECT run_id, batch_id, layer, started_at, finished_at, status,
               rows_in, rows_out, rows_rejected, error
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 20
        """,
    )


def _persona_text(name: str) -> str:
    if name not in PERSONAS:
        raise ValueError(f"unknown persona {name}")
    path = repo_root() / "bots" / f"{name}.md"
    if not path.is_file():
        raise ValueError(f"unknown persona {name}")
    return path.read_text(encoding="utf-8")


def _summary(data: dict) -> dict:
    latest_spend = data["spend"][0] if data["spend"] else {}
    return {
        "anomaly_count": data["anomaly_count"],
        "revenue_at_risk_usd": round(float(data["revenue_at_risk_usd"] or 0), 2),
        "open_cases": [
            {
                "case_id": row["case_id"],
                "metric": row["metric"],
                "status": row["status"],
                "severity": row["severity"],
            }
            for row in data["cases"]
        ],
        "topics": data["topics"][:6],
        "spend_usd": latest_spend.get("spend_usd", 0),
        "cap_usd": latest_spend.get("cap_usd", 25),
        "health": data["health"],
        "suspect_rows": data.get("suspect_rows") or 0,
        "suspect_reasons": sorted({row.get("reason") for row in data.get("quality") or [] if row.get("reason")}),
    }


def mock_answer(summary: dict) -> str:
    revenue = float(summary.get("revenue_at_risk_usd") or 0)
    spend = float(summary.get("spend_usd") or 0)
    cap = float(summary.get("cap_usd") or 0)
    cases = summary.get("open_cases") or []
    case_text = ", ".join(item["case_id"] for item in cases[:3]) or "none"
    topics = summary.get("topics") or []
    topic_text = ", ".join(f"{item['topic']} ({item['ticket_count']})" for item in topics[:3]) or "none yet"
    suspects = int(summary.get("suspect_rows") or 0)
    quality = ""
    if suspects:
        reasons = ", ".join(summary.get("suspect_reasons") or []) or "unspecified"
        quality = f" Data quality: {suspects} suspect rows ({reasons}). Affected metrics are marked."
    return (
        f"Gold marts: {summary.get('anomaly_count', 0)} hourly slices are below the seasonal baseline. "
        f"Revenue at risk across the mart is ${revenue:,.0f}. "
        f"Open cases: {case_text}. "
        f"Ticket topics: {topic_text}. "
        f"Recorded spend is ${spend:.2f} against a ${cap:.0f} cap."
        f"{quality}"
    )


def _from_tool(client, persona: str, result: dict) -> dict:
    reply = str(result.get("message") or "Cloud agent request failed.")
    task = result.get("task")
    if result.get("status") == "confirmation_required" and task:
        reply = f"{reply}\n\nTask: {task}"
    assert_redacted(reply)
    payload = {
        "provider": getattr(client, "provider", "mock"),
        "persona": persona,
        "reply": reply,
        "tool_result": result,
    }
    if result.get("status") == "confirmation_required" and task:
        payload["pending_launch"] = {"task": task}
    return payload


def answer(
    warehouse,
    client,
    persona: str,
    message: str,
    *,
    confirm: bool = False,
    launch_task: str | None = None,
) -> dict:
    """Read gold, then ask the model. This function does not land or change rows."""
    assert_redacted(message)
    if persona not in PERSONAS:
        raise ValueError(f"unknown persona {persona}")
    if launch_task is not None and str(launch_task).strip():
        assert_redacted(launch_task)
        result = launch_cloud_agent(str(launch_task), confirmed=confirm)
        return _from_tool(client, persona, result)
    summary = _summary(collect(warehouse))
    system = (
        _persona_text(persona)
        + "\n\nYou are answering from the gold mart only. "
        + "Do not invent numbers. Do not ask to load data or change the warehouse. "
        + "When suspect_rows is greater than zero, mention the suspect data and its reasons.\n"
        + json.dumps(summary, default=str)
    )
    provider = getattr(client, "provider", "mock")
    if provider == "grok" and launch_enabled():
        system = system + "\n\n" + TOOL_SYSTEM
    assert_redacted(system)
    if provider != "grok":
        reply = mock_answer(summary)
    elif launch_enabled():
        completed = client.complete(system=system, user=message, tools=TOOL_DEFINITIONS)
        tool_calls = completed.get("tool_calls") or []
        if tool_calls:
            result = execute_tool_call(tool_calls[0])
            payload = _from_tool(client, persona, result)
            content = completed.get("content") or ""
            if content:
                payload["reply"] = content + "\n\n" + payload["reply"]
                assert_redacted(payload["reply"])
            return payload
        reply = completed.get("content") or ""
    else:
        reply = client.chat(system=system, user=message)
    assert_redacted(reply)
    return {"provider": provider, "persona": persona, "reply": reply}


def _page(data: dict) -> str:
    blob = json.dumps(data).replace("<", "\\u003c")
    options = "".join(f'<option value="{name}">{name}</option>' for name in PERSONAS)
    health = "".join(
        f"<li><strong>{_esc(row['layer'])}</strong> "
        f"{_esc(row['row_count'])} rows, status {_esc(row['latest_status'])}, "
        f"fresh {_esc(row['fresh_at'])}</li>"
        for row in data["health"]
    )
    banner = "Gold is current"
    if any(row.get("latest_status") not in {None, "success"} for row in data["health"]):
        banner = "A layer step needs attention"
    suspects = data.get("quality") or []
    if suspects:
        quality_banner = (
            f"Data quality: {len(suspects)} suspect row"
            f"{'' if len(suspects) == 1 else 's'}. Affected metrics are marked."
        )
        quality_class = "banner warn"
    else:
        quality_banner = "Data quality: no suspect rows."
        quality_class = "banner"
    quality_rows = "".join(
        "<tr class=\"suspect\">"
        f"<td>{_esc(row.get('batch_id'))}</td>"
        f"<td>{_esc(row.get('table'))}</td>"
        f"<td>{_esc(row.get('key'))}</td>"
        f"<td>{_esc(row.get('reason'))}</td>"
        f"<td>{_esc(row.get('first_seen'))}</td>"
        "</tr>"
        for row in suspects
    ) or "<tr><td colspan=\"5\">No suspect rows.</td></tr>"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Signal gold</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.6/dist/chart.umd.min.js"></script>
  <style>
    :root {{ color-scheme: light; --ink: #1c2430; --paper: #f3efe6; --card: #fffdf8; --line: #d9d0c1; --teal: #0f5c56; --rust: #9a3412; }}
    body {{ margin: 0; font-family: "Iowan Old Style", Palatino, Georgia, serif; background: var(--paper); color: var(--ink); }}
    header {{ background: var(--ink); color: #f6f1e7; padding: 1.25rem 1.5rem; display: flex; justify-content: space-between; gap: 1rem; align-items: center; }}
    header p {{ margin: 0.2rem 0 0; font-family: "Avenir Next", "Segoe UI", sans-serif; font-size: 0.9rem; }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 1rem; display: grid; gap: 1rem; }}
    .banner {{ padding: 0.8rem 1rem; background: #e7f2f0; border-left: 4px solid var(--teal); }}
    .banner.warn {{ background: #fde8df; border-color: var(--rust); }}
    section {{ background: var(--card); border: 1px solid var(--line); padding: 0.9rem 1rem; }}
    h1, h2 {{ font-weight: 500; margin: 0 0 0.5rem; }}
    h2 {{ font-size: 1.15rem; }}
    .grid {{ display: grid; grid-template-columns: 1.4fr 1fr; gap: 1rem; }}
    canvas {{ width: 100%; max-height: 280px; }}
    table {{ width: 100%; border-collapse: collapse; font-family: "Avenir Next", "Segoe UI", sans-serif; font-size: 0.88rem; }}
    th, td {{ text-align: left; padding: 0.3rem 0.4rem; border-bottom: 1px solid var(--line); }}
    tr.suspect td {{ background: #fde8df; }}
    .mark {{ color: #9a3412; font-weight: 600; }}
    button, select, textarea {{ font: inherit; }}
    button {{ background: var(--teal); color: white; border: 0; padding: 0.55rem 0.8rem; cursor: pointer; }}
    textarea {{ width: 100%; min-height: 5rem; }}
    #reply {{ white-space: pre-wrap; }}
    @media (max-width: 800px) {{ .grid {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Signal gold</h1>
      <p>Marts only. A run moves from bronze to silver to gold after you ask for data.</p>
    </div>
    <button id="load" type="button">Load new data</button>
  </header>
  <main>
    <div id="banner" class="banner">{banner}</div>
    <div id="quality-banner" class="{quality_class}">{quality_banner}</div>
    <section id="data-quality">
      <h2>Data quality</h2>
      <table>
        <tr><th>Batch</th><th>Table</th><th>Key</th><th>Reason</th><th>First seen</th></tr>
        {quality_rows}
      </table>
    </section>
    <section>
      <h2>Layer freshness</h2>
      <ul id="health">{health}</ul>
      <div id="runs"></div>
    </section>
    <div class="grid">
      <section>
        <h2>Android India conversion vs baseline</h2>
        <canvas id="conversion"></canvas>
      </section>
      <section>
        <h2>Revenue at risk</h2>
        <p id="revenue-total"></p>
        <canvas id="revenue"></canvas>
      </section>
    </div>
    <div class="grid">
      <section>
        <h2>Open cases</h2>
        <div id="cases"></div>
      </section>
      <section>
        <h2>Ticket topics</h2>
        <div id="topics"></div>
      </section>
    </div>
    <section>
      <h2>Spend vs cap</h2>
      <div id="spend"></div>
    </section>
    <section>
      <h2>Ask the gold mart</h2>
      <label>Persona <select id="persona">{options}</select></label>
      <textarea id="message" placeholder="What changed in Android India checkout?"></textarea>
      <button id="ask" type="button">Ask</button>
      <button id="confirm-launch" type="button" hidden>Confirm cloud agent launch</button>
      <p id="reply"></p>
    </section>
  </main>
  <script>
    const initial = {blob};
    let conversionChart;
    let revenueChart;
    function money(value) {{
      return '$' + Number(value || 0).toLocaleString(undefined, {{maximumFractionDigits: 0}});
    }}
    function table(columns, rows) {{
      if (!rows.length) return '<p>Nothing here yet.</p>';
      const head = '<tr>' + columns.map(col => '<th>' + col[0] + '</th>').join('') + '</tr>';
      const body = rows.map(row => '<tr>' + columns.map(col => '<td>' + (row[col[1]] ?? '') + '</td>').join('') + '</tr>').join('');
      return '<table>' + head + body + '</table>';
    }}
    function draw(data) {{
      const banner = document.getElementById('banner');
      const attention = (data.health || []).some(row => row.latest_status && row.latest_status !== 'success');
      banner.textContent = attention ? 'A layer step needs attention' : 'Gold is current';
      banner.className = attention ? 'banner warn' : 'banner';
      document.getElementById('health').innerHTML = (data.health || []).map(row =>
        '<li><strong>' + row.layer + '</strong> ' + row.row_count + ' rows, status ' + row.latest_status
        + ', fresh ' + row.fresh_at
        + (Number(row.suspect_rows) ? ', <span class="mark">' + row.suspect_rows + ' suspect</span>' : '')
        + '</li>'
      ).join('');
      const suspects = data.quality || [];
      const qualityBanner = document.getElementById('quality-banner');
      qualityBanner.textContent = suspects.length
        ? 'Data quality: ' + suspects.length + ' suspect row' + (suspects.length === 1 ? '' : 's') + '. Affected metrics are marked.'
        : 'Data quality: no suspect rows.';
      qualityBanner.className = suspects.length ? 'banner warn' : 'banner';
      const qualityTable = document.querySelector('#data-quality table');
      qualityTable.innerHTML = '<tr><th>Batch</th><th>Table</th><th>Key</th><th>Reason</th><th>First seen</th></tr>'
        + (suspects.length ? suspects.map(row =>
          '<tr class="suspect"><td>' + (row.batch_id ?? '') + '</td><td>' + (row.table ?? '') + '</td><td>'
          + (row.key ?? '') + '</td><td>' + (row.reason ?? '') + '</td><td>' + (row.first_seen ?? '') + '</td></tr>'
        ).join('') : '<tr><td colspan="5">No suspect rows.</td></tr>');
      document.getElementById('revenue-total').textContent = 'Mart total ' + money(data.revenue_at_risk_usd);
      document.getElementById('cases').innerHTML = table(
        [['Case', 'case_id'], ['Metric', 'metric'], ['Status', 'status'], ['Severity', 'severity'], ['At risk / hour', 'revenue_at_risk_usd']],
        data.cases || []
      );
      document.getElementById('topics').innerHTML = table(
        [['Topic', 'topic'], ['Tickets', 'ticket_count'], ['Confidence', 'avg_confidence']],
        data.topics || []
      );
      document.getElementById('spend').innerHTML = table(
        [['Date', 'spend_date'], ['Spend', 'spend_usd'], ['Cap', 'cap_usd'], ['Headroom', 'headroom_usd']],
        data.spend || []
      );
      const labels = (data.conversion || []).map(row => String(row.hour).slice(0, 16));
      const rates = (data.conversion || []).map(row => row.conversion_rate);
      const baseline = (data.conversion || []).map(row => row.baseline_rate);
      const colors = (data.conversion || []).map(row =>
        Number(row.dq_suspect_rows) ? '#b45309' : (row.is_anomaly ? '#9a3412' : '#0f5c56')
      );
      if (conversionChart) conversionChart.destroy();
      conversionChart = new Chart(document.getElementById('conversion'), {{
        type: 'line',
        data: {{
          labels,
          datasets: [
            {{ label: 'Conversion', data: rates, borderColor: '#0f5c56', pointBackgroundColor: colors, tension: 0.15 }},
            {{ label: 'Baseline', data: baseline, borderColor: '#9a3412', borderDash: [4, 4], pointRadius: 0 }}
          ]
        }},
        options: {{ animation: false, plugins: {{ legend: {{ position: 'bottom' }} }} }}
      }});
      if (revenueChart) revenueChart.destroy();
      revenueChart = new Chart(document.getElementById('revenue'), {{
        type: 'bar',
        data: {{
          labels: (data.revenue || []).map(row => String(row.hour).slice(0, 16)),
          datasets: [{{
            label: 'USD',
            data: (data.revenue || []).map(row => row.revenue_at_risk_usd),
            backgroundColor: (data.revenue || []).map(row => Number(row.dq_suspect_rows) ? '#b45309' : '#0f5c56')
          }}]
        }},
        options: {{ animation: false, plugins: {{ legend: {{ display: false }} }} }}
      }});
    }}
    function showRuns(rows) {{
      document.getElementById('runs').innerHTML = table(
        [['Layer', 'layer'], ['Status', 'status'], ['In', 'rows_in'], ['Out', 'rows_out'], ['Rejected', 'rows_rejected'], ['Started', 'started_at']],
        rows || []
      );
    }}
    async function refresh() {{
      const data = await fetch('/v1/dashboard/data').then(response => response.json());
      draw(data);
    }}
    document.getElementById('load').addEventListener('click', async () => {{
      if (!confirm('Load the next hours into staging? They move to silver and then gold.')) return;
      const report = await fetch('/v1/ingest', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ batch: 'new', hours: 6 }})
      }}).then(response => response.json());
      const batch = report.batch_id;
      for (let attempt = 0; attempt < 30; attempt += 1) {{
        const rows = await fetch('/v1/dashboard/runs?batch_id=' + encodeURIComponent(batch)).then(response => response.json());
        showRuns(rows);
        const failed = rows.some(row => row.status === 'failed');
        const done = rows.some(row => row.layer === 'gold' && row.status === 'success');
        if (failed || done) break;
        await new Promise(resolve => setTimeout(resolve, 1000));
      }}
      await refresh();
    }});
    let pendingTask = '';
    function showReply(body) {{
      document.getElementById('reply').textContent = body.reply || body.detail || 'No reply';
      const confirmLaunch = document.getElementById('confirm-launch');
      if (body.pending_launch && body.pending_launch.task) {{
        pendingTask = body.pending_launch.task;
        confirmLaunch.hidden = false;
      }} else {{
        pendingTask = '';
        confirmLaunch.hidden = true;
      }}
    }}
    document.getElementById('ask').addEventListener('click', async () => {{
      const persona = document.getElementById('persona').value;
      const message = document.getElementById('message').value;
      const response = await fetch('/v1/dashboard/chat', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ persona, message }})
      }});
      showReply(await response.json());
    }});
    document.getElementById('confirm-launch').addEventListener('click', async () => {{
      if (!pendingTask) return;
      const confirmLaunch = document.getElementById('confirm-launch');
      confirmLaunch.disabled = true;
      const persona = document.getElementById('persona').value;
      const message = document.getElementById('message').value || pendingTask;
      const response = await fetch('/v1/dashboard/chat', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ persona, message, confirm: true, launch_task: pendingTask }})
      }});
      showReply(await response.json());
      confirmLaunch.disabled = false;
    }});
    draw(initial);
    setInterval(refresh, 60000);
  </script>
</body>
</html>
"""


def _esc(value) -> str:
    return (
        str(value if value is not None else "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def register_dashboard(app: FastAPI, warehouse_fn, client_fn) -> None:
    @app.get("/dashboard")
    def dashboard_page():
        from fastapi.responses import HTMLResponse

        return HTMLResponse(_page(collect(warehouse_fn())))

    @app.get("/v1/dashboard/data")
    def dashboard_data():
        return collect(warehouse_fn())

    @app.get("/v1/dashboard/runs")
    def dashboard_runs(batch_id: str | None = None):
        return list_runs(warehouse_fn(), batch_id)

    @app.post("/v1/dashboard/chat")
    def dashboard_chat(body: ChatBody):
        try:
            return answer(
                warehouse_fn(),
                client_fn(),
                body.persona,
                body.message,
                confirm=body.confirm,
                launch_task=body.launch_task,
            )
        except RedactionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except CircuitBreakerOpen as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
