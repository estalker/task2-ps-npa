from __future__ import annotations

import csv
import html
import os
import re
import shutil
import subprocess
from io import StringIO
from pathlib import Path
from typing import Literal

from neo4j import GraphDatabase
from fastapi import Body, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, PlainTextResponse, Response


KIND = Literal["ps", "npa"]

WORKSPACE = Path(os.getenv("WORKSPACE_DIR", "/workspace")).resolve()
INPUT_DIR = (WORKSPACE / "input").resolve()
OUTPUT_DIR = (WORKSPACE / "output").resolve()

PS_DIR = INPUT_DIR / "ps"
NPA_DIR = INPUT_DIR / "npa"

RESULT_MD = OUTPUT_DIR / "list_mandatory_ps.md"


app = FastAPI(title="Task2 Frontend")


INDEX_HTML = """<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Task2 — ПС / НПА</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color: #0f172a; }
      .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
      @media (max-width: 1100px) { .grid { grid-template-columns: 1fr; } }
      .card { border: 1px solid #e2e8f0; border-radius: 12px; padding: 16px; background: #fff; }
      h1 { font-size: 18px; margin: 0 0 12px; }
      h2 { font-size: 14px; margin: 0 0 8px; color: #334155; }
      .row { display:flex; gap: 10px; align-items:center; flex-wrap: wrap; }
      button { border: 1px solid #cbd5e1; background: #0b1220; color: #fff; padding: 8px 12px; border-radius: 10px; cursor: pointer; }
      button.secondary { background: #fff; color: #0b1220; }
      button.danger { background: #b91c1c; border-color: #b91c1c; }
      button:disabled { opacity: .5; cursor: not-allowed; }
      input[type=file] { border: 1px dashed #cbd5e1; padding: 10px; border-radius: 10px; }
      .muted { color: #64748b; font-size: 12px; }
      ul { margin: 10px 0 0; padding-left: 18px; }
      li { margin: 6px 0; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 12px; white-space: pre-wrap; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #e2e8f0; padding: 8px; vertical-align: top; }
      th { background: #f8fafc; text-align: left; position: sticky; top: 0; }
      .tableWrap { max-height: 55vh; overflow: auto; border: 1px solid #e2e8f0; border-radius: 12px; }
      .toolbar { display:flex; gap: 10px; align-items:center; flex-wrap: wrap; }
      .pill { display:inline-block; padding: 2px 8px; border-radius: 999px; background:#f1f5f9; border:1px solid #e2e8f0; font-size: 12px; }
    </style>
  </head>
  <body>
    <div class="row" style="justify-content: space-between;">
      <h1>Task2 — менеджер файлов ПС/НПА + таблица</h1>
      <span class="pill" id="status">idle</span>
    </div>

    <div class="grid">
      <div class="card">
        <h2>1) ПС (docx/rtf)</h2>
        <div class="row">
          <input id="psFiles" type="file" multiple accept=".docx,.rtf" />
          <button class="secondary" onclick="upload('ps')">Загрузить</button>
          <span class="muted">Сохраняется в <span class="mono">input/ps</span></span>
        </div>
        <ul id="psList" class="mono"></ul>
      </div>

      <div class="card">
        <h2>2) НПА (docx/rtf)</h2>
        <div class="row">
          <input id="npaFiles" type="file" multiple accept=".docx,.rtf" />
          <button class="secondary" onclick="upload('npa')">Загрузить</button>
          <span class="muted">Сохраняется в <span class="mono">input/npa</span></span>
        </div>
        <ul id="npaList" class="mono"></ul>
      </div>
    </div>

    <div class="card" style="margin-top: 16px;">
      <div class="toolbar">
        <button id="runBtn" onclick="runPipeline()">Запустить</button>
        <button class="secondary" onclick="refreshResult()">Обновить таблицу</button>
        <button class="secondary" onclick="downloadCsv()">Скачать CSV</button>
      </div>
      <div class="row" style="margin-top: 10px;">
        <label class="muted" style="display:flex; gap:8px; align-items:center;">
          <input type="checkbox" id="useLlm" />
          Использовать LLM
        </label>
        <span class="muted">Модель:</span>
        <select id="llmModel" style="border:1px solid #cbd5e1; padding:8px 10px; border-radius:10px;">
          <option value="qwen2.5:3b-instruct">qwen2.5:3b-instruct (default)</option>
          <option value="qwen2.5:7b-instruct">qwen2.5:7b-instruct</option>
          <option value="qwen2.5:14b-instruct">qwen2.5:14b-instruct</option>
          <option value="llama3.1:8b-instruct">llama3.1:8b-instruct</option>
        </select>
        <span class="muted">Применяется при запуске</span>
      </div>
      <div class="muted" style="margin-top: 8px;">Запуск выполняет: извлечение текста ПС → разбор ОТФ → ingest ПС/НПА → сопоставление → генерация <span class="mono">output/list_mandatory_ps.md</span></div>
      <details style="margin-top: 10px;">
        <summary>Лог последнего запуска</summary>
        <pre class="mono" id="log" style="margin-top: 10px; background:#0b1220; color:#e2e8f0; padding: 12px; border-radius: 12px; overflow:auto; max-height: 30vh;"></pre>
      </details>
    </div>

    <div class="card" style="margin-top: 16px;">
      <h2>Результат (HTML таблица из markdown)</h2>
      <div class="tableWrap" id="tableWrap"></div>
    </div>

    <script>
      const statusEl = document.getElementById('status');
      const logEl = document.getElementById('log');
      const runBtn = document.getElementById('runBtn');
      const useLlmEl = document.getElementById('useLlm');
      const llmModelEl = document.getElementById('llmModel');

      function setStatus(s) { statusEl.textContent = s; }
      function setRunning(r) { runBtn.disabled = r; setStatus(r ? 'running' : 'idle'); }

      async function apiJson(url, opts) {
        const r = await fetch(url, opts);
        if (!r.ok) throw new Error(await r.text());
        return await r.json();
      }

      async function refreshLists() {
        const ps = await apiJson('/api/files?kind=ps');
        const npa = await apiJson('/api/files?kind=npa');
        renderList('psList', 'ps', ps.files);
        renderList('npaList', 'npa', npa.files);
      }

      function renderList(elId, kind, files) {
        const el = document.getElementById(elId);
        el.innerHTML = '';
        if (!files.length) {
          const li = document.createElement('li');
          li.textContent = '(пусто)';
          el.appendChild(li);
          return;
        }
        for (const f of files) {
          const li = document.createElement('li');
          const name = document.createElement('span');
          name.textContent = f;
          const del = document.createElement('button');
          del.textContent = 'Удалить';
          del.className = 'danger';
          del.style.marginLeft = '10px';
          del.onclick = async () => {
            await fetch(`/api/files/${kind}/${encodeURIComponent(f)}`, { method: 'DELETE' });
            await refreshLists();
          };
          li.appendChild(name);
          li.appendChild(del);
          el.appendChild(li);
        }
      }

      async function upload(kind) {
        const input = document.getElementById(kind === 'ps' ? 'psFiles' : 'npaFiles');
        if (!input.files.length) return;
        const fd = new FormData();
        for (const f of input.files) fd.append('files', f);
        await fetch(`/api/upload?kind=${kind}`, { method: 'POST', body: fd });
        input.value = '';
        await refreshLists();
      }

      async function runPipeline() {
        setRunning(true);
        logEl.textContent = '';
        try {
          const payload = {
            use_llm: !!useLlmEl.checked,
            llm_model: llmModelEl.value || null,
          };
          localStorage.setItem('task2_use_llm', payload.use_llm ? '1' : '0');
          localStorage.setItem('task2_llm_model', payload.llm_model || '');

          const r = await fetch('/api/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await r.json();
          logEl.textContent = data.log || '';
          await refreshResult();
        } catch (e) {
          logEl.textContent = String(e);
        } finally {
          setRunning(false);
        }
      }

      async function refreshResult() {
        const r = await fetch('/api/result.html');
        const html = await r.text();
        document.getElementById('tableWrap').innerHTML = html;
      }

      function downloadCsv() {
        window.location.href = '/api/result.csv';
      }

      refreshLists().catch(console.error);
      refreshResult().catch(() => {});

      // Restore LLM UI state
      try {
        useLlmEl.checked = (localStorage.getItem('task2_use_llm') || '0') === '1';
        const m = localStorage.getItem('task2_llm_model') || '';
        if (m) llmModelEl.value = m;
      } catch (_) {}
    </script>
  </body>
</html>
"""


def _safe_filename(name: str) -> str:
    name = name.replace("\\", "/").split("/")[-1]
    # keep basic set; prevent sneaky paths
    name = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._() \\-]+", "_", name)
    return name.strip() or "file"


def _kind_dir(kind: KIND) -> Path:
    if kind == "ps":
        return PS_DIR
    return NPA_DIR


def _ensure_dirs() -> None:
    PS_DIR.mkdir(parents=True, exist_ok=True)
    NPA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _strip_html(s: str) -> str:
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</?small>", "", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    return " ".join(s.split()).strip()


def _parse_markdown_table(md_text: str) -> tuple[list[str], list[list[str]]]:
    lines = [ln.strip("\n") for ln in (md_text or "").splitlines()]
    table_lines = [ln for ln in lines if ln.strip().startswith("|")]
    if len(table_lines) < 2:
        return [], []
    header = [c.strip() for c in table_lines[0].strip("|").split("|")]
    rows: list[list[str]] = []
    for ln in table_lines[2:]:
        if not ln.strip().startswith("|"):
            continue
        cols = [c.strip() for c in ln.strip("|").split("|")]
        # pad/truncate to header length
        if len(cols) < len(header):
            cols += [""] * (len(header) - len(cols))
        rows.append(cols[: len(header)])
    return header, rows


def _rows_to_html_table(header: list[str], rows: list[list[str]]) -> str:
    if not header:
        return "<div class='muted'>Нет таблицы в output/list_mandatory_ps.md</div>"
    out = ["<table>"]
    out.append("<thead><tr>")
    for h in header:
        out.append(f"<th>{html.escape(h)}</th>")
    out.append("</tr></thead>")
    out.append("<tbody>")
    for r in rows:
        out.append("<tr>")
        for c in r:
            # markdown output already uses <br>, <small>; keep as-is (trusted local artifact)
            out.append(f"<td>{c}</td>")
        out.append("</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def _rows_to_csv(header: list[str], rows: list[list[str]]) -> str:
    buf = StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(header)
    for r in rows:
        w.writerow([_strip_html(c) for c in r])
    return buf.getvalue()


def _run_cmd(args: list[str], cwd: Path) -> tuple[int, str]:
    p = subprocess.run(
        args,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=os.environ.copy(),
    )
    return p.returncode, p.stdout


def _reset_neo4j() -> None:
    uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687").strip() or "bolt://neo4j:7687"
    user = os.getenv("NEO4J_USER", "neo4j").strip() or "neo4j"
    password = os.getenv("NEO4J_PASSWORD", "neo4j_password").strip() or "neo4j_password"
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as s:
            # Keep constraints; just delete data.
            res = s.run("MATCH (n) DETACH DELETE n")
            res.consume()
    finally:
        driver.close()


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


@app.get("/api/files")
def list_files(kind: KIND = Query(...)) -> dict:
    _ensure_dirs()
    d = _kind_dir(kind)
    files = sorted([p.name for p in d.iterdir() if p.is_file()])
    return {"kind": kind, "files": files}


@app.post("/api/upload")
async def upload_files(kind: KIND = Query(...), files: list[UploadFile] = File(...)) -> dict:
    _ensure_dirs()
    d = _kind_dir(kind)
    saved: list[str] = []
    for f in files:
        name = _safe_filename(f.filename or "file")
        if not name.lower().endswith((".docx", ".rtf")):
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {name}")
        dst = d / name
        with dst.open("wb") as w:
            shutil.copyfileobj(f.file, w)
        saved.append(name)
    return {"saved": saved}


@app.delete("/api/files/{kind}/{name}")
def delete_file(kind: KIND, name: str) -> dict:
    _ensure_dirs()
    d = _kind_dir(kind)
    safe = _safe_filename(name)
    p = d / safe
    if p.exists() and p.is_file():
        p.unlink()
    return {"deleted": safe}


@app.post("/api/run")
def run_pipeline(payload: dict = Body(default_factory=dict)) -> dict:
    """
    Best-effort pipeline (no interactive prompts):
    - Ingest ALL PS (.docx) into Neo4j
    - Ingest ALL NPA (.rtf) into Neo4j
    - Build matching graph for ALL PS
    - Export mandatory PS table (multi-row)
    """
    _ensure_dirs()
    log_parts: list[str] = []

    # Always start from a clean graph to keep results deterministic.
    try:
        _reset_neo4j()
        log_parts.append("[NEO4J] cleared graph (MATCH (n) DETACH DELETE n)\n")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Neo4j reset failed: {e}")

    use_llm = bool(payload.get("use_llm", False))
    llm_model = payload.get("llm_model", None)
    if llm_model is not None and not isinstance(llm_model, str):
        llm_model = None
    llm_model = (llm_model or "").strip() or None

    if use_llm and llm_model:
        os.environ["OPENAI_MODEL"] = llm_model
    log_parts.append(f"[LLM] use_llm={use_llm} model={llm_model or '(env/default)'}\n")

    ps_files = [p for p in PS_DIR.iterdir() if p.is_file()]
    if not ps_files:
        raise HTTPException(status_code=400, detail="Нет файлов ПС в input/ps (нужно загрузить .docx)")

    # Ingest PS docx (ingester currently supports docx)
    if any(p.suffix.lower() == ".docx" for p in ps_files):
        cmd = ["python", "-m", "app.ingest", "--input", "input/ps", "--doc-source", "profstandard"]
        if not use_llm:
            cmd.append("--no-llm")
        else:
            # Safety defaults for interactive UI runs on weaker machines.
            # On a stronger Linux box you can raise these.
            cmd += ["--llm-max-chunks", "4"]
        rc, out = _run_cmd(cmd, cwd=WORKSPACE)
        log_parts.append(out)
        if rc != 0:
            raise HTTPException(status_code=500, detail="PS ingest failed")
    else:
        raise HTTPException(status_code=400, detail="В input/ps нет .docx (текущий ingest ПС работает с .docx)")

    # Ingest NPA rtf (ingester currently supports rtf)
    npa_files = [p for p in NPA_DIR.iterdir() if p.is_file()]
    if any(p.suffix.lower() == ".rtf" for p in npa_files):
        cmd = ["python", "-m", "app.npa_ingest", "--input", "input/npa"]
        if use_llm:
            cmd += ["--use-llm", "--llm-max-calls", "6"]
        rc, out = _run_cmd(cmd, cwd=WORKSPACE)
        log_parts.append(out)
        if rc != 0:
            raise HTTPException(status_code=500, detail="NPA ingest failed")
    else:
        log_parts.append("[NPA] No .rtf in input/npa; skipping app.npa_ingest.\n")

    # Build matching graph (ALL PS)
    rc, out = _run_cmd(["python", "scripts/build_matching_graph.py"], cwd=WORKSPACE)
    log_parts.append(out)
    if rc != 0:
        raise HTTPException(status_code=500, detail="Matching graph build failed")

    # Export markdown table (multi-row)
    rc, out = _run_cmd(["python", "scripts/export_mandatory_ps_table.py"], cwd=WORKSPACE)
    log_parts.append(out)
    if rc != 0:
        raise HTTPException(status_code=500, detail="Export table failed")

    return {"ok": True, "log": "\n".join(log_parts)}


@app.get("/api/result.md", response_class=PlainTextResponse)
def result_md() -> str:
    if not RESULT_MD.exists():
        return ""
    return RESULT_MD.read_text(encoding="utf-8", errors="replace")


@app.get("/api/result.html", response_class=HTMLResponse)
def result_html() -> str:
    if not RESULT_MD.exists():
        return "<div class='muted'>Файл output/list_mandatory_ps.md пока не создан.</div>"
    md = RESULT_MD.read_text(encoding="utf-8", errors="replace")
    header, rows = _parse_markdown_table(md)
    return _rows_to_html_table(header, rows)


@app.get("/api/result.csv")
def result_csv() -> Response:
    if not RESULT_MD.exists():
        raise HTTPException(status_code=404, detail="output/list_mandatory_ps.md not found")
    md = RESULT_MD.read_text(encoding="utf-8", errors="replace")
    header, rows = _parse_markdown_table(md)
    body = _rows_to_csv(header, rows)
    return Response(
        content=body,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=list_mandatory_ps.csv"},
    )

