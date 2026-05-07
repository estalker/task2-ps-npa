// Task2 frontend (offline, no external dependencies)
// Loads data from FastAPI endpoints and renders:
// - file manager (ps/npa)
// - pipeline runner with streaming logs
// - result table (HTML)
// - projections (flat "JOIN-like" views for debugging)

const statusEl = document.getElementById('status');
const logEl = document.getElementById('log');
const lightsEl = document.getElementById('lights');
const stageBtnIds = ['btnStagePs', 'btnStageNpa', 'btnStageTable', 'btnStageAll'];

function setStatus(s) { if (statusEl) statusEl.textContent = s; }
function setRunning(r) {
  for (const id of stageBtnIds) {
    const b = document.getElementById(id);
    if (b) b.disabled = r;
  }
  setStatus(r ? 'running' : 'idle');
}

function llmPayload() {
  return {
    ps: { use_llm: !!document.getElementById('psLlmUse').checked, model: document.getElementById('psLlmModel').value || null },
    npa: { use_llm: !!document.getElementById('npaLlmUse').checked, model: document.getElementById('npaLlmModel').value || null },
    table: { use_llm: !!document.getElementById('tableLlmUse').checked, model: document.getElementById('tableLlmModel').value || null },
  };
}

function resetPayload() {
  return {
    ps: { reset_db: !!document.getElementById('psResetDb').checked },
    npa: { reset_db: !!document.getElementById('npaResetDb').checked },
    table: { reset_db: true }, // stage 3 always rebuilds matching layer / result
  };
}

function persistLlmUi() {
  const p = llmPayload();
  try {
    localStorage.setItem('task2_llm_ps', JSON.stringify(p.ps));
    localStorage.setItem('task2_llm_npa', JSON.stringify(p.npa));
    localStorage.setItem('task2_llm_table', JSON.stringify(p.table));
  } catch (_) {}
}

function persistResetUi() {
  const p = resetPayload();
  try {
    localStorage.setItem('task2_reset_ps', JSON.stringify(p.ps));
    localStorage.setItem('task2_reset_npa', JSON.stringify(p.npa));
  } catch (_) {}
}

function restoreLlmUi() {
  try {
    for (const [key, useId, modelId] of [
      ['task2_llm_ps', 'psLlmUse', 'psLlmModel'],
      ['task2_llm_npa', 'npaLlmUse', 'npaLlmModel'],
      ['task2_llm_table', 'tableLlmUse', 'tableLlmModel'],
    ]) {
      const raw = localStorage.getItem(key);
      if (!raw) continue;
      const o = JSON.parse(raw);
      if (o && typeof o === 'object') {
        document.getElementById(useId).checked = !!o.use_llm;
        if (o.model) document.getElementById(modelId).value = o.model;
      }
    }
  } catch (_) {}
}

function restoreResetUi() {
  try {
    for (const [key, id, defaultVal] of [
      ['task2_reset_ps', 'psResetDb', true],
      ['task2_reset_npa', 'npaResetDb', true],
    ]) {
      const el = document.getElementById(id);
      if (!el) continue;
      el.checked = defaultVal;
      const raw = localStorage.getItem(key);
      if (!raw) continue;
      const o = JSON.parse(raw);
      if (o && typeof o === 'object' && o.reset_db !== undefined) {
        el.checked = !!o.reset_db;
      }
    }
  } catch (_) {}
}

async function apiJson(url, opts) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

function renderLights(health) {
  const items = [
    { label: 'UI', data: health.frontend },
    { label: 'Neo4j', data: health.neo4j },
    { label: 'LLM', data: health.ollama },
  ];
  if (!lightsEl) return;
  lightsEl.innerHTML = '';
  for (const it of items) {
    const wrap = document.createElement('span');
    wrap.className = 'lightLabel';
    const dot = document.createElement('span');
    dot.className = 'light ' + (it.data && it.data.ok === true ? 'ok' : (it.data && it.data.ok === false ? 'bad' : 'unk'));
    dot.title = (it.data && it.data.detail) ? it.data.detail : 'unknown';
    const txt = document.createElement('span');
    txt.textContent = it.label;
    wrap.appendChild(dot);
    wrap.appendChild(txt);
    lightsEl.appendChild(wrap);
  }
}

async function refreshHealth() {
  try {
    const h = await apiJson('/api/health');
    renderLights(h);
  } catch (e) {
    renderLights({ frontend: { ok: false, detail: String(e) }, neo4j: null, ollama: null });
  }
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

async function runStage(stage) {
  setRunning(true);
  if (logEl) logEl.textContent = '';
  try {
    persistLlmUi();
    persistResetUi();

    // IMPORTANT: both llmPayload() and resetPayload() contain ps/npa/table objects.
    // Shallow spreading would overwrite these nested objects and drop fields.
    const llm = llmPayload();
    const rst = resetPayload();
    const payload = {
      stage,
      ps: { ...(llm.ps || {}), ...(rst.ps || {}) },
      npa: { ...(llm.npa || {}), ...(rst.npa || {}) },
      table: { ...(llm.table || {}), ...(rst.table || {}) },
    };

    const r = await fetch('/api/run.stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!r.ok) throw new Error(await r.text());

    const reader = r.body.getReader();
    const decoder = new TextDecoder('utf-8');
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value && logEl) {
        logEl.textContent += decoder.decode(value, { stream: true });
        logEl.scrollTop = logEl.scrollHeight;
      }
    }
    await refreshResult();
  } catch (e) {
    if (logEl) logEl.textContent = String(e);
  } finally {
    setRunning(false);
  }
}

async function refreshResult() {
  const r = await fetch('/api/result.html');
  const html = await r.text();
  document.getElementById('tableWrap').innerHTML = html;
}

async function rephraseRow(rowIdx) {
  try {
    setRunning(true);
    const r = await fetch(`/api/rephrase-row?row=${encodeURIComponent(rowIdx)}`, { method: 'POST' });
    const body = await r.text();
    if (!r.ok) throw new Error(body || 'rephrase failed');
    document.getElementById('tableWrap').innerHTML = body;
  } catch (e) {
    if (logEl) logEl.textContent = String(e);
  } finally {
    setRunning(false);
  }
}

function downloadCsv() { window.location.href = '/api/result.csv'; }
function downloadXlsx() { window.location.href = '/api/result.xlsx'; }
function downloadProjectionsXlsx() { window.location.href = '/api/projections.xlsx'; }

function setTab(which) {
  const isResult = which === 'result';
  const isProj = which === 'proj';
  document.getElementById('tabResult').classList.toggle('active', isResult);
  document.getElementById('tabProj').classList.toggle('active', isProj);
  document.getElementById('panelResult').classList.toggle('active', isResult);
  document.getElementById('panelProj').classList.toggle('active', isProj);
  if (isProj) loadProjection().catch(() => {});
}

function _escapeHtml(s) {
  return String(s ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function _renderProjectionTable(header, rows) {
  if (!header || !header.length) return "<div class='muted'>Нет данных</div>";
  let out = "<table><thead><tr>";
  for (const h of header) out += `<th>${_escapeHtml(h)}</th>`;
  out += "</tr></thead><tbody>";
  for (const r of (rows || [])) {
    out += "<tr>";
    for (const c of r) out += `<td class="mono">${_escapeHtml(c)}</td>`;
    out += "</tr>";
  }
  out += "</tbody></table>";
  return out;
}

async function loadProjection() {
  const kind = (document.getElementById('projKind').value || 'match');
  const limit = parseInt(document.getElementById('projLimit').value || '400', 10) || 400;
  const wrap = document.getElementById('projWrap');
  if (wrap) wrap.innerHTML = "<div class='muted'>Загрузка...</div>";
  try {
    const data = await apiJson(`/api/projection?kind=${encodeURIComponent(kind)}&limit=${encodeURIComponent(limit)}`);
    if (!wrap) return;
    wrap.innerHTML = _renderProjectionTable(data.header, data.rows);
  } catch (e) {
    if (!wrap) return;
    wrap.innerHTML = `<div class='muted'>Ошибка загрузки проекции: <span class="mono">${_escapeHtml(String(e))}</span></div>`;
  }
}

// bootstrap
refreshLists().catch(console.error);
refreshResult().catch(() => {});
refreshHealth().catch(() => {});
setInterval(() => refreshHealth().catch(() => {}), 5000);
restoreLlmUi();
restoreResetUi();

