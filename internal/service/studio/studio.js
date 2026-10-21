'use strict';
const $ = id => document.getElementById(id);
let token = '', transaction = null, busy = false;
const client = crypto.randomUUID();
const history = [];
function element(tag, text, className) { const el = document.createElement(tag); if (text !== undefined) el.textContent = text; if (className) el.className = className; return el; }
function controls() {
  for (const id of ['run','explain','refresh','begin','commit','rollback','add-param']) $(id).disabled = busy || !token;
  $('begin').disabled ||= !!transaction; $('commit').disabled ||= !transaction; $('rollback').disabled ||= !transaction;
  $('transaction').textContent = transaction ? `Transaction active · expires ${new Date(transaction.expires_at).toLocaleTimeString()} · pending changes are visible here` : 'Auto-commit · each statement commits independently';
  $('transaction').classList.toggle('active', !!transaction);
}
async function request(action, body = {}, transactional = true) {
  const headers = {'Content-Type':'application/json', Authorization:`Bearer ${token}`, 'X-Sceptre-Client':client};
  if (transactional && transaction) headers['X-Sceptre-Transaction'] = transaction.id;
  const path = transactional && transaction ? `tx/${action}` : action;
  let response, data;
  try { response = await fetch(`/api/${path}`, {method:'POST',headers,body:JSON.stringify(body)}); data = await response.json(); }
  catch { throw new Error('Connection interrupted. The operation may have committed; reconnect and inspect application keys before retrying. Abandoned transactions expire on the service.'); }
  if (response.status !== 401 && response.status !== 403 && Object.hasOwn(data, 'transaction')) transaction = data.transaction;
  controls();
  if (data.error) throw new Error(`${data.error.code}: ${data.error.message}`);
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  return data;
}
async function action(fn) {
  if (busy) return; busy = true; $('notice').textContent = ''; controls();
  try { await fn(); } catch(error) { $('notice').textContent = error.message; }
  finally { busy = false; controls(); }
}
function renderSchema(defs) {
  $('schema').replaceChildren();
  if (!defs.length) { $('schema').append(element('p','No tables yet. Create your first schema in the editor.','hint')); return; }
  for (const def of defs) {
    const details = element('details'); details.open = true; details.append(element('summary',def.name));
    const list = element('ul');
    for (const col of def.columns) { const li = element('li'); li.append(element('span',col.name),element('small',`${col.type}${def.primary_key.includes(col.name) ? ' · PK' : ''}`)); list.append(li); }
    details.append(list);
    for (const index of def.indexes) details.append(element('div',`${index.name} (${index.columns.join(', ')})`,'index'));
    $('schema').append(details);
  }
}
async function refresh() { const data = await request('schema'); renderSchema(data.schema || []); }
function params() {
  return [...$('params').children].map(row => {
    const type = row.querySelector('select').value, value = row.querySelector('input').value;
    if (type === 'int64') { if (!/^-?\d+$/.test(value)) throw new Error('Integer parameters require decimal digits.'); return {type, value}; }
    if (type === 'base64') return {type:'bytes', value};
    const bytes = new TextEncoder().encode(value); let raw = ''; for (const byte of bytes) raw += String.fromCharCode(byte);
    return {type:'bytes',value:btoa(raw)};
  });
}
function addParam() {
  const row = element('div',undefined,'param'); const n = $('params').children.length + 1;
  const type = element('select'); type.setAttribute('aria-label',`Parameter ${n} type`);
  for (const [value,label] of [['int64','Integer'],['text','Text / UTF-8'],['base64','Base64 bytes']]) { const opt = element('option',label); opt.value = value; type.append(opt); }
  const input = element('input'); input.setAttribute('aria-label',`Parameter ${n} value`); input.placeholder = 'Value';
  const remove = element('button','×','quiet'); remove.setAttribute('aria-label','Remove parameter'); remove.addEventListener('click',()=>row.remove());
  row.append(element('span',String(n),'hint'),type,input,remove); $('params').append(row);
}
function bytesDisplay(value) {
  const raw = atob(value), bytes = Uint8Array.from(raw,c=>c.charCodeAt(0));
  try { const text = new TextDecoder('utf-8',{fatal:true}).decode(bytes); if (/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(text)) return `base64:${value}`; return text; }
  catch { return `base64:${value}`; }
}
function renderResult(result) {
  $('output').replaceChildren();
  if (!result.columns.length) { $('output').append(element('p',`${result.rows_affected} row(s) affected`,'empty')); return; }
  if (!result.rows.length) { $('output').append(element('p','No rows returned.','empty')); return; }
  const table = element('table'), head = element('thead'), hr = element('tr');
  for (const name of result.columns) hr.append(element('th',name)); head.append(hr); table.append(head);
  const tbody = element('tbody');
  for (const row of result.rows) { const tr = element('tr'); for (const value of row) { const td = element('td',value.type === 'int64' ? value.value : bytesDisplay(value.value)); td.title = `${value.type}: ${value.value}`; tr.append(td); } tbody.append(tr); }
  table.append(tbody); $('output').append(table);
}
async function execute(explain) {
  const sql = $('sql').value, bindings = params();
  if (!sql.trim()) throw new Error('Enter a SQL statement.');
  const started = performance.now();
  try {
    const data = await request(explain ? 'explain' : 'execute', {sql,params:bindings});
    $('metrics').textContent = `${data.duration_ms.toFixed(2)} ms service · ${(performance.now()-started).toFixed(0)} ms round trip${data.result ? ` · ${data.result.rows.length} row(s)` : ''}`;
    if (explain) $('output').replaceChildren(element('pre',JSON.stringify(data.plan,null,2))); else renderResult(data.result);
    history.unshift({sql,bindings:structuredClone(bindings),ok:true}); history.splice(20);
    // The statement already succeeded; a failed schema refresh must not be reported as a failed statement.
    try { await refresh(); } catch(error) { $('notice').textContent = `Statement succeeded, but the schema refresh failed: ${error.message}`; }
  } catch(error) {
    $('output').replaceChildren(element('pre',error.message,'error')); $('metrics').textContent = 'Execution failed';
    history.unshift({sql,bindings:structuredClone(bindings),ok:false}); history.splice(20); throw error;
  } finally {
    $('history').replaceChildren();
    for (const item of history) { const li = element('li'), button = element('button',`${item.ok ? '✓' : '×'} ${item.sql}`); button.addEventListener('click',()=>{
      $('sql').value = item.sql; $('params').replaceChildren();
      for (const binding of item.bindings) { addParam(); const row = $('params').lastElementChild; row.querySelector('select').value = binding.type === 'int64' ? 'int64' : 'base64'; row.querySelector('input').value = binding.value; }
    }); li.append(button); $('history').append(li); }
  }
}
$('connect-form').addEventListener('submit',event=>{ event.preventDefault(); action(async()=>{
  token = $('token').value.trim(); transaction = null;
  try { await refresh(); } catch(error) { token = ''; throw error; }
  $('token').value = ''; $('login').hidden = true; $('workspace').hidden = false; $('connection').textContent = 'Connected · local'; $('connection').classList.add('live');
}); });
$('run').addEventListener('click',()=>action(()=>execute(false)));
$('explain').addEventListener('click',()=>action(()=>execute(true)));
$('sql').addEventListener('keydown',event=>{ if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') { event.preventDefault(); action(()=>execute(false)); } });
$('refresh').addEventListener('click',()=>action(refresh)); $('add-param').addEventListener('click',addParam);
$('begin').addEventListener('click',()=>action(async()=>{ await request('tx/begin',{},false); await refresh(); }));
$('commit').addEventListener('click',()=>action(async()=>{ await request('commit'); await refresh(); }));
$('rollback').addEventListener('click',()=>action(async()=>{ await request('rollback'); await refresh(); }));
controls();
