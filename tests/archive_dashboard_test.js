// Exercise the real inline archive renderer without a browser or provider calls.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
class Element {
  constructor() { this.children = []; this.value = ''; this.options = [{}]; this._text = ''; }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(c => c.textContent).join(' '); }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren() { this.children = []; this._text = ''; }
  addEventListener() {}
}
const nodes = new Map();
const get = id => { if (!nodes.has(id)) nodes.set(id, new Element()); return nodes.get(id); };
const context = vm.createContext({document: {getElementById: get, createElement: () => new Element()},
  fetch: () => new Promise(() => {}), setInterval() {}, console,
  window: {addEventListener() {}}, location: {hash: ''}});
const html = fs.readFileSync(require('node:path').join(__dirname, '../scripts/local-console/index.html'), 'utf8');
vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1], context);
const render = value => { context.data = value; vm.runInContext('renderArchive(data)', context); };
render({worker: {state: 'budget_exhausted'}, configuration: {mint: 'USDC', slot_start: 1, slot_end: 100},
  bridge: {archive_tip: 90, historical: {pending_slots: 80, contiguous_through: 20, complete: false}},
  summary: {transfer_count: 4, unknown_amount_count: 1, sampling: {policy: 'sha256-transfer-id-v1', numerator: 10, denominator: 100}},
  backfill: {window: {cutoff_unix_seconds: 100, measured_at_unix_seconds: 200},
    budget: {reserved_bytes: 738197504, max_reserved_bytes: 2147483648, actual_response_bytes: 12, unknown_actual_batches: 1}}});
assert.match(get('stages').textContent, /80/);
assert.match(get('stages').textContent, /Charged bytes: 738197504/);
assert.match(get('stages').textContent, /Batches with unknown actual bytes: 1/);
assert.match(get('meta').textContent, /sample/i);
assert.match(get('notice').textContent, /incomplete/i);
assert.match(get('flowNote').textContent, /unknown|unverified/i);
render({});
assert.doesNotMatch(get('stages').textContent, /undefined|NaN/);
assert.match(get('notice').textContent, /unknown/i);
console.log('Archive dashboard checks passed');
