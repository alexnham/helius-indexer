// Exercise the real inline live-health and event renderers without a browser or provider calls.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
class Element {
  constructor() { this.children = []; this.value = ''; this.options = [{}]; this._text = ''; this.style = {}; this.dataset = {}; }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(c => c.textContent).join(' '); }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren() { this.children = []; this._text = ''; }
  addEventListener() {}
  setAttribute() {}
}
const nodes = new Map();
const get = id => { if (!nodes.has(id)) nodes.set(id, new Element()); return nodes.get(id); };
const context = vm.createContext({document: {getElementById: get, createElement: () => new Element(),
  createTextNode: value => { const node = new Element(); node.textContent = value; return node; }},
  fetch: () => new Promise(() => {}), setInterval() {}, console,
  window: {addEventListener() {}}, location: {hash: ''}});
const html = fs.readFileSync(require('node:path').join(__dirname, '../scripts/local-console/index.html'), 'utf8');
vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1], context);
const run = (code, value) => { context.data = value; vm.runInContext(code, context); };
const USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

// Available: real reference and rates; lag is labelled as received distance, verified freshness unknown.
run('renderHealth(data)', {configuration: {mint: USDC, sample_percent: 10}, worker: {state: 'running', cycle: 4, resume_from: 990},
  live: {last_received_slot: 1000, unresolved_items: 2, oldest_unresolved_age_seconds: 30, coverage: 'unknown',
    recent: {window_seconds: 60, received: 120, decoded: 118, pending: 2},
    runs: [{state: 'partial', reference_slot: 1010, reference_age_seconds: 5, replay_margin_slots: 400, replay_info_age_seconds: 7}]},
  summary: {sampling: {policy: 'sha256-transfer-id-v1', numerator: 10, denominator: 100}}});
let text = get('health').textContent;
assert.match(text, /USDC/);
assert.match(text, /2\.00 tx\/s \(last 60 s\)/);
assert.match(text, /10 slots · not verified lag/);
assert.match(text, /Verified freshness \/ slot gap unknown \(no verified live boundary\)/);
assert.match(text, /sampled 10\/100 · actual counts, not extrapolated/);
assert.match(text, /oldest 30 s/);

// Missing: an uninitialized runtime shows unknown, never zero lag or a rate.
run('renderHealth(data)', {live: {state: 'uninitialized', runs: [], recent: {}}});
text = get('health').textContent;
assert.doesNotMatch(text, /undefined|NaN|0\.00 tx/);
assert.match(text, /no run recorded yet/);
assert.match(text, /Received-slot distance to reference unknown/);
run('renderHealth(data)', {});
assert.doesNotMatch(get('health').textContent, /undefined|NaN/);

// Stale / skewed: a received slot past the reference and a future reference time are called out.
run('renderHealth(data)', {live: {last_received_slot: 1020, runs: [{reference_slot: 1000, clock_skew: true}]}});
assert.match(get('health').textContent, /ahead by 20/);
assert.match(get('health').textContent, /Clock skew/);

// Events: missing log, empty filter and an error event with expandable details.
run('renderEvents(data)', {available: false, events: [], limit: 100});
assert.match(get('eventNote').textContent, /No event log yet/);
run('eventCategory="errors";renderEvents(data)', {available: true, events: [], limit: 100});
assert.match(get('eventList').textContent, /No errors events/);
run('renderEvents(data)', {available: true, truncated: true, tail_bytes: 262144, unparsed_lines: 1, limit: 100,
  events: [{timestamp_unix_ms: 0, severity: 'error', event: 'connection_failed', component: 'live_ingestion',
    error_class: 'stream_unavailable', categories: ['live', 'errors']}]});
assert.match(get('eventNote').textContent, /1 shown/);
assert.match(get('eventNote').textContent, /256 KiB/);
assert.match(get('eventNote').textContent, /1 non-JSON/);
text = get('eventList').textContent;
assert.match(text, /ERROR/);
assert.match(text, /connection_failed · live_ingestion · stream_unavailable/);
assert.match(text, /"error_class": "stream_unavailable"/);
console.log('Live dashboard checks passed');
