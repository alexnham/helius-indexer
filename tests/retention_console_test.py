"""The console's retention text matches what the runtime enforces; runs the page script in Node with a stub DOM."""
import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HARNESS = r"""
const els={},texts=[];
function el(){const e={children:[],options:[{}],style:{},value:'',appendChild(c){this.children.push(c);return c},replaceChildren(){this.children=[]},
  addEventListener(){},setAttribute(){},set textContent(v){texts.push(String(v))},get textContent(){return ''}};return e}
globalThis.document={hidden:false,getElementById:id=>els[id]||(els[id]=el()),createElement:el,querySelector:el};
globalThis.setInterval=()=>{};globalThis.window={addEventListener(){}};globalThis.location={hash:''};
const fixture=JSON.parse(process.argv[1]);
globalThis.fetch=async url=>({ok:true,json:async()=>url.startsWith('/api/session')?fixture.session:url.startsWith('/api/status')?fixture.status:{transfers:[]}});
eval(require('fs').readFileSync(0,'utf8'));
setTimeout(()=>console.log(JSON.stringify(texts)),50);
"""


def page_text(config):
    script = re.search(r"<script>(.*)</script>", (ROOT / "scripts/local-console/index.html").read_text(), re.S).group(1)
    status = {"worker": {"state": "running", "pruned": None}, "configuration": config, "summary": {"transfer_count": 2000},
              "live": {}, "historical": {}}
    fixture = {"session": {**config, "mints": [config["mint"]]}, "status": status}
    out = subprocess.run(["node", "-e", HARNESS, json.dumps(fixture)], input=script, capture_output=True, text=True, timeout=30, check=True)
    return " ".join(json.loads(out.stdout))


@unittest.skipUnless(shutil.which("node"), "node is not installed")
class RetentionTextTest(unittest.TestCase):
    config = {"mode": "live and backfill", "mint": "M", "keep": 1000, "slot_start": 1, "slot_end": 10,
              "historical_source": "laserstream_replay"}

    def test_sampled_demo_promises_no_pruning(self):
        text = page_text({**self.config, "sample_percent": 10})
        self.assertIn("never pruned", text)
        for promise in ["Keeps newest", "Older backfill data is deleted", "Retention: newest"]:
            self.assertNotIn(promise, text)

    def test_exact_dataset_states_the_enforced_limit(self):
        text = page_text({**self.config, "sample_percent": None})
        for promise in ["Keeps newest 1,000", "Older backfill data is deleted", "Retention: newest 1000"]:
            self.assertIn(promise, text)
        self.assertNotIn("never pruned", text)


if __name__ == "__main__":
    unittest.main()
