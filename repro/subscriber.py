# Minimal subscriber endpoint with a switch: POST /mode/fail or /mode/ok; GET /received lists ids.
import json, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
state = {"mode": "ok", "received": [], "requests": []}
lock = threading.Lock()
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, body=b""):
        self.send_response(code); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        with lock: self._send(200, json.dumps(state).encode())
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0)); body = self.rfile.read(n)
        if self.path.startswith("/mode/"):
            with lock: state["mode"] = self.path.split("/")[-1]
            return self._send(200)
        with lock:
            mode = state["mode"]
            items = json.loads(body) if body else []
            ids = [(i.get("message", i)).get("id") for i in (items if isinstance(items, list) else [items])]
            state["requests"].append({"mode": mode, "ids": ids, "t": time.time(), "len": len(body)})
            if mode == "ok": state["received"].extend(ids)
        self._send(200 if mode == "ok" else 503)
ThreadingHTTPServer(("0.0.0.0", 9000), H).serve_forever()
