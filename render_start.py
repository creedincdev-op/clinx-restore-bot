import json
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path in ("/", "/healthz"):
            body = json.dumps({"ok": True}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def log_message(self, format: str, *args) -> None:
        return


def run_health_server(port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def main() -> int:
    port = int(os.getenv("PORT", "10000"))
    server = run_health_server(port)

    # Render env values are sometimes pasted with quotes; normalize once.
    token = os.getenv("BOT_TOKEN", "").strip().strip("\"'")
    if token:
        os.environ["BOT_TOKEN"] = token

    state = {"stop": False, "proc": None}

    def shutdown_handler(signum, frame) -> None:
        state["stop"] = True
        proc = state["proc"]
        if proc is not None and proc.poll() is None:
            proc.terminate()
        server.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    backoff_seconds = 15
    max_backoff_seconds = 600

    while not state["stop"]:
        proc = subprocess.Popen([sys.executable, "advanced_restore_bot.py"])
        state["proc"] = proc
        code = proc.wait()
        state["proc"] = None

        if state["stop"]:
            break

        print(
            f"Bot process exited with code {code}. "
            f"Restarting in {backoff_seconds} seconds..."
        )
        time.sleep(backoff_seconds)
        backoff_seconds = min(backoff_seconds * 2, max_backoff_seconds)

    server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
