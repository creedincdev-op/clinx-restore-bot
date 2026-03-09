import json
import os
import signal
import subprocess
import sys
import threading
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

    bot_proc = subprocess.Popen([sys.executable, "advanced_restore_bot.py"])

    def shutdown_handler(signum, frame) -> None:
        server.shutdown()
        if bot_proc.poll() is None:
            bot_proc.terminate()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    code = bot_proc.wait()
    server.shutdown()
    return code


if __name__ == "__main__":
    raise SystemExit(main())
