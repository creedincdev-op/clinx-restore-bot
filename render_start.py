import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


STATE_FILE = Path(__file__).with_name("data") / "render_runtime_state.json"


class HealthHandler(BaseHTTPRequestHandler):
    def _send_health(self, include_body: bool) -> None:
        if self.path in ("/", "/healthz"):
            body = json.dumps({"ok": True}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if include_body:
                self.wfile.write(body)
            return
        self.send_error(404)

    def do_GET(self) -> None:
        self._send_health(include_body=True)

    def do_HEAD(self) -> None:
        self._send_health(include_body=False)

    def log_message(self, format: str, *args) -> None:
        return


def run_health_server(port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def load_runtime_state(initial_backoff: int) -> dict[str, float | int]:
    if not STATE_FILE.exists():
        return {
            "backoff_seconds": initial_backoff,
            "rapid_failures": 0,
            "next_start_after": 0.0,
        }

    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}

    return {
        "backoff_seconds": int(payload.get("backoff_seconds", initial_backoff)),
        "rapid_failures": int(payload.get("rapid_failures", 0)),
        "next_start_after": float(payload.get("next_start_after", 0.0)),
    }


def save_runtime_state(runtime_state: dict[str, float | int]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(runtime_state, indent=2), encoding="utf-8")


def sleep_with_stop(stop_state: dict[str, object], seconds: int | float) -> None:
    deadline = time.monotonic() + max(0.0, float(seconds))
    while not stop_state["stop"]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(5.0, remaining))


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

    backoff_seconds = int(os.getenv("BOT_RESTART_BACKOFF_INITIAL", "600"))
    max_backoff_seconds = int(os.getenv("BOT_RESTART_BACKOFF_MAX", "7200"))
    rapid_exit_threshold = int(os.getenv("BOT_RAPID_EXIT_SECONDS", "180"))
    startup_jitter_seconds = int(os.getenv("BOT_STARTUP_JITTER_MAX", "30"))
    runtime_state = load_runtime_state(backoff_seconds)
    runtime_state["backoff_seconds"] = min(int(runtime_state["backoff_seconds"]), max_backoff_seconds)

    if startup_jitter_seconds > 0:
        initial_delay = random.randint(0, startup_jitter_seconds)
        print(f"Applying startup jitter of {initial_delay} seconds before first bot launch.")
        sleep_with_stop(state, initial_delay)

    while not state["stop"]:
        now = time.time()
        next_start_after = float(runtime_state.get("next_start_after", 0.0))
        if next_start_after > now:
            wait_for = int(next_start_after - now)
            print(f"Cooling down for {wait_for} seconds before next bot launch.")
            sleep_with_stop(state, wait_for)
            if state["stop"]:
                break

        started_at = time.monotonic()
        proc = subprocess.Popen(
            [sys.executable, "advanced_restore_bot.py"],
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        state["proc"] = proc
        code = proc.wait()
        state["proc"] = None
        runtime_seconds = int(time.monotonic() - started_at)

        if state["stop"]:
            break

        if runtime_seconds >= rapid_exit_threshold:
            runtime_state["rapid_failures"] = 0
            runtime_state["backoff_seconds"] = backoff_seconds
        else:
            runtime_state["rapid_failures"] = int(runtime_state.get("rapid_failures", 0)) + 1
            runtime_state["backoff_seconds"] = min(
                max(backoff_seconds, int(runtime_state["backoff_seconds"]) * 2),
                max_backoff_seconds,
            )

        current_backoff = int(runtime_state["backoff_seconds"])
        jitter = random.randint(0, max(15, current_backoff // 5))
        delay = min(current_backoff + jitter, max_backoff_seconds)
        runtime_state["next_start_after"] = time.time() + delay
        save_runtime_state(runtime_state)
        print(
            f"Bot process exited with code {code}. "
            f"Runtime was {runtime_seconds} seconds. "
            f"Restarting in {delay} seconds..."
        )
        sleep_with_stop(state, delay)

    server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
