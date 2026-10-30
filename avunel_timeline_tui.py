"""Avunel Timeline TUI Client (Fase 0 - F0-12).

Terminal UI client that connects to `avunel serve` SSE stream and renders
a live or historical event timeline.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
import signal
import sys
import time
from typing import Any, Dict, Iterator, Optional, TextIO
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from f0_contract import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    EVENT_TYPE_BLOB_STORED,
    EVENT_TYPE_MESSAGE_APPENDED,
    EVENT_TYPE_SESSION_CREATED,
    EVENT_TYPE_SESSION_REWOUND,
    HEADER_AUTH_PREFIX,
    validate_host,
    validate_port,
)

# ANSI Color escapes (safe fallback when colors disabled)
COLOR_RESET = "\033[0m"
COLOR_BOLD = "\033[1m"
COLOR_DIM = "\033[2m"
COLOR_CYAN = "\033[36m"
COLOR_GREEN = "\033[32m"
COLOR_YELLOW = "\033[33m"
COLOR_MAGENTA = "\033[35m"
COLOR_BLUE = "\033[34m"
COLOR_RED = "\033[31m"
COLOR_BG_DARK = "\033[48;5;236m"


def supports_color(file: TextIO = sys.stdout) -> bool:
    """Check if output stream supports ANSI colors."""
    if os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        return False
    return hasattr(file, "isatty") and file.isatty()


def format_ts(ts_ms: Optional[int]) -> str:
    """Format millisecond timestamp into HH:MM:SS.mmm."""
    if not ts_ms:
        return "--:--:--.---"
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0)
        return dt.strftime("%H:%M:%S.%f")[:-3]
    except Exception:
        return "--:--:--.---"


class TimelineTUIClient:
    """TUI Client for consuming and rendering Avunel SSE event stream."""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        token: Optional[str] = None,
        last_id: int = 0,
        session_id: Optional[str] = None,
        event_type: Optional[str] = None,
        color: Optional[bool] = None,
        verbose: bool = False,
        out: TextIO = sys.stdout,
    ) -> None:
        self.host = validate_host(host)
        self.port = validate_port(port)
        self.token = (token or os.environ.get("AVUNEL_SERVE_TOKEN") or "").strip()
        self.last_id = last_id
        self.session_id = session_id
        self.event_type = event_type
        self.use_color = supports_color(out) if color is None else color
        self.verbose = verbose
        self.out = out
        self._stopped = False

    def stop(self) -> None:
        self._stopped = True

    def _c(self, code: str, text: str) -> str:
        """Helper to conditionally wrap text in ANSI code."""
        if not self.use_color:
            return text
        return f"{code}{text}{COLOR_RESET}"

    def build_stream_url(self) -> str:
        """Construct full SSE URL with query parameters."""
        params: Dict[str, Any] = {}
        if self.token:
            params["token"] = self.token
        if self.last_id > 0:
            params["last_id"] = self.last_id
        if self.session_id:
            params["session_id"] = self.session_id
        if self.event_type:
            params["type"] = self.event_type

        qs = urlencode(params)
        return f"http://{self.host}:{self.port}/api/events/stream{('?' + qs) if qs else ''}"

    def format_event(self, event: Dict[str, Any]) -> str:
        """Format an event dictionary into a clean terminal timeline line."""
        ev_id = event.get("id", 0)
        ts_str = format_ts(event.get("ts"))
        ev_type = event.get("type", "unknown")
        session = event.get("session_id") or "-"
        actor = event.get("actor") or "-"
        payload = event.get("payload") or {}

        # Badges and type color
        if ev_type == EVENT_TYPE_MESSAGE_APPENDED:
            badge = self._c(COLOR_CYAN + COLOR_BOLD, "[MSG]")
            role = payload.get("role", "info")
            role_styled = self._c(COLOR_BLUE, f"<{role}>")
            content = payload.get("content", "")
            if not content:
                blob_ref = payload.get("content_blob_ref")
                if blob_ref:
                    detail = f"{role_styled} [CAS blob {blob_ref[:12]}...]"
                elif payload.get("tool_name"):
                    detail = f"{role_styled} call {payload.get('tool_name')}()"
                else:
                    detail = f"{role_styled} (empty)"
            else:
                first_line = content.strip().splitlines()[0] if content else ""
                if len(first_line) > 70:
                    first_line = first_line[:67] + "..."
                detail = f"{role_styled} {first_line}"
        elif ev_type == EVENT_TYPE_BLOB_STORED:
            badge = self._c(COLOR_GREEN + COLOR_BOLD, "[BLOB]")
            digest = payload.get("digest", "")[:12]
            size = payload.get("byte_size", 0)
            mime = payload.get("mime_type", "app/oct")
            detail = f"{self._c(COLOR_GREEN, digest)} ({size}B, {mime})"
        elif ev_type == EVENT_TYPE_SESSION_CREATED:
            badge = self._c(COLOR_MAGENTA + COLOR_BOLD, "[SESS]")
            title = payload.get("title", session)
            detail = f"Created session '{title}'"
        elif ev_type == "session.title_updated":
            badge = self._c(COLOR_MAGENTA, "[TITLE]")
            title = payload.get("title", "")
            detail = f"Updated title to '{title}'"
        elif ev_type in {EVENT_TYPE_SESSION_REWOUND, "conversation.undone"}:
            badge = self._c(COLOR_YELLOW + COLOR_BOLD, "[UNDO]")
            count = payload.get("count", payload.get("rewound_count", 0))
            target = payload.get("target_message_id", payload.get("target_message_uid", ""))
            detail = f"Undone {count} messages from {target}"
        elif ev_type == "heartbeat":
            badge = self._c(COLOR_DIM, "[PING]")
            detail = f"heartbeat @ {ts_str}"
        else:
            badge = self._c(COLOR_BOLD, f"[{ev_type}]")
            detail = json.dumps(payload, ensure_ascii=False)
            if len(detail) > 60:
                detail = detail[:57] + "..."

        id_str = self._c(COLOR_DIM, f"#{ev_id:<4}")
        time_str = self._c(COLOR_DIM, ts_str)
        sess_str = self._c(COLOR_YELLOW, f"[{session[:12]}]")

        return f"{id_str} {time_str} {badge:<16} {sess_str:<18} {detail}"

    def render_header(self) -> None:
        """Render TUI top header bar."""
        header_text = f"=== Avunel Cockpit Timeline (http://{self.host}:{self.port}) ==="
        info_text = f"Filter: session={self.session_id or 'all'}, type={self.event_type or 'all'}, cursor={self.last_id}"
        if self.use_color:
            self.out.write(f"{COLOR_BOLD}{header_text}{COLOR_RESET}\n")
            self.out.write(f"{COLOR_DIM}{info_text}{COLOR_RESET}\n")
            self.out.write(f"{COLOR_DIM}{'-' * 80}{COLOR_RESET}\n")
        else:
            self.out.write(f"{header_text}\n{info_text}\n{'-' * 80}\n")
        self.out.flush()

    def stream_events(
        self,
        max_events: Optional[int] = None,
        timeout: Optional[float] = None,
        auto_reconnect: bool = True,
    ) -> Iterator[Dict[str, Any]]:
        """Yield parsed event dictionaries from the SSE stream."""
        if not self.token:
            raise ValueError(
                "Auth token is mandatory: provide --token or set AVUNEL_SERVE_TOKEN environment variable."
            )
        events_yielded = 0
        start_time = time.time()
        backoff = 0.5

        while not self._stopped:
            if max_events is not None and events_yielded >= max_events:
                break
            if timeout is not None and (time.time() - start_time) >= timeout:
                break

            url = self.build_stream_url()
            req = Request(url)
            if self.token:
                req.add_header("Authorization", f"{HEADER_AUTH_PREFIX}{self.token}")

            try:
                with urlopen(req, timeout=timeout or 30.0) as resp:
                    backoff = 0.5  # Reset backoff on successful connection
                    while not self._stopped:
                        if max_events is not None and events_yielded >= max_events:
                            return
                        if timeout is not None and (time.time() - start_time) >= timeout:
                            return

                        line_bytes = resp.readline()
                        if not line_bytes:
                            # Stream ended or server closed
                            break

                        line = line_bytes.decode("utf-8").strip()
                        if not line:
                            continue

                        if line.startswith("data: "):
                            raw_data = line[6:].strip()
                            try:
                                ev_data = json.loads(raw_data)
                            except json.JSONDecodeError:
                                continue

                            ev_id = ev_data.get("id")
                            if ev_id and isinstance(ev_id, int):
                                self.last_id = max(self.last_id, ev_id)

                            # Filter heartbeats in generator if not verbose
                            if ev_data.get("type") == "heartbeat" and not self.verbose:
                                continue

                            events_yielded += 1
                            yield ev_data

            except (HTTPError, URLError, ConnectionError, TimeoutError, OSError) as e:
                if not auto_reconnect or self._stopped:
                    break
                # Reconnect after backoff
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 5.0)

    def run(
        self,
        max_events: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> int:
        """Run the TUI event loop printing formatted events."""
        self.render_header()
        count = 0
        try:
            for ev in self.stream_events(max_events=max_events, timeout=timeout):
                rendered = self.format_event(ev)
                self.out.write(rendered + "\n")
                self.out.flush()
                count += 1
        except KeyboardInterrupt:
            self.out.write("\nInterrupted by user. Exiting.\n")
            self.out.flush()
        return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Avunel Timeline TUI - Live SSE Event Stream Viewer"
    )
    parser.add_argument(
        "--host", default=DEFAULT_HOST, help=f"Host (default: {DEFAULT_HOST})"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Auth bearer token (or set AVUNEL_SERVE_TOKEN)",
    )
    parser.add_argument(
        "--session",
        dest="session_id",
        default=None,
        help="Filter events by session ID",
    )
    parser.add_argument(
        "--type",
        dest="event_type",
        default=None,
        help="Filter events by event type",
    )
    parser.add_argument(
        "--last-id",
        type=int,
        default=0,
        help="Resume cursor from event ID (default: 0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Exit after receiving N events (bounded mode)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Stop after N seconds",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include heartbeat and debug events",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colors",
    )

    args = parser.parse_args()

    token = (args.token or os.environ.get("AVUNEL_SERVE_TOKEN") or "").strip()
    if not token:
        print(
            "Error: Auth token is mandatory. Provide --token or set AVUNEL_SERVE_TOKEN.",
            file=sys.stderr,
        )
        sys.exit(1)

    client = TimelineTUIClient(
        host=args.host,
        port=args.port,
        token=token,
        last_id=args.last_id,
        session_id=args.session_id,
        event_type=args.event_type,
        color=False if args.no_color else None,
        verbose=args.verbose,
    )

    def sig_handler(sig, frame):
        client.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    client.run(max_events=args.limit, timeout=args.timeout)


if __name__ == "__main__":
    main()
