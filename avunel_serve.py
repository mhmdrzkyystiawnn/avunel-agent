"""F0-11: Avunel Serve — SQLite Polling Event Stream & Cockpit Server.

Provides:
- Loopback-only HTTP & SSE server (127.0.0.1:31032 by default).
- Strict Token authentication (Bearer header or ?token= query param).
- Strict Origin validation against loopback origins.
- Resumable SSE streaming with cursor (id > last_id).
- Heartbeat comments (: heartbeat <ts>) every 15 seconds during idle.
- Embedded static Cockpit web UI (zero external assets or npm build required).
- REST endpoints: /api/health, /api/events, /api/sessions.
"""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import signal
import socket
import sys
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from f0_contract import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    HEARTBEAT_INTERVAL_SECONDS,
    POLL_INTERVAL_SECONDS,
    MAX_SSE_BATCH_SIZE,
    event_has_blob,
    format_sse_event,
    format_sse_heartbeat,
    is_event_redacted,
    is_origin_allowed,
    validate_host,
    validate_port,
    verify_token,
)
from hermes_state_events import EventStore, open_events

logger = logging.getLogger("avunel.serve")


COCKPIT_HTML = """<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Avunel Cockpit v0 — Event Timeline</title>
  <style>
    :root {
      --bg: #0d1117;
      --card-bg: #161b22;
      --border: #30363d;
      --text: #e6edf3;
      --text-muted: #8b949e;
      --accent: #58a6ff;
      --success: #3fb950;
      --warning: #d29922;
      --danger: #f85149;
      --badge-bg: #21262d;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      line-height: 1.5;
      display: flex;
      flex-direction: column;
      height: 100vh;
      overflow: hidden;
    }
    header {
      background: var(--card-bg);
      border-bottom: 1px solid var(--border);
      padding: 12px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-shrink: 0;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .brand h1 { font-size: 18px; font-weight: 600; }
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 12px;
      font-size: 12px;
      font-weight: 500;
      background: var(--badge-bg);
      border: 1px solid var(--border);
    }
    .status-badge {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--warning);
    }
    .status-dot.connected { background: var(--success); }
    .status-dot.disconnected { background: var(--danger); }
    .toolbar {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 10px 24px;
      background: var(--card-bg);
      border-bottom: 1px solid var(--border);
      font-size: 13px;
    }
    select, input {
      background: var(--bg);
      color: var(--text);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 6px 10px;
      font-size: 13px;
    }
    main {
      flex: 1;
      display: flex;
      overflow: hidden;
    }
    .timeline-container {
      flex: 1;
      overflow-y: auto;
      padding: 20px 24px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .event-card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px 16px;
      cursor: pointer;
      transition: border-color 0.15s ease;
    }
    .event-card:hover { border-color: var(--accent); }
    .event-card.selected { border-color: var(--accent); background: #1c2128; }
    .event-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 6px;
      font-size: 12px;
      color: var(--text-muted);
    }
    .event-title {
      font-size: 14px;
      font-weight: 600;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .type-badge {
      color: var(--accent);
      background: rgba(88, 166, 255, 0.1);
      border: 1px solid rgba(88, 166, 255, 0.3);
      padding: 2px 6px;
      border-radius: 4px;
      font-family: monospace;
      font-size: 12px;
    }
    .redacted-badge {
      color: var(--warning);
      background: rgba(210, 153, 34, 0.1);
      border: 1px solid rgba(210, 153, 34, 0.3);
      padding: 1px 5px;
      border-radius: 4px;
      font-size: 11px;
    }
    .blob-badge {
      color: var(--success);
      background: rgba(63, 185, 80, 0.1);
      border: 1px solid rgba(63, 185, 80, 0.3);
      padding: 1px 5px;
      border-radius: 4px;
      font-size: 11px;
    }
    .event-body {
      font-size: 13px;
      color: var(--text);
      white-space: pre-wrap;
      word-break: break-word;
      max-height: 80px;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .drawer {
      width: 450px;
      background: var(--card-bg);
      border-left: 1px solid var(--border);
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    .drawer-header {
      padding: 14px 18px;
      border-bottom: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .drawer-content {
      padding: 16px 18px;
      overflow-y: auto;
      flex: 1;
      font-family: monospace;
      font-size: 12px;
      white-space: pre-wrap;
      word-break: break-all;
    }
    .empty-state {
      text-align: center;
      padding: 60px 20px;
      color: var(--text-muted);
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <h1>Avunel Cockpit</h1>
      <span class="badge">Fase 0 Vertical Slice</span>
    </div>
    <div class="brand">
      <div class="badge status-badge">
        <span class="status-dot" id="statusDot"></span>
        <span id="statusText">Menghubungkan...</span>
      </div>
      <span class="badge" id="eventCountBadge">0 Event</span>
    </div>
  </header>
  <div class="toolbar">
    <label for="sessionSelect">Filter Sesi:</label>
    <select id="sessionSelect">
      <option value="">Semua Sesi</option>
    </select>
    <label for="typeSelect">Filter Tipe:</label>
    <select id="typeSelect">
      <option value="">Semua Tipe</option>
      <option value="session.created">session.created</option>
      <option value="session.updated">session.updated</option>
      <option value="message.appended">message.appended</option>
      <option value="conversation.undone">conversation.undone</option>
    </select>
  </div>
  <main>
    <div class="timeline-container" id="timelineList">
      <div class="empty-state">Menunggu event dari server SSE...</div>
    </div>
    <div class="drawer" id="detailDrawer" style="display: none;">
      <div class="drawer-header">
        <h3 id="drawerTitle">Detail Event</h3>
        <button id="closeDrawer" style="background: none; border: none; color: var(--text-muted); cursor: pointer; font-size: 16px;">✕</button>
      </div>
      <div class="drawer-content" id="drawerBody"></div>
    </div>
  </main>

  <script>
    let events = [];
    let lastId = 0;
    let selectedEventId = null;
    let eventSource = null;
    let reconnectTimer = null;

    const urlParams = new URLSearchParams(window.location.search);
    const token = urlParams.get('token') || '';

    function handleIncomingEvent(e) {
      try {
        const ev = JSON.parse(e.data);
        if (ev.id <= lastId) {
          // Deduplication guard: ignore already received events
          return;
        }
        lastId = ev.id;
        events.push(ev);
        updateSessionOptions(ev.session_id);
        renderTimeline();
      } catch (err) {
        console.error('Failed to parse SSE event:', err);
      }
    }

    function connectSSE() {
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }

      const statusDot = document.getElementById('statusDot');
      const statusText = document.getElementById('statusText');

      let streamUrl = '/api/events/stream?last_id=' + lastId;
      if (token) streamUrl += '&token=' + encodeURIComponent(token);

      statusDot.className = 'status-dot';
      statusText.textContent = 'Menghubungkan...';

      if (eventSource) {
        eventSource.close();
        eventSource = null;
      }

      eventSource = new EventSource(streamUrl);

      eventSource.onopen = () => {
        statusDot.className = 'status-dot connected';
        statusText.textContent = 'Live SSE Terhubung';
      };

      eventSource.onerror = () => {
        statusDot.className = 'status-dot disconnected';
        statusText.textContent = 'Terputus (Reconnect...)';
        if (eventSource) {
          eventSource.close();
          eventSource = null;
        }
        // Explicit reconnect with latest lastId to prevent duplicate replay
        if (!reconnectTimer) {
          reconnectTimer = setTimeout(connectSSE, 2000);
        }
      };

      // Standard message handler
      eventSource.onmessage = handleIncomingEvent;

      // Named event listeners per F0 Contract & W3C SSE standard
      const eventTypes = [
        'session.created',
        'session.updated',
        'session.rewound',
        'session.title_updated',
        'message.appended',
        'conversation.undone',
        'blob.stored',
        'tool.called',
        'tool.completed',
        'message'
      ];
      eventTypes.forEach(t => {
        eventSource.addEventListener(t, handleIncomingEvent);
      });
    }

    function updateSessionOptions(sid) {
      if (!sid) return;
      const select = document.getElementById('sessionSelect');
      for (let i = 0; i < select.options.length; i++) {
        if (select.options[i].value === sid) return;
      }
      const opt = document.createElement('option');
      opt.value = sid;
      opt.textContent = sid;
      select.appendChild(opt);
    }

    function renderTimeline() {
      const container = document.getElementById('timelineList');
      const sessionFilter = document.getElementById('sessionSelect').value;
      const typeFilter = document.getElementById('typeSelect').value;

      const filtered = events.filter(e => {
        if (sessionFilter && e.session_id !== sessionFilter) return false;
        if (typeFilter && e.type !== typeFilter) return false;
        return true;
      });

      document.getElementById('eventCountBadge').textContent = filtered.length + ' / ' + events.length + ' Event';

      if (filtered.length === 0) {
        container.innerHTML = '<div class="empty-state">Tidak ada event yang cocok dengan filter.</div>';
        return;
      }

      container.innerHTML = '';
      filtered.forEach(ev => {
        let payload = {};
        try { payload = typeof ev.payload_json === 'string' ? JSON.parse(ev.payload_json) : (ev.payload_json || {}); } catch(err) {}

        const card = document.createElement('div');
        card.className = 'event-card' + (ev.id === selectedEventId ? ' selected' : '');
        card.onclick = () => selectEvent(ev);

        let previewText = '';
        if (payload.content) previewText = payload.content;
        else if (payload.title) previewText = 'Title updated to: ' + payload.title;
        else previewText = JSON.stringify(payload);

        let badges = `<span class="type-badge">${ev.type}</span>`;
        if (ev.is_redacted || JSON.stringify(payload).includes('...') || JSON.stringify(payload).includes('[REDACTED:')) {
          badges += ` <span class="redacted-badge">REDACTED</span>`;
        }
        if (ev.has_blob || payload.content_blob_ref || payload.tool_calls_blob_ref || (ev.api_content_ref && ev.api_content_ref.startsWith('sha256:'))) {
          badges += ` <span class="blob-badge">CAS BLOB</span>`;
        }

        const dateStr = ev.created_at || (ev.ts ? new Date(ev.ts).toLocaleTimeString() : '');

        card.innerHTML = `
          <div class="event-header">
            <span>#${ev.id} • ${ev.actor || 'system'} • ${ev.session_id || 'no-session'}</span>
            <span>${dateStr}</span>
          </div>
          <div class="event-title">
            ${badges}
          </div>
          <div class="event-body">${escapeHtml(previewText)}</div>
        `;
        container.appendChild(card);
      });
    }

    function selectEvent(ev) {
      selectedEventId = ev.id;
      document.getElementById('detailDrawer').style.display = 'flex';
      document.getElementById('drawerTitle').textContent = `Event #${ev.id} (${ev.type})`;
      document.getElementById('drawerBody').textContent = JSON.stringify(ev, null, 2);
      renderTimeline();
    }

    document.getElementById('closeDrawer').onclick = () => {
      document.getElementById('detailDrawer').style.display = 'none';
      selectedEventId = null;
      renderTimeline();
    };

    document.getElementById('sessionSelect').onchange = renderTimeline;
    document.getElementById('typeSelect').onchange = renderTimeline;

    function escapeHtml(str) {
      if (!str) return '';
      return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    connectSSE();
  </script>
</body>
</html>
"""


class AvunelServeHandler(BaseHTTPRequestHandler):
    avunel_server: AvunelEventServer

    # Quiet standard log messages in tests unless logging is at DEBUG
    def log_message(self, format: str, *args: Any) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            super().log_message(format, *args)

    def _send_json_error(self, status_code: int, message: str) -> None:
        """Send a sanitized JSON error response."""
        resp = json.dumps({"error": message}).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(resp)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(resp)

    def _check_security(self, query_params: Dict[str, List[str]]) -> bool:
        """Enforce Token Auth and Origin validation."""
        # 1. Origin header check (if present)
        origin = self.headers.get("Origin")
        if not is_origin_allowed(origin, port=self.avunel_server.port):
            self._send_json_error(403, "Forbidden: untrusted origin")
            return False

        # 2. Token auth check
        expected_token = self.avunel_server.token
        if expected_token:
            provided_token = None
            auth_header = self.headers.get("Authorization")
            if auth_header and auth_header.startswith("Bearer "):
                provided_token = auth_header[7:].strip()
            elif "token" in query_params and query_params["token"]:
                provided_token = query_params["token"][0]

            if not verify_token(provided_token, expected_token):
                self._send_json_error(401, "Unauthorized: missing or invalid bearer token")
                return False

        return True

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # 1. Static Cockpit UI (root and /cockpit)
        if path in {"/", "/cockpit"}:
            body = COCKPIT_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # Security check for all API and SSE routes
        if not self._check_security(query):
            return

        # 2. Health check endpoint
        if path in {"/api/health", "/health"}:
            total_events = self.avunel_server.event_store.count()
            data = {
                "status": "ok",
                "host": self.avunel_server.host,
                "port": self.avunel_server.port,
                "total_events": total_events,
            }
            body = json.dumps(data).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # 3. SSE Stream endpoint: /api/events/stream or /events
        if path in {"/api/events/stream", "/events"}:
            self._handle_sse_stream(query)
            return

        # 4. JSON Events list endpoint: /api/events
        if path == "/api/events":
            self._handle_json_events(query)
            return

        # 5. Sessions list endpoint: /api/sessions
        if path == "/api/sessions":
            self._handle_sessions()
            return

        self._send_json_error(404, "Endpoint not found")

    def _handle_sse_stream(self, query: Dict[str, List[str]]) -> None:
        """Handle persistent W3C Server-Sent Events stream."""
        # Resolve cursor: prioritize Last-Event-ID header if present, then query param
        last_id = 0
        if "Last-Event-ID" in self.headers and self.headers["Last-Event-ID"]:
            try:
                last_id = int(self.headers["Last-Event-ID"])
            except ValueError:
                pass
        elif "last_id" in query and query["last_id"]:
            try:
                last_id = int(query["last_id"][0])
            except ValueError:
                self._send_json_error(400, "Invalid last_id cursor")
                return

        session_id_filter = query.get("session_id", [None])[0]
        type_filter = query.get("type", [None])[0]

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", f"http://127.0.0.1:{self.avunel_server.port}")
        self.end_headers()

        last_heartbeat = time.time()

        try:
            while not self.avunel_server.is_stopped():
                # Query next batch of events after last_id
                events = self.avunel_server.event_store.events_after(last_id, limit=MAX_SSE_BATCH_SIZE)

                if events:
                    for ev in events:
                        # Apply filters if set
                        if session_id_filter and ev.get("session_id") != session_id_filter:
                            last_id = ev["id"]
                            continue
                        if type_filter and ev.get("type") != type_filter:
                            last_id = ev["id"]
                            continue

                        last_id = ev["id"]
                        sse_msg = format_sse_event(ev)
                        self.wfile.write(sse_msg.encode("utf-8"))

                    self.wfile.flush()
                    last_heartbeat = time.time()
                else:
                    # Idle check: emit heartbeat if interval elapsed
                    now = time.time()
                    if now - last_heartbeat >= HEARTBEAT_INTERVAL_SECONDS:
                        hb_msg = format_sse_heartbeat(now)
                        self.wfile.write(hb_msg.encode("utf-8"))
                        self.wfile.flush()
                        last_heartbeat = now

                    time.sleep(POLL_INTERVAL_SECONDS)

        except (BrokenPipeError, ConnectionResetError, socket.error):
            # Client disconnected cleanly
            pass

    def _handle_json_events(self, query: Dict[str, List[str]]) -> None:
        last_id = 0
        if "last_id" in query and query["last_id"]:
            try:
                last_id = int(query["last_id"][0])
            except ValueError:
                self._send_json_error(400, "Invalid last_id cursor")
                return

        limit = MAX_SSE_BATCH_SIZE
        if "limit" in query and query["limit"]:
            try:
                limit = min(int(query["limit"][0]), 1000)
            except ValueError:
                pass

        events = self.avunel_server.event_store.events_after(last_id, limit=limit)
        for ev in events:
            ev["is_redacted"] = is_event_redacted(ev)
            ev["has_blob"] = event_has_blob(ev)
        max_id = max([e["id"] for e in events], default=last_id)
        data = {
            "events": events,
            "count": len(events),
            "last_id": max_id,
        }
        body = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_sessions(self) -> None:
        events = list(self.avunel_server.event_store.all_events())
        sessions = sorted(list({e["session_id"] for e in events if e.get("session_id")}))
        body = json.dumps({"sessions": sessions, "count": len(sessions)}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class AvunelEventServer:
    """Standalone HTTP + SSE Server for Avunel Event Log & Cockpit."""

    def __init__(
        self,
        event_store: EventStore,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        token: Optional[str] = None,
    ) -> None:
        self.event_store = event_store
        self.host = validate_host(host)
        self.port = validate_port(port)
        resolved_token = (token or os.environ.get("AVUNEL_SERVE_TOKEN") or "").strip()
        if not resolved_token:
            raise ValueError(
                "Auth token is mandatory: provide --token or set AVUNEL_SERVE_TOKEN environment variable."
            )
        self.token = resolved_token
        self._stopped = threading.Event()
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    def start(self, background: bool = False) -> None:
        """Start the server. If background=True, run in a daemon thread."""
        logger.info("Starting Avunel serve on http://%s:%d (token: %s)", self.host, self.port, "set" if self.token else "none")

        class BoundHandler(AvunelServeHandler):
            pass

        BoundHandler.avunel_server = self

        self._httpd = ThreadingHTTPServer((self.host, self.port), BoundHandler)

        if background:
            self._thread = threading.Thread(target=self._run_server, daemon=True)
            self._thread.start()
            # Brief yield to let socket bind
            time.sleep(0.05)
        else:
            self._run_server()

    def _run_server(self) -> None:
        if not self._httpd:
            return
        try:
            self._httpd.serve_forever(poll_interval=0.2)
        finally:
            self._stopped.set()

    def shutdown(self) -> None:
        """Gracefully shut down the server."""
        self._stopped.set()
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            self._thread = None
        logger.info("Avunel serve shutdown complete.")


def run_serve_command(
    events_db_path: Optional[Path] = None,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    token: Optional[str] = None,
) -> None:
    """CLI runner for avunel serve."""
    resolved_token = (token or os.environ.get("AVUNEL_SERVE_TOKEN") or "").strip()
    if not resolved_token:
        print(
            "Error: Auth token is mandatory. Provide --token or set AVUNEL_SERVE_TOKEN environment variable.",
            file=sys.stderr,
        )
        sys.exit(1)
    event_store = open_events(events_db_path)
    server = AvunelEventServer(event_store=event_store, host=host, port=port, token=resolved_token)

    def handle_signal(sig: int, frame: Any) -> None:
        print("\n[avunel serve] Shutting down cleanly...")
        server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(f"[avunel serve] Event server listening on http://{host}:{port}/")
    print(f"[avunel serve] Cockpit UI: http://{host}:{port}/?token={server.token}")
    print(f"[avunel serve] SSE Stream: http://{host}:{port}/api/events/stream?token={server.token}")

    try:
        server.start(background=False)
    except KeyboardInterrupt:
        server.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="avunel serve",
        description="Run the Avunel Event Log SSE server and Cockpit UI.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Loopback port to bind (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"Loopback host to bind (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Auth bearer token (or set AVUNEL_SERVE_TOKEN)",
    )
    parser.add_argument(
        "--events-db",
        type=Path,
        default=None,
        help="Path to events.db (defaults to $AVUNEL_HOME/events.db)",
    )

    args = parser.parse_args()
    run_serve_command(
        events_db_path=args.events_db,
        host=args.host,
        port=args.port,
        token=args.token,
    )


if __name__ == "__main__":
    main()
