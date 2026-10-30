# Avunel Fase 0 — Spesifikasi Kontrak Vertical Slice (G0-Contract)

**Status**: DIBEKUKAN & DILENGKAPI (FROZEN)  
**Tanggal**: 2026-10-30  
**Keputusan The Leader**: `DECISION: APPROVED - port 31032` (sequence 68)  
**Referensi Planning**: Sequence 56 (`planning/avunel-planning.md`)  
**Lokasi Berkas**: `docs/avunel-f0-contract.md` (di-track dalam git repository)

---

## 1. Parameter Jaringan & Keamanan

| Parameter | Nilai | Penjelasan |
|---|---|---|
| **Host / Bind** | `127.0.0.1` | Loopback lokal saja. Menolak bind ke interface publik (0.0.0.0) untuk keamanan desktop runtime. |
| **Port** | `31032` | Diputuskan oleh The Leader. Rentang valid: 1024–65535. Port invalid akan ditolak saat startup. |
| **Autentikasi Token** | Wajib (Tanpa Default Fallback) | Diambil dari argumen CLI `--token` atau variabel env `AVUNEL_SERVE_TOKEN`. Server dan klien menolak start tanpa token. |
| **Transport Token** | Header / Query | `Authorization: Bearer <token>` atau query param `?token=<token>` (dibutuhkan untuk SSE browser Native EventSource). |
| **Verifikasi Token** | `hmac.compare_digest` | Perbandingan konstan waktu (constant-time) untuk mencegah timing attack. |
| **Origin Policy** | Loopback Allowlist | Jika header `Origin` ada, wajib cocok dengan `http://127.0.0.1:<port>` atau `http://localhost:<port>`. Origin luar ditolak HTTP 403 Forbidden. Permintaan non-browser tanpa header Origin (CLI/TUI/curl) diizinkan selama token valid. |
| **Penanganan Error** | Sanitasi Ketat | Respons error berformat JSON murni `{"error": "<pesan_umum>"}`. Tidak ada stack trace atau path filesystem yang dibocorkan ke klien. |

---

## 2. Kontrak Endpoint SSE (`/api/events/stream`)

### Parameter Query
- `token`: Bearer token (jika tidak lewat header `Authorization`).
- `last_id`: Integer, default `0`. Server hanya mengirim event dengan `id > last_id`. Alternatif: header standard `Last-Event-ID` (diprioritaskan pada reconnect).
- `session_id`: (Opsional) Filter event hanya untuk sesi tertentu.
- `type`: (Opsional) Filter event berdasarkan tipe (misal `message.appended`).

### Format Payload SSE
Setiap event SSE dikirim dalam format standar W3C SSE dengan event type eksplisit:
```
id: <event_id>
event: <event_type>
data: {"id": <int>, "ts": <float>, "type": "<str>", "session_id": "<str>", "actor": "<str>", "branch_id": "<str>", "correlation_id": "<str|null>", "causation_id": "<int|null>", "payload_json": "<str>", "api_content_ref": "<str|null>", "created_at": "<str>", "is_redacted": <bool>, "has_blob": <bool>}

```

- `is_redacted`: Boolean penanda apakah event memuat rahasia yang telah disanitasi. Format redaksi nyata adalah masking head/tail (seperti `sk-liv...5678` atau `sk-liv...5555`) per engine sanitasi `agent/redact.py`.
- `has_blob`: Boolean penanda apakah payload/konten event di-offload ke CAS Blob Store (>60 KiB atau tipe `blob.stored`).

### Heartbeat & Reconnect
- **Heartbeat**: Dikirim setiap 15 detik jika tidak ada event baru lewat baris komentar SSE:
  ```
  : heartbeat <timestamp>

  ```
- **Reconnect Semantics**: Klien (TUI / Web) menyimpan `last_id` tertinggi yang berhasil diterima. Saat koneksi terputus dan tersambung kembali, klien mengirimkan `last_id` (via query param atau header `Last-Event-ID`), dan klien menerapkan deduplication guard `if (ev.id <= lastId) return;` sehingga tidak ada event yang hilang dan tidak ada event duplikat.
- **Polling Loop**: Server membaca SQLite view `events_effective` dengan polling interval 0.25 detik saat idle untuk mencegah busy-loop 100% CPU.

---

## 3. Kontrak Endpoint Lainnya

- `GET /`: Menyajikan Cockpit Web Dashboard statis mandiri (HTML/CSS/JS tanpa server dev eksternal), mendengarkan event via `addEventListener` per tipe nama event (`message.appended`, `session.created`, dll.) dan `onmessage`.
- `GET /api/health`: Status kesehatan server, port, dan jumlah event aktif.
- `GET /api/events`: Daftar event berhalaman dalam format JSON, memuat metadata `is_redacted` dan `has_blob`.
- `GET /api/sessions`: Daftar sesi unik yang tercatat di `events.db`.

---

## 4. Fixture Demo Sintetis

Untuk menjamin isolasi total dari data harian pengguna (`~/avunel`), demo dan automated gate menggunakan fixture sintetis `fixtures/synthetic_demo_events.json` yang mencakup:
- Sesi baru (`session.created`)
- Pesan pengguna & jawaban asisten (`message.appended`)
- Tool call & tool execution event (`tool.called`, `tool.completed`)
- Pengujian secret redaction otomatis (token `sk-...` disanitasi menjadi `sk-liv...5678`)
- Pengujian CAS blob offload (>60 KiB dipindahkan ke hash SHA-256)
- Pengujian undo/rewind percakapan (`conversation.undone`)

---

## 5. Rekaman Baseline & Pengecualian Test Suite Tertulis

- **Environment**: Linux x86_64, Python 3.14.7, pytest 9.1.1, uv 0.13.6
- **Test Suite F0 Eksisting**: 100 unit tests (`test_events_*.py`, `test_dual_sink.py`, `test_blob_store.py`, `test_importer.py`) — **100 PASSED**.
- **Dual-write Parity 49 Sesi**: `test_parity_projection.py` (18.883 pesan) — **6 PASSED**.
- **Runtime Harian `~/avunel/state.db`**: SHA-256 `a24dfbebf2580564fb3977d81f15e1ec0b9330ebd17d74a375b5fb2d69f33660`, mtime `2026-10-28 11:51:08` (0 byte tersentuh).
- **Status Suite Penuh `tests/hermes_state/`**: 1650 passed, 1 failed, 1 error pada `test_state_db_repair_non_destructive.py` bila dijalankan serentak dalam satu proses pytest besar. Berkas tersebut 100% lolos (24 passed) bila dijalankan tersendiri (`pytest tests/hermes_state/test_state_db_repair_non_destructive.py`). Ini merupakan kontaminasi isolasi state antar-test lama (pre-existing issue, bukan bagian kode Fase 0 yang diubah).
- **Keputusan Tertulis**: Isu pre-existing `test_state_db_repair_non_destructive.py` **secara resmi dikecualikan dari baseline G0-Release**. Baseline rilis Fase 0 mengacu pada 100 test backend F0 + seluruh test F0 vertical slice yang 100% hijau.
