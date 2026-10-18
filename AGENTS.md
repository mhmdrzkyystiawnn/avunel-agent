# Avunel — Development Guide

Instructions for AI coding assistants and developers working on the **Avunel** codebase.

This root file holds only what applies everywhere. Each area has its own `AGENTS.md` (aim for
~8k chars; `agent/subdirectory_hints.py` delivers up to 32k and truncates head/tail with a warning
past that); see the **routing table** at the end and read the area file before editing in that area.

**Never give up on the right solution.**

---

## What Avunel Is

Avunel is a **personal AI agent** that runs the same agent core across a CLI, a messaging
gateway (Telegram, Discord, Slack, ~20 platforms), a TUI, and an Electron desktop app. It
learns across sessions (memory + skills), delegates to subagents, runs scheduled jobs, and
drives a real terminal and browser. It is extended primarily through **plugins and skills**,
not by growing the core.

**Avunel is a personal fork of Hermes Agent (Nous Research).** Avunel takes the **framework**
from Hermes — the runtime (`hermes_agent` binary, `hermes_cli` modules, `hermes_bootstrap`,
PM-managed venv), the plugin system, the skill system, the state.db layout — and **replaces
the rules with Iki's own.** Avunel does **not** follow Hermes upstream updates. Avunel does
**not** need updates from Hermes. When in doubt about a convention, **ask Iki** — do not
default to "what Hermes does".

The runtime install is `~/.avunel/hermes-agent/` (the folder name `hermes-agent` stays
intact: it is the binary install identifier, not a brand statement). The data home is
`~/.avunel/`. The fork repo is `git@github.com:mhmdrzkyystiawnn/avunel-agent.git`.

Two invariants shape almost every design decision and are the lens for reviewing any change.
These are **Avunel-specific** — they are not the NousResearch Hermes invariants.

- **Event log is the source of truth.** `state.db` is a projection. Every state mutation
  flows through `emit()`; every read is a re-derivation from the event log. This is set
  out in `planning/avunel-addendum-v5-final.md` (K3 = event log replaces state.db as
  source of truth; K4 = `events.db` lives in a separate file from `state.db`). It changes
  the design surface area: no FTS5-corruption machinery to keep working (transcript_repair,
  session_recovery, session_lost_and_found, hermes_state_repair — all go away in favour of
  append-only event log + projection rebuild), no big table to back up, no schema-migration
  story to maintain. The price is write-amplification (1 INSERT into events per mutation
  plus 1 update on the projection), paid for in the form of a recoverable, auditable,
  branchable history.
- **One install, three launchers, one source of truth.** The binary install at
  `~/.avunel/hermes-agent/` is what `avunel` runs. The clean transport repo at
  `~/projects/avunel-agent-clean/` (with its data home `~/avunel-daily-data/`) is what
  `avunel-daily` runs. The sandbox at `~/avunel-events-data/` (when fase -1 lands) is what
  `avunel-events-test` runs. The fork's `main` branch is the runtime install; `development`
  is the clean repo working tree; `ci/repair-failing-tests` is the CI branch. Push only from
  the clean repo. Test the runtime behaviour from each launcher before trusting a fix.

## What We Want / What We Don't

The project's intent layer. Read the balance right: Avunel ships a **lot** of capability at
the edges, and the rest of this document is about which of that capability is paid for at
the core. "Smallest footprint" governs *how a capability is wired into the core*, not whether
the product may grow: expansive at the edges, conservative at the core.

### What we want

- **Fix real bugs, well.** Reproduce the symptom on current `main`, point to the exact line
  where it manifests, and fix the whole bug class — sibling call paths included.
- **Expand reach at the edges.** New adapters, channels, providers, models, desktop/TUI/
  dashboard features land routinely, including large ones — as long as they integrate with
  the existing setup/config UX (`avunel tools`, `avunel setup`, auto-install) rather than
  bolting on a raw env var.
- **Refactor god-files into clean modules.** Huge mechanical `+N/-N` extraction PRs are
  wanted work. "Every line traces to the request" applies to *feature* PRs; a declared
  refactor's request IS the extraction.
- **Keep the core narrow.** Prefer, in order: extend existing code → CLI command + skill →
  service-gated tool (`check_fn`) → plugin → MCP server in the catalog → new core tool (last
  resort).
- **Extend, don't duplicate.** Check whether existing infrastructure covers the use case
  before adding a module/manager/hook. When 3+ open PRs integrate the same *category* of
  feature into the core, the right answer is to extract a plugin shape, not to ship the
  third binding.
- **Design for replay and rewind.** The event log is the source of truth precisely so that
  any past state is reproducible from the log. New state mutations should make the event
  easy to log (a single `emit()` call) and the projection update easy to re-derive
  (idempotent). Branching and undo follow the same line of reasoning.
- **Test the real outcome.** "Tests pass" is not "behaviour is correct." A test that
  asserts the wrong value, or that runs against the wrong fixture, passes. For DB issues,
  probe the live state. For visual / output issues, inspect the actual rendered result.
  For runtime behaviour, exercise the same code path the user would.

### What we don't want

- **Mocking the data layer in a way that does not match the live schema.** `state.db` schema
  is not what you guess from the type stubs. `sessions.id` (not `session_id`),
  `messages.session_id`. Inspect with `PRAGMA table_info(<table>)` before writing a query.
- **Adding a new core tool when a CLI command + skill would do.** Every model tool is sent
  on every API call. The bar for a new *core* tool is high.
- **Touching memory, skills, or system-prompt state mid-conversation without going through
  the cache-aware path.** The prompt cache is a long-lived prefix that survives a
  conversation; mutating it mid-session invalidates the cache and multiplies cost. Slash
  commands that touch these layers (skills, memory, tools) default to deferred
  invalidation (`--now` is the explicit opt-in; `/skills install --now` is the canonical
  pattern). Compression is the one exception.
- **Fictional evidence.** "I ran the test" when you did not. "The DB is in this state" when
  you did not probe. "The user said X" when they did not. Claims must trace to a real
  command, a real file read, a real probe. When unsure, say so plainly.
- **Hard-coded values that should be data.** A real bug, a real skin, a real plugin — not
  a placeholder.
- **Mid-conversation cache break.** Mutating past context, swapping toolsets, reloading
  memories, or rebuilding the system prompt mid-conversation invalidates the prompt cache
  and multiplies the user's cost. Do not do it; compression is the ONE exception.

## Contribution Rubric (Avunel-specific)

These are the rules Iki has set. They are **not** inherited from NousResearch; they are
explicit choices for Avunel.

- **One source of truth: the event log.** Every state mutation goes through `emit()`.
  Direct `INSERT`/`UPDATE` to tables in the conversation domain (sessions, messages,
  system_prompts, branches, undo) is forbidden outside the projection handler. The runtime
  guard fails the test if a forbidden write is detected.
- **Three launchers, three data homes, one source of truth.** `avunel` (daily binary
  install, `~/.avunel/`), `avunel-daily` (clean transport repo + data home,
  `~/projects/avunel-agent-clean/` + `~/avunel-daily-data/`), `avunel-events-test` (sandbox
  for the event log work, `~/avunel-events-data/`, lands at fase -1). Each launcher sets
  `HERMES_HOME` to its own data home. They never share either path. Push from the clean repo
  only.
- **State.db as projection, not source.** Read-only `state.db` consumers stay read-only;
  the projection is rebuilt from the event log when the schema or schema-version changes.
  The fork-specific bug is that `_real_platform_state_root` was hardcoded to `~/.hermes`
  in `hermes_state_guard.py`; it must check both `~/.avunel` (primary) and `~/.hermes`
  (legacy) so the guard catches subprocess escapes on Avunel hosts.
- **`hermes update` defaults to fork origin, not NousResearch.** `origin` in the fork repo
  points to `git@github.com:mhmdrzkyystiawnn/avunel-agent.git`. `hermes update` fetch
  default = `origin/main`. The NousResearch upstream reference exists in
  `.github/actions/plugin-validate/action.yml`; that reference is **removed** for Avunel
  (K6 from the addendum). Diff baseline against NousResearch may be a manual `git fetch
  upstream` if needed; it is not part of the default flow.
- **Skill maintenance: Avunel-specific paths, brand, and launchers.** A skill that
  references `~/.hermes/`, `hermes-dev`, or "Hermes Agent" in display text is partial.
  `avunel-skill-update` is the maintenance entry point; its procedure classifies every
  hit into Display text / Runtime paths / Launchers / Identifier (skip) before patching.
  Skill **folders** are named `avunel-*` (e.g. `avunel-dev-environment`,
  `avunel-skin-theming`, `avunel-agent-skill-authoring`, `avunel-agent`); the folder name
  is an identifier and is renamed in lockstep across the catalog, not file-by-file.
- **Identifier boundary.** Module names (`hermes_cli`, `hermes_state`, `hermes_constants`,
  `hermes_yaml`, `hermes_agent`), env vars (`HERMES_HOME`, `HERMES_RUNTIME_DIR`,
  `HERMES_TEST_SLICE`), and the install folder name (`hermes-agent`) stay intact. Renaming
  them breaks thousands of imports and the app will not boot. The fact that the runtime
  install is `~/.avunel/hermes-agent/` is intentional. License / credit: `MIT` + `Nous
  Research` upstream attribution + "the Avunel modifications are mine" — preserved.
- **Event-log semantics:** `append-only` enforced at the DB level (`BEFORE UPDATE` /
  `BEFORE DELETE` triggers on the `events` table raise `ABORT`). Branches use `parent_id`
  (FK to `events.id`), not `parent_session_id`. Undo writes an event
  (`conversation.undone`) — it does not UPDATE the messages table. Rebase / squash /
  rewrite happens in a new event chain, not by mutating existing events.

## Testing (Avunel-specific)

The runtime's test suite is the contract. Adapt it to the actual requirement — do not weaken
it, do not skip a failing test, do not pass a wrong assertion.

- **Entry point:** `bash scripts/run_tests.sh [path] [args]`. The script honours
  `HERMES_TEST_SLICE` (0-indexed out of `HERMES_TEST_SLICE_OF`) and `HERMES_TEST_WORKERS`
  (parallel processes; default 4 to match the 4-core runner). Do not invoke `pytest`
  directly — that bypasses the slice/timeout logic and may write to `~/.hermes/` by
  accident. Tests must redirect `HERMES_HOME` to a temp dir; never hardcode `~/.hermes/`
  in tests. (Avunel tests will, in due course, redirect to `~/.avunel/` — same pattern,
  updated value.)
- **One test = one contract.** Tests that are loosely worded or that assert the wrong
  value will pass and give false confidence. The agent must read the test before trusting
  it. If the test says "X is a dict" but the contract is "X is a dict with at least these
  two keys," rewrite the test to assert what matters.
- **Test that fails before the fix, passes after.** A test that runs only against the
  patched code proves nothing. `git stash` the fix, run the test, confirm it fails with
  the bug's symptom; unstash, confirm it passes. The pre-fix failure is the test's
  evidence that it actually exercises the bug.
- **For DB issues, probe the live database.** `~/.avunel/state.db` is the real
  data. Schema files, Prisma models, type stubs, and test fixtures are *approximations*.
  When they disagree, the live DB wins. `sqlite3 -readonly /home/iki/.avunel/state.db
  ".schema <table>"` to see what is actually there; `PRAGMA table_info(messages)` to see
  the columns.
- **For state recovery, test through the event log.** The state-recovery machinery
  (transcript_repair, session_recovery, session_lost_and_found, hermes_state_repair) is
  replaced by event-log replay. Tests for the old recovery paths are deleted; tests for
  the new replay are added. FTS5 corruption tests are dropped in favour of "rebuild
  FTS5 from the projection."
- **CI gate before merge.** A change lands in `ci/repair-failing-tests` first, then
  moves to `development`, then to `main` after CI is green. CI is green when the full
  Python suite, the JS/TS suite, and `check:test:ui` are all passing. `check:test:ui` is
  flaky by design (timer-after-teardown in `apps/desktop/src/store/notifications.ts:344`),
  so the gate is "stable over the last 3 CI runs," not "single run green."
- **No fabrication.** A test that "passes" because the assertion is `assertTrue(True)`
  is a fabrication. A test that passes because the fix changed the test rather than the
  behaviour is a fabrication. The agent must read the test, read the implementation, read
  the spec, and confirm that what the test asserts is what the user actually wants.
- **Change-detector tests are forbidden.** A test that fails whenever data *expected to
  change* is updated (model catalogs, `_config_version`, enumeration counts, hardcoded
  model lists) adds no coverage and taxes every routine update. The test reads as a
  contract between two pieces of data — keep it; reads as a snapshot — delete it.
- **Tests must not read source code as text.** A test that reads a `.py`/`.ts`/`.tsx`
  file's text tests the *shape of the source*, not behaviour — banned outright. It
  passes when the implementation is subtly broken (regex matches a mis-wired call site)
  and fails on correct refactors. Extract the logic into a pure / DI-testable function
  and call it.

## Plugin Development (Avunel-specific)

Avunel plugins live in `~/.avunel/plugins/<name>/` (user-local) or
`~/projects/avunel-agent-clean/plugins/<name>/` (in-repo). All Avunel plugins are owned by
Iki; there is no third-party migration story. Convention below replaces the Hermes
plugin convention.

- **Two plugin kinds: `standalone` (hook-only) and `MemoryProvider`.** A `standalone`
  plugin is the cheapest way to observe the agent loop; a `MemoryProvider` is the only
  way to extend the memory subsystem.
- **For `MemoryProvider`, the 5 s drain budget is sacred.** `MemoryManager.sync_all()`
  does not call `sync_turn` inline; it submits to a single-worker executor with a 5 s
  drain budget. Any work slower than 5 s is silently lost. Expensive work goes in
  `on_session_end` (which runs before the drain, bounded by the CLI exit watchdog) — not
  in `sync_turn`. The 5 s budget is a core constant, not a tunable.
- **For `standalone` plugins, `pre_api_request.request_messages` is the truth.** Not
  `pre_llm_call.conversation_history` (which excludes the current turn's injected context).
  `messages.api_content` in `state.db` is what was actually sent to the provider. Read
  these fields when measuring accumulation; do not infer from `conversation_history`.
- **`avunel plugins enable` is the admission gate** (network needed, runs `pm
  sync_venv`). A user-dir MemoryProvider does not need the admission gate — set
  `memory.provider: <name>` in `~/.avunel/config.yaml` and it loads.
- **No third-party plugin contracts.** Avunel does not support plugin-as-a-product. The
  NousResearch "ship your plugin as a standalone repo" path is closed for Avunel. Every
  Avunel plugin is in-tree (under `plugins/` or `~/.avunel/plugins/`) and is owned by
  Iki. There is no catalogue, no admission-pipeline, no plugin-as-pip-package contract.
- **No fabricated plugin state.** A plugin that claims to have persisted X must be able
  to read X back from the same data store in a fresh process. `avunel -z "..."` is a
  fresh process. If you cannot read the data in a fresh process, the persistence is
  fictional.

## State, FTS5, and Recovery

The Hermes-agent state-recovery machinery (transcript_repair, session_recovery,
session_lost_and_found, hermes_state_repair, FTS5 corruption handling, JSONL replay at
`gateway/shutdown_flush.py`) exists because `state.db` is mutable and can be corrupted or
lose sessions. **Avunel replaces that machinery with event-log replay.**

- **Recovery = replay the event log.** No more `transcript_repair` patching active=0 in
  `messages`; no more `session_lost_and_found` stitching session ids back together. If
  the event log is intact, the projection can be rebuilt. If the projection is stale,
  rebuild it.
- **Snapshot = event chain from a specific event id + filesystem snapshot.** Branch
  from event N, restore the snapshot at event N, replay forward. The "right solution" is
  the one that always works, not the one that patches the failure.
- **FTS5 stays in the projection.** The 11 triggers in `messages` go away; FTS5
  becomes a virtual table rebuilt from the projection. FTS5 corruption machinery goes
  away; if the FTS5 index is stale, rebuild it from the projection.
- **JSONL journal at `gateway/shutdown_flush.py`** contains events, not messages.
  Pending events are appended before shutdown, replayed on start.
- **Failure mode: the event log itself can be lost or corrupted.** That is a real
  failure mode for an append-only store, just as it was for `state.db`. The recovery
  story in that case is: restore the event log from a snapshot (daily backup of
  `events.db` is the contract — every `avunel update` takes one), then rebuild the
  projection. There is no "transcript_repair"-style patching of the event log.

## Skin Theming

The skin engine (`hermes_cli/skin_engine.py`, `display.skin` in config) is the single
source of palette truth for every surface. Avunel uses this engine directly; the
conventions below are Avunel-specific adaptations.

- **Two kinds of skin.** Built-in (in `hermes_cli/skin_engine.py::_BUILTIN_SKINS`) and
  user-local (`~/.avunel/skins/<name>.yaml`). User-local wins for the same name. The
  Avunel default is `default` (built-in); `~/.avunel/skins/avunel.yaml` is the personal
  override when present.
- **Three layers of "the word" — Hermes / Avunel.** Display text (rename) is what the
  user reads (banner, `--version`, `--help`, persona prompt, locale `en.yaml`,
  welcome/goodbye). Internal identifiers (`hermes_cli`, `hermes_state`, `HERMES_HOME`,
  install folder name) NEVER rename. License / credit (`MIT`, `Nous Research`, upstream
  repo URLs) stays. `display-text-rebrand.md` (in the `avunel-skin-theming` skill) is
  the canonical reference; read it before editing any surface.
- **The product-NAME rename is display text only.** The word "Hermes" in
  `hermes_constants.PRODUCT_NAME` was changed to "Avunel" in the fork; the env var
  `HERMES_HOME`, the install folder `hermes-agent`, the module name `hermes_cli`, and
  the URL `github.com/NousResearch/hermes-agent` stay. Display sites read
  `PRODUCT_NAME`; identifiers stay.
- **A renamed fork also has its own version.** `AVUNEL_VERSION: str = "0.1.0"` in
  `hermes_constants.py`, reported separately from the upstream base. Provenance
  (upstream sha, local sha, carried commit count, dirty flag) is computed from live git
  (`_git_upstream_base` in `hermes_cli/version_info.py`) — never hardcoded.
- **Banner reading:** `--version` prints the Avunel install directory, version block, and
  the live git provenance. The banner patch is in `hermes_cli/banner.py`; the version
  printer is in `hermes_cli/version_info.py`. Both share `_git_upstream_base` so they
  cannot disagree.
- **The terminal cannot render arbitrary fonts.** Express the brand through spacing, a
  monogram mark, and monospace. Deliver a real display font only to graphical surfaces
  via the skin's `customCSS` field (injected as a `<style>` tag on desktop / web), scoped
  to a selector (e.g. `.wordmark`), never to conversation / code.

## CI

The fork's CI is in `.github/workflows/tests.yml`. The user-local CI behaviour is run
manually with `avunel update` (after a manual commit on the fork) — Avunel does not
auto-update.

- **Branch model:** `ci/repair-failing-tests` (work in progress), `development` (clean
  repo working tree, ready to push), `main` (runtime install, mirror to `~/.avunel/
  hermes-agent/` via `avunel update`).
- **Gate:** CI is green when the full Python suite, JS/TS suite, and `check:test:ui`
  are all passing — for 3 consecutive runs. `check:test:ui` is flaky (timer-after-
  teardown in `apps/desktop/src/store/notifications.ts:344`); a single green run is not
  a green gate.
- **Upstream reference removed.** The NousResearch upstream reference in
  `.github/actions/plugin-validate/action.yml` is removed for Avunel (K6 from the
  addendum). Diff baseline against NousResearch, if needed, is a manual `git fetch
  upstream` — it is not part of the default CI flow.
- **Workflow contract:** the Avunel CI workflow file is the runtime. If you change
  the workflow, also change the runtime install (`~/.avunel/hermes-agent/`) — both
  must agree on what "green" means.
- **CI-only Python requirements:** `==exact`. A bare `>=X.Y.Z` is rejected by CI and
  reviewers.

## Hardline Conventions (Avunel)

These are the rules that, when broken, are PR-blocking. They are **Avunel-specific**; do
not invoke NousResearch convention as a counter-argument.

### Skill authoring (K7)

- `name`: lowercase, hyphens, ≤ 64 chars.
- `description`: ≤ 60 chars (hardline; the validator's 1024 is not the standard), one
  sentence, ends with a period, no marketing words, no colon unless quoted.
- `name` field is unique across the catalog at any given moment. Rename via the
  catalog (move to a new folder), not via patch.
- `metadata.hermes.tags` and `metadata.hermes.related_skills` must resolve. Related
  skills must exist in the catalog; tags must be lowercase, hyphenated, descriptive.
- The skill body is capped at 100,000 chars. Aim for ~150 lines for a typical skill;
  longer material goes into `references/`, `templates/`, or `scripts/`.
- Frontmatter begins with `---` on the first byte (no leading blank line), closes
  with `\n---\n`, then a non-empty body.
- Skill **folder names** are Avunel-style: `avunel-*`. Renaming a folder is a lockstep
  operation across the catalog (related_skills references, prose references), not a
  file-by-file change. Backup to `~/.avunel/.skill-backup-<YYYYMMDD>/` first.

### Code and runtime

- `scripts/run_tests.sh` is the test entry point. `pytest` direct is forbidden.
- `state.db` writes go through `SessionDB.acquire()` (or the future `emit()`). Direct
  SQL outside the projection handler is forbidden in the conversation domain.
- Branching is `parent_id`-based, not `parent_session_id`-based.
- Undo writes an event; it does not mutate messages rows.
- Every state mutation must be loggable as a single event. If a state change requires
  multiple writes, the writes belong in a single transaction with the event log
  insertion.
- `busy_timeout` is set per-connection via `open_db(... timeout=5)` (5 s for write
  paths). Event-log writes must set this explicitly; `busy_timeout=0` means fail-fast,
  not "wait forever."
- `wal_autocheckpoint=1000` is the default for state.db; `100` for kanban.db
  (write-heavy). Event log may use either, but the choice must be explicit.
- `synchronous=FULL` is the durable default. NORMAL is acceptable for projections only.
- `BEGIN IMMEDIATE` is used on every write that takes the WAL write lock up front; 1
  retry on `SQLITE_IOERR` is the contract. No busy-loop retries.
- `~/.avunel/hermes-agent/` is the runtime install. The folder name `hermes-agent`
  stays; it is an identifier, not a brand. Hardcode at your peril — `get_hermes_home()`
  for code paths, `display_hermes_home()` for user-facing text.

### Repository and git

- Push only from the clean transport repo (`~/projects/avunel-agent-clean/`). The
  runtime install is a working copy, not a push source.
- The clean repo's `upstream` is a *manual* reference to NousResearch; it is not
  fetched by `avunel update`. `origin` is the fork on GitHub.
- `LICENSE` keeps the MIT + Nous Research attribution. The Avunel modifications are
  marked separately.
- Commit history should be readable: many small commits, not one giant one. The
  user has asked for this explicitly; do not bundle.

## Documentation

- `avunel-roadmap.md` and `avunel-addendum-v5-final.md` in `~/Downloads/` (or
  `~/projects/avunel-agent-clean/planning/` after a copy) are the source of truth for
  what Avunel is and where it is going. The addendum is referenced from the body
  sections above.
- After a real problem is resolved, write the journey in
  `/home/iki/documentation-problem-solving/`. The format is in
  `collaborating-with-iki` § 11.
- Read the addendum before any design work. Read the problem-solving docs after any
  resolved incident.

## Routing Table

Read the area `AGENTS.md` before editing in that area. The root file holds only what
applies everywhere.

| Area | AGENTS.md | Notes |
|---|---|---|
| `agent/` | `agent/AGENTS.md` | Agent loop, prompt builder, memory provider interface |
| `cron/` | `cron/AGENTS.md` | Scheduled jobs, delivery, cron-specific state |
| `gateway/` | `gateway/AGENTS.md` | Messaging platform adapters, hosted rooms, slash commands |
| `hermes_cli/` | `hermes_cli/AGENTS.md` | CLI commands, subcommands, banner, version, skin engine |
| `plugins/` | `plugins/AGENTS.md` | Plugin contract, `register(ctx)`, admission gate |
| `skills/` | `skills/AGENTS.md` | Skill authoring, in-repo vs user-local |
| `tools/` | `tools/AGENTS.md` | Standalone tools (skill manager, etc.) |
| `tui_gateway/` | `tui_gateway/AGENTS.md` | TUI, change_watcher, widget host |
| `web/` | `web/AGENTS.md` | Web UI (Electron), desktop bridge, IPC |

A short prompt (`agent/subdirectory_hints.py`) reads the relevant `AGENTS.md` and injects
it into the system prompt. Up to 32k chars; truncated head/tail with a warning past that.
