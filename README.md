<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/avunel/banner-dark.svg">
    <img src="assets/avunel/banner-light.svg" alt="avunel — temporal memory agent" width="720">
  </picture>
</p>

# Avunel

**A personal AI agent built on the [Hermes Agent](https://github.com/NousResearch/hermes-agent) framework by [Nous Research](https://nousresearch.com).** Avunel takes the engine, the plugin system, the skill system, and the state layout from Hermes — then runs on Iki's own rules. **Avunel does not track upstream Hermes and does not take updates from it.** The framework is shared; the rules, the identity, and the direction are not.

This is not a separate product for other people. If you want the upstream project, install Hermes Agent from Nous Research (see [Framework reference](#framework-reference)).

## Brand

The wordmark is **avunel** in [DM Serif Display](https://fonts.google.com/specimen/DM+Serif+Display), the same typeface the desktop surfaces load. The terminal cannot render arbitrary fonts, so the CLI and TUI carry the identity through a monogram, spacing, and the palette instead.

| Colour | Hex | Role |
|--------|-----|------|
| Burgundy | `#790D16` | Primary accent, active indicators |
| Warm Ivory | `#F5EFE1` | Wordmark, foreground text |
| Dusty Blue | `#AEC4D4` | Secondary accents, metadata |

The terminal's own background is never overridden — the theme adapts to a dark or light terminal rather than imposing one.


## What Avunel changes

| Area | Change |
|------|--------|
| **Skin** | An `avunel` theme (burgundy, warm ivory, dusty blue) with a DM Serif wordmark, used by the CLI and TUI. |
| **Identity** | User-facing text says *Avunel* instead of *Hermes Agent*. Internal package/module names, `HERMES_*` environment variables, licensing, and Nous Research attribution are unchanged. |
| **Data home** | New installs default to `~/.avunel/` (Windows: `%LOCALAPPDATA%\avunel`). `AVUNEL_HOME` is the canonical override; the legacy `HERMES_HOME` still works. A machine that already has only `~/.hermes/` keeps using it — existing data is never moved automatically. |
| **Versioning** | Avunel has its own version (`0.1.0`), independent of Hermes releases. The base Hermes commit and live Git provenance are shown for reference, not as an update channel. |
| **Temporal memory** | A `temporal` memory provider that extracts dated facts from conversation, with a guard so habits without a concrete time are not stored as temporal items. |
| **State model** | Event log as the source of truth; `state.db` is a projection. See `planning/avunel-addendum-v5-final.md`. |

Everything else — providers, tools, skills, gateway, cron, subagents — is the shared Hermes framework, carried forward by Iki's own decisions rather than by upstream merges.

## Data home

Avunel keeps its state in its own directory:

| Platform | Default |
|----------|---------|
| Linux / macOS | `~/.avunel/` |
| Windows | `%LOCALAPPDATA%\avunel\` |

Resolution order (highest first):

1. `AVUNEL_HOME` — canonical override.
2. `HERMES_HOME` — legacy override, still honored.
3. An existing `~/.hermes/` on a machine with no `~/.avunel/` — reused so old data is not orphaned.
4. The platform default above — a brand-new install.

Existing data is **never** moved, overwritten, or deleted automatically. To adopt the new home deliberately, set `AVUNEL_HOME` or move the directory yourself.

## Running it

Three launchers, kept apart so testing never touches real data:

```bash
avunel              # binary install (~/.avunel/hermes-agent)        data: ~/.avunel
avunel-daily        # clean checkout (~/projects/avunel-agent-clean) data: ~/avunel-daily-data
avunel-events-test  # sandbox for event-log work                     data: ~/avunel-events-data
```

`avunel-events-test` lands with fase -1 of the event-log work; until then only the first two exist.

Both live launchers take the same arguments as the `hermes` command (`avunel --help`, `avunel model`, …).

## Version

```bash
$ avunel --version
Avunel 0.1.0
Based on Hermes v0.21.5
Base: 27c02f63
Local: 378ac235 (+15 commits)
Build: dirty · 2026.10.10
```

Avunel's version (`0.1.0`) is its own and does not inherit Hermes releases. The lines below it are provenance: the Hermes commit the fork started from, the local commit, how many commits Avunel carries since that base, and the working-tree date. **These are historical reference points, not an update channel** — Avunel does not pull from upstream. If Git is unavailable, those fields report `unknown` rather than a fake value.

## Framework reference

The Hermes Agent project is the origin of the framework Avunel is built on. Its documentation is useful for understanding the engine Avunel inherited; it is **not** Avunel's documentation and its issues are **not** Avunel's issue tracker.

- **Framework docs:** [hermes-agent.nousresearch.com/docs](https://hermes-agent.nousresearch.com/docs/)
- **Framework repo:** [github.com/NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent)

Avunel's own roadmap and design decisions live in `planning/` (`avunel-roadmap.md`, `avunel-addendum-v5-final.md`).

## License

MIT — see [LICENSE](LICENSE). Avunel is built on the Hermes Agent framework by [Nous Research](https://nousresearch.com); the Avunel modifications are Iki's.
