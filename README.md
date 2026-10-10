<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/avunel/banner-dark.svg">
    <img src="assets/avunel/banner-light.svg" alt="avunel — temporal memory agent" width="720">
  </picture>
</p>

# Avunel

**A personal fork of [Hermes Agent](https://github.com/NousResearch/hermes-agent) by [Nous Research](https://nousresearch.com).** Same engine, my own skin and identity. Avunel tracks upstream Hermes and adds a few personal changes on top — nothing more.

This is not a separate product. If you want the real thing, install Hermes Agent from upstream (see [Upstream](#upstream)).

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
| **Versioning** | Avunel has its own version, independent of Hermes. The upstream Hermes version and Git provenance (upstream commit, local commit, carried commits, dirty state) are shown alongside it, computed live from Git. |
| **Temporal memory** | A `temporal` memory provider that extracts dated facts from conversation, with a guard so habits without a concrete time are not stored as temporal items. |

Everything else — providers, tools, skills, gateway, cron, subagents — is upstream Hermes.

## Running it

Two launchers, kept apart so testing never touches real data:

```bash
avunel          # dev checkout   (~/projects/hermes-agent-dev)  data: ~/hermes-dev-data
avunel-daily    # clean checkout (~/projects/avunel-agent-clean) data: ~/avunel-daily-data
```

Both take the same arguments as the `hermes` command (`avunel --help`, `avunel model`, …).

## Version

```bash
$ avunel --version
Avunel 0.1.0
Based on Hermes v0.21.5
Upstream: 27c02f63
Local: 378ac235 (+15 carried commits)
Build: dirty · 2026.10.10
```

Avunel's version (`0.1.0`) is its own and does not inherit Hermes releases. The lines below it are provenance: the upstream Hermes commit this fork is based on, the local commit, how many commits Avunel carries on top of upstream, and the working-tree date. If Git or the upstream ref is unavailable, those fields report `unknown` rather than a fake value.

## Upstream

Avunel is Hermes Agent. Read the upstream project for documentation, install instructions, and issues:

- **Docs:** [hermes-agent.nousresearch.com/docs](https://hermes-agent.nousresearch.com/docs/)
- **Repository:** [github.com/NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent)
- **Issues:** [NousResearch/hermes-agent/issues](https://github.com/NousResearch/hermes-agent/issues)

## License

MIT — see [LICENSE](LICENSE). Avunel is built on Hermes Agent by [Nous Research](https://nousresearch.com); the Avunel changes are mine.
