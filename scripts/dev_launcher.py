"""Avunel Dev Sandbox Launcher & Dispatcher.

Enforces strict isolation from ~/avunel production environment.
Dispatches dev sandbox commands:
- serve: start F0 event server
- tui: start timeline TUI
- verify-g0a2: run dual-write verification in sandbox
- python / shell fallback
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

PROD_DIR = Path.home() / "avunel"


def enforce_isolation_guard() -> None:
    """Ensure no environment variable or working directory targets ~/avunel."""
    prod_resolved = PROD_DIR.resolve()

    # Enforce sandbox environment variables: if not set, set to safe default sandbox
    if not os.environ.get("AVUNEL_HOME"):
        default_sandbox = (Path.home() / "avunel-events-data").resolve()
        os.environ["AVUNEL_HOME"] = str(default_sandbox)
    if not os.environ.get("HERMES_HOME"):
        os.environ["HERMES_HOME"] = os.environ["AVUNEL_HOME"]

    for var in ("AVUNEL_HOME", "HERMES_HOME"):
        val = os.environ.get(var)
        if val:
            path_resolved = Path(val).resolve()
            if path_resolved == prod_resolved or prod_resolved in path_resolved.parents:
                sys.stderr.write(
                    f"FATAL ISOLATION GUARD: Environment variable {var}={val} resolves "
                    f"to production path {prod_resolved}!\n"
                    f"Refusing to execute to protect production data.\n"
                )
                sys.exit(1)


def main() -> None:
    enforce_isolation_guard()

    if len(sys.argv) < 2:
        print("Avunel Dev Sandbox Launcher")
        print("Usage: avunel-sandbox <subcommand> [args...]")
        print("Subcommands:")
        print("  serve         Start F0 event SSE server (avunel_serve)")
        print("  tui           Start F0 timeline TUI client")
        print("  verify-g0a2   Run G0a-2 dual-write runtime verification")
        print("  demo          Run full F0 vertical slice demo")
        print("  python        Run Python interpreter in sandbox")
        sys.exit(0)

    subcmd = sys.argv[1]
    args = sys.argv[2:]

    if subcmd == "serve":
        from avunel_serve import main as serve_main
        sys.argv = ["avunel-serve"] + args
        serve_main()
    elif subcmd == "tui":
        from avunel_timeline_tui import main as tui_main
        sys.argv = ["avunel-timeline-tui"] + args
        tui_main()
    elif subcmd == "verify-g0a2":
        from scripts.verify_g0a2 import main as verify_main
        sys.argv = ["verify-g0a2"] + args
        verify_main()
    elif subcmd == "demo":
        from scripts.demo_f0_e2e import main as demo_main
        sys.argv = ["demo-f0-e2e"] + args
        demo_main()
    elif subcmd == "python":
        import runpy
        if args:
            sys.argv = args
            runpy.run_path(args[0], run_name="__main__")
        else:
            import code
            code.interact()
    else:
        # Fallback to hermes_cli main
        from hermes_cli.main import main as cli_main
        sys.argv = [sys.argv[0]] + sys.argv[1:]
        cli_main()


if __name__ == "__main__":
    main()
