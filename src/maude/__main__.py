"""Command line entry point: python -m maude --env dev --stage all"""

from __future__ import annotations

import argparse
import os

from maude.config import ENVIRONMENTS, load_config
from maude.pipeline import STAGES, run_pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m maude", description=__doc__)
    parser.add_argument(
        "--env",
        choices=ENVIRONMENTS,
        default=os.environ.get("MAUDE_ENV", "dev"),
        help="configuration to use (default: $MAUDE_ENV or dev)",
    )
    parser.add_argument(
        "--stage",
        choices=(*STAGES, "all"),
        default="all",
        help="run a single stage, or all stages in order (default)",
    )
    args = parser.parse_args(argv)

    stages = STAGES if args.stage == "all" else (args.stage,)
    run_pipeline(load_config(args.env), stages)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
