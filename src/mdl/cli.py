"""Command line entry point: ``mdl datasets | ingest-ecb | synth``.

The same commands run locally (landing root = a git-ignored folder) and in
Lakeflow Jobs (landing root = ``/Volumes/<catalog>/landing/raw``).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from mdl import synth
from mdl.sources import ecb
from mdl.sources.landing import Landing
from mdl.thermal import ThermalGuard

log = logging.getLogger("mdl")


def _parse_start(value: str | None) -> datetime:
    if not value:
        return datetime.now(UTC).replace(microsecond=0)
    at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return at if at.tzinfo else at.replace(tzinfo=UTC)


def cmd_datasets(_: argparse.Namespace) -> int:
    for ds in ecb.DATASETS.values():
        print(f"{ds.name:22} {ds.kind.value:9} {ds.resource:12} {ds.description}")
    return 0


def cmd_ingest_ecb(args: argparse.Namespace) -> int:
    landing = Landing(args.landing)
    names = list(ecb.DATASETS) if args.dataset == ["all"] else args.dataset
    unknown = sorted(set(names) - set(ecb.DATASETS))
    if unknown:
        print(f"unknown dataset(s): {', '.join(unknown)}", file=sys.stderr)
        return 2
    for name in names:
        manifests = ecb.ingest(
            ecb.DATASETS[name],
            landing,
            backfill_start=args.backfill_start,
            window_days=args.window_days,
            max_windows=args.max_windows,
            margin=timedelta(hours=args.margin_hours),
            full=args.full,
        )
        for manifest in manifests:
            summary = {k: v for k, v in asdict(manifest).items() if k not in ("extra", "request_params")}
            summary["mode"] = manifest.extra.get("mode")
            print(json.dumps(summary, sort_keys=True))
    return 0


def cmd_synth(args: argparse.Namespace) -> int:
    landing = Landing(args.landing)
    guard = ThermalGuard.from_env()
    start = _parse_start(args.start)
    total = 0
    for burst in range(args.bursts):
        guard.check()
        cfg = synth.SynthConfig(
            start=start + timedelta(seconds=burst * args.seconds),
            seconds=args.seconds,
            seed=args.seed + burst,
            trade_rate_hz=args.rate,
        )
        batch = synth.generate(cfg)
        paths = synth.write_bursts(batch, landing, burst_size=args.burst_size)
        total += len(batch.events)
        log.info("burst %d: %d events, %d files", burst, len(batch.events), len(paths))
    print(json.dumps({"events": total, "bursts": args.bursts, "landing": str(landing.root)}))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mdl", description=__doc__.splitlines()[0])
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("datasets", help="list ECB datasets").set_defaults(func=cmd_datasets)

    ing = sub.add_parser("ingest-ecb", help="fetch ECB datasets into the landing zone")
    ing.add_argument("--landing", required=True, help="landing root (local folder or /Volumes/...)")
    ing.add_argument("--dataset", nargs="+", default=["all"], help="dataset names, or 'all'")
    ing.add_argument("--backfill-start", default="2020-01-01", help="startPeriod for first/full loads")
    ing.add_argument("--window-days", type=int, default=366, help="backfill window length")
    ing.add_argument("--max-windows", type=int, default=8, help="backfill windows per run and dataset")
    ing.add_argument("--margin-hours", type=float, default=6.0, help="overlap for updatedAfter")
    ing.add_argument("--full", action="store_true", help="ignore state and reload from backfill start")
    ing.set_defaults(func=cmd_ingest_ecb)

    syn = sub.add_parser("synth", help="write synthetic trades and book snapshots")
    syn.add_argument("--landing", required=True)
    syn.add_argument("--seconds", type=int, default=60, help="simulated seconds per burst")
    syn.add_argument("--bursts", type=int, default=1)
    syn.add_argument("--seed", type=int, default=7)
    syn.add_argument("--rate", type=float, default=5.0, help="trades per second per instrument")
    syn.add_argument("--burst-size", type=int, default=5000, help="max events per file")
    syn.add_argument("--start", help="ISO start time (default: now, UTC)")
    syn.set_defaults(func=cmd_synth)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(message)s")
    code: int = args.func(args)
    return code


if __name__ == "__main__":
    sys.exit(main())
