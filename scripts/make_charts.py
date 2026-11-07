"""Render the README figures from aggregate queries on the workspace.

Only derived counts leave the workspace: row counts per table, quarantined
rows per rule, freshness lags, and table-to-table lineage edges. No
observation values are read. Outputs (committed):

* docs/img/rows_by_layer.png
* docs/img/quarantine_by_rule.png
* docs/img/freshness.png
* docs/lineage.mmd (Mermaid graph for the README)

Colors follow the validated reference palette: categorical slots 1-4 in fixed
order for the four layers (light-mode contrast WARN, so every bar is labeled),
a single blue for one-series charts, and the status green/red only beside a
text label.

Usage: python scripts/make_charts.py --profile mdl --catalog mdl_dev
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle

from uc_bootstrap import _api, _warehouse

ROOT = Path(__file__).resolve().parents[1]
IMG = ROOT / "docs" / "img"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e6e5e1"
LAYER_COLORS = {"bronze": "#2a78d6", "silver": "#eb6834", "gold": "#1baf7a", "ops": "#eda100"}
SERIES = "#2a78d6"
GOOD, CRITICAL = "#0ca30c", "#d03b3b"


def query(profile: str, warehouse: str, sql: str) -> list[list[str | None]]:
    body = {"statement": sql, "warehouse_id": warehouse, "wait_timeout": "50s"}
    res = _api(profile, "post", "/api/2.0/sql/statements", body)
    while res.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        res = _api(profile, "get", f"/api/2.0/sql/statements/{res['statement_id']}")
    if res["status"]["state"] != "SUCCEEDED":
        raise SystemExit(f"query failed: {res['status'].get('error', {}).get('message')}")
    rows: list[list[str | None]] = res.get("result", {}).get("data_array", [])
    return rows


def _style(ax: Axes, title: str, subtitle: str) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.xaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_title(title, loc="left", fontsize=12, color=INK, pad=24, fontweight="bold")
    # Offset in points (not axes fraction) so tall figures keep title and subtitle apart.
    ax.annotate(subtitle, xy=(0, 1), xycoords="axes fraction", xytext=(0, 7), textcoords="offset points",
                fontsize=9, color=INK_2)  # fmt: skip


def _figure(n_rows: int) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots(figsize=(9, 0.32 * n_rows + 1.4), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    return fig, ax


def _save(fig: Figure, name: str) -> Path:
    IMG.mkdir(parents=True, exist_ok=True)
    path = IMG / name
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
    return path


def rows_by_layer(profile: str, wh: str, catalog: str, stamp: str) -> Path:
    tables = query(
        profile,
        wh,
        f"SELECT table_schema, table_name FROM {catalog}.information_schema.tables "
        "WHERE table_schema IN ('bronze', 'silver', 'gold', 'ops') "
        # Pipeline-internal tables (materialization backing tables, the event log) are not
        # part of the lakehouse and their names embed internal ids.
        "AND left(table_name, 2) <> '__' AND table_name NOT LIKE 'event_log%' ORDER BY 1, 2",
    )
    union = " UNION ALL ".join(
        f"SELECT '{s}' AS layer, '{t}' AS t, count(*) AS n FROM {catalog}.{s}.{t}" for s, t in tables
    )
    order = {"bronze": 0, "silver": 1, "gold": 2, "ops": 3}
    counts = sorted(
        ((str(r[0]), str(r[1]), int(str(r[2]))) for r in query(profile, wh, union)),
        key=lambda r: (order[r[0]], -r[2]),
    )
    fig, ax = _figure(len(counts))
    ys = list(range(len(counts)))[::-1]
    ax.barh(ys, [c[2] for c in counts], color=[LAYER_COLORS[c[0]] for c in counts], height=0.62,
            edgecolor=SURFACE, linewidth=2)  # fmt: skip
    ax.set_yticks(ys, [f"{c[0]}.{c[1]}" for c in counts])
    ax.set_xscale("log")
    ax.set_xlabel("rows (log scale)", color=INK_2, fontsize=9)
    for y, c in zip(ys, counts, strict=True):
        ax.text(c[2] * 1.08, y, f"{c[2]:,}", va="center", fontsize=8, color=INK)
    ax.set_xlim(1, max(c[2] for c in counts) * 4)
    handles = [Rectangle((0, 0), 1, 1, color=v) for v in LAYER_COLORS.values()]
    ax.legend(handles, list(LAYER_COLORS), loc="lower right", frameon=False, fontsize=9, ncol=4)
    _style(ax, "Rows per table, by medallion layer", f"{catalog} on {stamp}, after the first deploy runs")
    return _save(fig, "rows_by_layer.png")


def quarantine_by_rule(profile: str, wh: str, catalog: str, stamp: str) -> Path:
    rows = query(
        profile,
        wh,
        f"SELECT split(source_table, '[.]')[2] AS t, rule, count(*) FROM {catalog}.ops.quarantine "
        "LATERAL VIEW explode(failed_rules) x AS rule GROUP BY 1, 2 ORDER BY 3 DESC",
    )
    data = [(f"{r[0]} · {r[1]}", int(str(r[2]))) for r in rows]
    fig, ax = _figure(max(len(data), 3))
    ys = list(range(len(data)))[::-1]
    ax.barh(ys, [d[1] for d in data], color=SERIES, height=0.62)
    ax.set_yticks(ys, [d[0] for d in data])
    for y, d in zip(ys, data, strict=True):
        ax.text(d[1] + 0.3, y, str(d[1]), va="center", fontsize=8, color=INK)
    ax.set_xlabel("quarantined rows", color=INK_2, fontsize=9)
    total = sum(d[1] for d in data)
    ecb_tables = ("fx_rates", "yield_curve", "estr", "currencies")
    ecb = sum(d[1] for d in data if d[0].split(" · ")[0] in ecb_tables)
    _style(ax, "Quarantined rows by failed rule",
           f"{total} rule failures in {catalog}.ops.quarantine (cumulative), "
           f"{ecb} on ECB tables · {stamp}")  # fmt: skip
    return _save(fig, "quarantine_by_rule.png")


def freshness(profile: str, wh: str, catalog: str, stamp: str) -> Path:
    rows = query(
        profile,
        wh,
        f"SELECT dataset, fetch_lag_hours, sla_hours, within_sla FROM {catalog}.gold.dataset_freshness "
        "ORDER BY source, dataset",
    )
    data = [(str(r[0]), float(str(r[1])), float(str(r[2])), r[3] == "true") for r in rows]
    fig, ax = _figure(len(data))
    ys = list(range(len(data)))[::-1]
    for y, (_, lag, sla, ok) in zip(ys, data, strict=True):
        ax.plot([lag, sla], [y, y], color=GRID, linewidth=2, zorder=1)
        ax.scatter([sla], [y], marker="|", s=220, color=INK_2, zorder=2, linewidths=2)
        ax.scatter([lag], [y], s=60, color=SERIES, zorder=3, edgecolors=SURFACE, linewidths=2)
        label, color = ("✓ within SLA", GOOD) if ok else ("✗ late", CRITICAL)
        ax.text(sla * 1.6, y, label, va="center", fontsize=8, color=color)
        ax.text(lag / 1.6, y, f"{lag * 60:.0f} min", va="center", ha="right", fontsize=8, color=INK)
    ax.set_yticks(ys, [d[0] for d in data])
    ax.set_xscale("log")
    ax.set_xlim(min(d[1] for d in data) / 8, max(d[2] for d in data) * 8)
    ax.set_xlabel("hours since last successful fetch (dot) vs freshness SLA (tick), log scale",
                  color=INK_2, fontsize=9)  # fmt: skip
    _style(ax, "Freshness per landed dataset",
           f"lag at the last pipeline refresh, {catalog}.gold.dataset_freshness · {stamp}")  # fmt: skip
    return _save(fig, "freshness.png")


def lineage(profile: str, wh: str, catalog: str) -> Path:
    edges = query(
        profile,
        wh,
        "SELECT DISTINCT source_table_full_name, target_table_full_name FROM system.access.table_lineage "
        f"WHERE source_table_full_name LIKE '{catalog}.%' AND target_table_full_name LIKE '{catalog}.%' "
        "AND source_table_full_name <> target_table_full_name ORDER BY 1, 2",
    )

    # The pipeline event log is an internal table whose name embeds the pipeline id: show it as
    # one generic node instead.
    def clean(name: object) -> str:
        text = str(name)
        return f"{catalog}.ops.pipeline_event_log" if text.split(".")[2].startswith("event_log") else text

    pairs = sorted({(clean(s), clean(t)) for s, t in edges if not str(s).split(".")[2].startswith("__")})
    short = {}
    lines = ["flowchart LR"]
    for layer in ("bronze", "silver", "gold", "ops"):
        names = sorted({t for e in pairs for t in e if t.split(".")[1] == layer})
        if not names:
            continue
        lines.append(f"  subgraph {layer}")
        for name in names:
            node = name.split(".", 1)[1].replace(".", "_")
            short[name] = node
            lines.append(f'    {node}["{name.split(".")[2]}"]')
        lines.append("  end")
    lines += [
        f"  {short[str(s)]} --> {short[str(t)]}" for s, t in edges if str(s) in short and str(t) in short
    ]
    path = ROOT / "docs" / "lineage.mmd"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--catalog", default="mdl_dev")
    args = parser.parse_args(argv)
    wh = _warehouse(args.profile)
    stamp = datetime.now(UTC).strftime("%Y-%m-%d")
    for path in (
        rows_by_layer(args.profile, wh, args.catalog, stamp),
        quarantine_by_rule(args.profile, wh, args.catalog, stamp),
        freshness(args.profile, wh, args.catalog, stamp),
        lineage(args.profile, wh, args.catalog),
    ):
        print(path.relative_to(ROOT).as_posix())
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
