#!/usr/bin/env python3
"""Chart an M2 result: client scan vs windowed read for each window size and selectivity. Median over
repetitions, whiskers min to max.

Usage: python3 bench/m2/chart.py <m2-result.json> [--out chart.png]
"""
import argparse
import json
import os
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

WINDOWS = ["1min", "1h", "24h"]
SELECTIVITY = ["entity", "1pct", "all"]
SEL_LABEL = {"entity": "1 entity", "1pct": "~1%", "all": "all"}
ARMS = [("scan", "scan", "#9aa5b1"), ("window", "window_read", "#2f6fdf")]
PANELS = [
    ("Time to complete (s)", lambda x: x["client"]["elapsedMs"], 1e3, True),
    ("Bytes to client (MB)", lambda x: x["wire_bytes_to_client"], 1e6, True),
    ("Broker CPU (s)", lambda x: x["broker_cpu_s"], 1, False),
]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("result")
    p.add_argument("--out", help="PNG path (default: next to the result file)")
    args = p.parse_args()
    with open(args.result) as f:
        data = json.load(f)
    env, runs = data["environment"], data["runs"]
    cells = [(w, s) for w in WINDOWS for s in SELECTIVITY
             if any(r["window"] == w and r["selectivity"] == s for r in runs)]

    fig, axes = plt.subplots(len(PANELS), 1, figsize=(12, 11))
    for ax, (title, value, scale, log) in zip(axes, PANELS):
        for i, (arm, key, color) in enumerate(ARMS):
            meds, lows, highs = [], [], []
            for w, s in cells:
                vals = sorted(v / scale for v in (value(r[key]) for r in runs
                                                  if r["window"] == w and r["selectivity"] == s)
                              if v is not None and v > 0)
                mid = vals[len(vals) // 2] if vals else 0
                meds.append(mid)
                lows.append(mid - vals[0] if vals else 0)
                highs.append(vals[-1] - mid if vals else 0)
            xs = [j + (i - 0.5) * 0.4 for j in range(len(cells))]
            bars = ax.bar(xs, meds, 0.4, yerr=[lows, highs], capsize=3, color=color, label=arm)
            for bar, mid in zip(bars, meds):
                if mid:
                    label = f"{mid:,.0f}" if mid >= 100 else f"{mid:.3g}"
                    ax.annotate(label, (bar.get_x() + bar.get_width() / 2, bar.get_height()), ha="center",
                                va="bottom", fontsize=7, xytext=(0, 2), textcoords="offset points")
        ax.set_title(title, fontsize=10)
        ax.set_xticks(range(len(cells)), [f"last {w}\n{SEL_LABEL[s]}" for w, s in cells], fontsize=8)
        if log:
            ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=8)

    reps = max(r["rep"] for r in runs)
    fig.suptitle(f"M2: client scan vs windowed read, by window and selectivity ({env['events']:,} events)",
                 fontsize=12)
    footer = (f"median of {reps} runs, whiskers min–max; log scales for time and bytes. Windows are the last "
              f"1/1440, 1/24 and all of the load's span on the log's clock (a simulated day). 1 entity: "
              f"__key__ = '8675309'; ~1%: region = 'r07'; all: region LIKE 'r%'. "
              f"{'Every' if data['all_correct'] else 'NOT every'} windowed read returned the scan's match count. "
              f"The windowed read's output is LZ4-compressed (the Functions runtime's default) while the source "
              f"topic isn't, so when the selector matches everything its bytes are well below the records' size. "
              f"Broker {env['broker_image']}, Docker {env['docker_cpus']} CPU / "
              f"{env['docker_mem_bytes'] / 2**30:.0f} GiB ({env['docker_arch']}), repo {env['repo_sha']}"
              f"{' (dirty)' if env['repo_dirty'] else ''}.")
    fig.text(0.5, 0.01, textwrap.fill(footer, 170), ha="center", fontsize=8, color="#555")
    fig.tight_layout(rect=(0, 0.06, 1, 0.96))
    out = args.out or os.path.splitext(args.result)[0] + ".png"
    fig.savefig(out, dpi=150)
    print(out)


if __name__ == "__main__":
    main()
