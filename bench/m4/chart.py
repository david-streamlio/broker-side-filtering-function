#!/usr/bin/env python3
"""Chart an M4 function-arm result: the filter's CPU per message and its throughput, by selector type
(key, property, JSON field) and compression (none, lz4, zstd). Median over repetitions, whiskers min to
max.

Usage: python3 bench/m4/chart.py <m4-function-result.json> [--out chart.png]
"""
import argparse
import json
import os
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SELECTORS = ["key", "property", "json"]
LABELS = {"key": "key\n__key__ = '…'", "property": "property\norder_id = '…'", "json": "JSON field\ncarrier = '…'"}
COMPRESSIONS = ["none", "lz4", "zstd"]
COLORS = {"none": "#9aa5b1", "lz4": "#2f6fdf", "zstd": "#123a8f"}


def stats(values):
    values = sorted(v for v in values if v is not None)
    if not values:
        return 0, 0, 0
    mid = values[len(values) // 2]
    return mid, mid - values[0], values[-1] - mid


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("result")
    p.add_argument("--out", help="PNG path (default: next to the result file)")
    args = p.parse_args()
    with open(args.result) as f:
        data = json.load(f)
    env, runs = data["environment"], data["runs"]
    comps = [c for c in COMPRESSIONS if any(r["compression"] == c for r in runs)]
    sels = [s for s in SELECTORS if any(r["selector_type"] == s for r in runs)]

    panels = [
        ("Filter CPU per message (µs)", lambda r: r["filter_cpu_us_per_msg"]),
        ("Filter throughput (k msg/s)",
         lambda r: r["read"]["client"]["windowStats"]["scanned"] / r["read"]["client"]["windowStats"]["elapsedMs"]),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    width = 0.8 / len(comps)
    for ax, (title, value) in zip(axes, panels):
        for i, comp in enumerate(comps):
            meds, lows, highs = [], [], []
            for sel in sels:
                mid, lo, hi = stats(value(r) for r in runs if r["compression"] == comp and r["selector_type"] == sel)
                meds.append(mid)
                lows.append(lo)
                highs.append(hi)
            xs = [j + (i - (len(comps) - 1) / 2) * width for j in range(len(sels))]
            bars = ax.bar(xs, meds, width, yerr=[lows, highs], capsize=3, color=COLORS[comp], label=comp)
            for bar, mid in zip(bars, meds):
                ax.annotate(f"{mid:.1f}", (bar.get_x() + bar.get_width() / 2, bar.get_height()), ha="center",
                            va="bottom", fontsize=8, xytext=(0, 2), textcoords="offset points")
        ax.set_title(title, fontsize=10)
        ax.set_xticks(range(len(sels)), [LABELS[s] for s in sels], fontsize=8)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(title="compression", fontsize=8, title_fontsize=8)

    reps = max(r["rep"] for r in runs)
    fig.suptitle(f"M4, function arm: what the selector costs the filter ({env['events_per_topic']:,} events per "
                 f"topic, batched)", fontsize=12)
    footer = (f"median of {reps} runs, whiskers min–max; CPU per message is the slope of the filter JVM's CPU over "
              f"10–90% of each scan. {'All' if data['all_correct'] else 'NOT all'} runs returned the expected "
              f"matches. Broker {env['broker_image']}, Docker {env['docker_cpus']} CPU / "
              f"{env['docker_mem_bytes'] / 2**30:.0f} GiB ({env['docker_arch']}), repo {env['repo_sha']}"
              f"{' (dirty)' if env['repo_dirty'] else ''}.")
    fig.text(0.5, 0.01, textwrap.fill(footer, 160), ha="center", fontsize=8, color="#555")
    fig.tight_layout(rect=(0, 0.06, 1, 0.94))
    out = args.out or os.path.splitext(args.result)[0] + ".png"
    fig.savefig(out, dpi=150)
    print(out)


if __name__ == "__main__":
    main()
