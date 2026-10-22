#!/usr/bin/env python3
"""Chart an M4 broker-arm result: what a subscription with the selector EntryFilter delivers, and what
it costs the broker, by topic layout (unbatched / batched, compression) and selector. Median over
repetitions, whiskers min to max.

Usage: python3 bench/m4/chart_broker.py <m4-broker-result.json> [--out chart.png]
"""
import argparse
import json
import os
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

TOPICS = ["unbatched-none", "unbatched-lz4", "batched-lz4"]
SELECTORS = ["none", "key", "property"]
COLORS = {"none": "#9aa5b1", "key": "#2f6fdf", "property": "#123a8f"}
PANELS = [
    ("Records to client", lambda r: r["client"]["recordsReceived"], 1, True),
    ("Bytes to client (MB)", lambda r: r["client"]["subscriptionBytesOut"], 1e6, True),
    ("Broker CPU (s)", lambda r: r["broker_cpu_s"], 1, False),
    ("EntryFilter evaluation (ns per entry)", lambda r: r["eval_ns_per_entry"], 1, False),
]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("result")
    p.add_argument("--out", help="PNG path (default: next to the result file)")
    args = p.parse_args()
    with open(args.result) as f:
        data = json.load(f)
    env, runs = data["environment"], data["runs"]
    topics = [t for t in TOPICS if any(r["topic_label"] == t for r in runs)]
    sels = [s for s in SELECTORS if any(r["selector_type"] == s for r in runs)]
    width = 0.8 / len(sels)

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    for ax, (title, value, scale, log) in zip(axes.flat, PANELS):
        for i, sel in enumerate(sels):
            meds, lows, highs = [], [], []
            for topic in topics:
                vals = sorted(v / scale for v in (value(r) for r in runs
                                                  if r["topic_label"] == topic and r["selector_type"] == sel)
                              if v is not None)
                mid = vals[len(vals) // 2] if vals else 0
                meds.append(mid)
                lows.append(mid - vals[0] if vals else 0)
                highs.append(vals[-1] - mid if vals else 0)
            xs = [j + (i - (len(sels) - 1) / 2) * width for j in range(len(topics))]
            bars = ax.bar(xs, meds, width, yerr=[lows, highs], capsize=3, color=COLORS[sel], label=sel)
            for bar, mid in zip(bars, meds):
                if mid:
                    label = f"{mid:,.0f}" if mid >= 10 else f"{mid:.2g}"
                    ax.annotate(label, (bar.get_x() + bar.get_width() / 2, bar.get_height()), ha="center",
                                va="bottom", fontsize=7, xytext=(0, 2), textcoords="offset points")
        ax.set_title(title, fontsize=10)
        ax.set_xticks(range(len(topics)), topics, fontsize=8)
        if log:
            ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3)
        ax.legend(title="selector", fontsize=8, title_fontsize=8)

    reps = max(r["rep"] for r in runs)
    fig.suptitle(f"M4, broker arm: the selector EntryFilter ({env['events_per_topic']:,} events per topic)",
                 fontsize=12)
    footer = (f"median of {reps} runs, whiskers min–max. key: __key__ = '8675309'; property: region = 'r07'. "
              f"Batched entries pass the EntryFilter unfiltered (their keys and properties are inside the "
              f"payload). {'All' if data['all_correct'] else 'NOT all'} runs delivered the expected records. "
              f"Broker {env['broker_image']}, Docker {env['docker_cpus']} CPU / "
              f"{env['docker_mem_bytes'] / 2**30:.0f} GiB ({env['docker_arch']}), repo {env['repo_sha']}"
              f"{' (dirty)' if env['repo_dirty'] else ''}.")
    fig.text(0.5, 0.01, textwrap.fill(footer, 170), ha="center", fontsize=8, color="#555")
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    out = args.out or os.path.splitext(args.result)[0] + ".png"
    fig.savefig(out, dpi=150)
    print(out)


if __name__ == "__main__":
    main()
