#!/usr/bin/env python3
"""Chart an M3 result: the window -> ledger timeline, and what each read cost in MinIO GETs, bytes and
time, for the windowed read and the client scan.

Usage: python3 bench/m3/chart.py <m3-result.json> [--out chart.png]
"""
import argparse
import json
import os
import textwrap

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

LABELS = ["cold", "cold-again", "hot", "all"]
LABEL_TEXT = {"cold": "cold window", "cold-again": "cold window,\nagain (cache)", "hot": "hot window",
              "all": "whole topic"}
ARMS = [("pulsar-window", "windowed read", "#2f6fdf"), ("pulsar-scan", "client scan", "#9aa5b1")]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("result")
    p.add_argument("--out", help="PNG path (default: next to the result file)")
    args = p.parse_args()
    with open(args.result) as f:
        data = json.load(f)
    env, timeline, reads = data["environment"], data["timeline"], data["reads"]

    fig = plt.figure(figsize=(13, 10))
    grid = fig.add_gridspec(2, 3, height_ratios=[1, 1.2])

    # Timeline: one bar per closed ledger at its close time; windows shaded.
    ax = fig.add_subplot(grid[0, :])
    t0 = timeline[0]["timestamp"]
    xs = [(l["timestamp"] - t0) / 1000 for l in timeline]
    width = (xs[-1] - xs[0]) / max(len(xs), 1) * 0.8 if len(xs) > 1 else 1
    colors = ["#123a8f" if l["offloaded"] else "#7fb069" for l in timeline]
    ax.bar(xs, [(l["size"] or 0) / 1e6 for l in timeline], width=width, color=colors)
    handles = [Patch(color="#123a8f", label="ledger offloaded to MinIO"), Patch(color="#7fb069", label="ledger in BookKeeper")]
    for name, (lo, hi), shade in (("cold window", data["windows"]["cold"], "#2f6fdf"),
                                  ("hot window", data["windows"]["hot"], "#e07a5f")):
        ax.axvspan((lo - t0) / 1000, (hi - t0) / 1000, color=shade, alpha=0.2)
        handles.append(Patch(color=shade, alpha=0.2, label=name))
    ax.set_title(f"{len(timeline)} closed ledgers by close time ({sum(l['offloaded'] for l in timeline)} offloaded, "
                 f"keep {env['keep']} in BookKeeper)", fontsize=10)
    ax.set_xlabel("seconds since the first ledger closed (broker time)", fontsize=8)
    ax.set_ylabel("ledger size (MB)", fontsize=8)
    ax.legend(handles=handles, fontsize=8, loc="upper left")

    panels = [("MinIO GETs", lambda r: r["minio"]["gets"]), ("MinIO bytes sent (MB)", lambda r: r["minio"]["sent_bytes"] / 1e6),
              ("Time to complete (s)", lambda r: r["client"]["elapsedMs"] / 1000)]
    by = {(r["label"], r["arm"]): r for r in reads}
    labels = [lbl for lbl in LABELS if any(r["label"] == lbl for r in reads)]
    for i, (title, value) in enumerate(panels):
        ax = fig.add_subplot(grid[1, i])
        for j, (arm, arm_label, color) in enumerate(ARMS):
            vals = [value(by[(lbl, arm)]) if (lbl, arm) in by else 0 for lbl in labels]
            pos = [k + (j - 0.5) * 0.38 for k in range(len(labels))]
            bars = ax.bar(pos, vals, 0.38, color=color, label=arm_label)
            for bar, v in zip(bars, vals):
                if v:
                    ax.annotate(f"{v:,.0f}" if v >= 10 else f"{v:.2g}", (bar.get_x() + bar.get_width() / 2, v),
                                ha="center", va="bottom", fontsize=7, xytext=(0, 2), textcoords="offset points")
        ax.set_title(title, fontsize=10)
        ax.set_xticks(range(len(labels)), [LABEL_TEXT[lbl] for lbl in labels], fontsize=7)
        ax.grid(axis="y", alpha=0.3)
        if i == 0:
            ax.legend(fontsize=8)

    ret = data.get("retention") or {}
    retention_text = (f"Retention: a window starting before the oldest retained message scanned "
                      f"{ret['scanned']:,} of {ret['events']:,} events and reported {ret['status']}. "
                      if "status" in ret else "")
    fig.suptitle(f"M3: windows on hot vs cold storage ({env['events']:,} unbatched events, 2,000 per ledger)",
                 fontsize=12)
    footer = (f"Each cold read starts from a broker restart (empty caches); 'again' reruns the same window right "
              f"after. MinIO counters are sampled until they settle. {retention_text}"
              f"{'All' if data['all_correct'] else 'NOT all'} checks passed. Broker {env['broker_image']} runs "
              f"emulated (amd64 on {env['docker_arch']}) and local MinIO has no per-request latency, so cold cost "
              f"shows as requests and bytes, not time. Repo {env['repo_sha']}{' (dirty)' if env['repo_dirty'] else ''}.")
    fig.text(0.5, 0.01, textwrap.fill(footer, 175), ha="center", fontsize=8, color="#555")
    fig.tight_layout(rect=(0, 0.06, 1, 0.95))
    out = args.out or os.path.splitext(args.result)[0] + ".png"
    fig.savefig(out, dpi=150)
    print(out)


if __name__ == "__main__":
    main()
