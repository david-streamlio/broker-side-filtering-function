#!/usr/bin/env python3
"""Chart an M1 result file: client-side scan vs windowed read, per protocol.

Six panels, one bar per arm (median over repetitions, whiskers min to max):
bytes and records to the client (log scale), time to complete, bytes the broker dispatched from the
source topic, broker CPU, and client connections. The point of the last three: the client's cost
shrinks, the server's reads don't.

Pass several result files (e.g. the Pulsar arms from one broker, the Kafka arms from the KoP broker)
to chart them together.

Usage: python3 bench/m1/chart.py <m1-result.json> [<m1-result.json> ...] [--out chart.png]
"""
import argparse
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ARM_ORDER = ["pulsar-scan", "pulsar-window", "kafka-scan", "kafka-window"]
COLORS = {"scan": "#9aa5b1", "window": "#2f6fdf"}

PANELS = [
    # title, value from a run, unit scale, log scale
    ("Bytes to client (MB)", lambda r: r["wire_bytes_to_client"], 1e6, True),
    ("Records to client", lambda r: r["client"]["recordsReceived"], 1, True),
    ("Time to complete (s)", lambda r: r["client"]["elapsedMs"], 1e3, False),
    ("Broker: bytes read from source (MB)", lambda r: r["broker"]["source_bytes_out"], 1e6, False),
    ("Broker CPU (s)", lambda r: r["broker"]["cpu_s"], 1, False),
    ("Client TCP connections", lambda r: r["connections"]["max_established"], 1, False),
]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("results", nargs="+")
    p.add_argument("--out", help="PNG path (default: next to the first result file)")
    args = p.parse_args()
    envs, runs = [], []
    for path in args.results:
        with open(path) as f:
            data = json.load(f)
        envs.append(data["environment"])
        runs.extend(data["runs"])
    env = envs[0]
    arms = [a for a in ARM_ORDER if any(r["arm"] == a for r in runs)]

    fig, axes = plt.subplots(2, 3, figsize=(13, 7.5))
    for ax, (title, value, scale, log) in zip(axes.flat, PANELS):
        medians, lows, highs = [], [], []
        for arm in arms:
            values = sorted(v / scale for v in (value(r) for r in runs if r["arm"] == arm) if v is not None)
            mid = values[len(values) // 2] if values else 0
            medians.append(mid)
            lows.append(mid - values[0] if values else 0)
            highs.append(values[-1] - mid if values else 0)
        colors = [COLORS["window" if a.endswith("window") else "scan"] for a in arms]
        bars = ax.bar(range(len(arms)), medians, yerr=[lows, highs], color=colors, capsize=4)
        integer = title.startswith(("Records", "Client TCP"))
        for bar, mid in zip(bars, medians):
            label = (f"{mid:,.0f}" if integer or mid >= 100 else f"{mid:,.2f}" if mid >= 0.01 else f"{mid:.2g}")
            ax.annotate(label, (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        ha="center", va="bottom", fontsize=8, xytext=(0, 2), textcoords="offset points")
        ax.set_title(title, fontsize=10)
        ax.set_xticks(range(len(arms)), [a.replace("-", "\n") for a in arms], fontsize=8)
        if log:
            ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.3)

    reps = max(r["rep"] for r in runs)
    correct = all(r["checks"]["ok"] for r in runs)
    brokers = ", ".join(dict.fromkeys(e["broker_image"] for e in envs))
    shas = ", ".join(dict.fromkeys(e["repo_sha"] + (" (dirty)" if e["repo_dirty"] else "") for e in envs))
    fig.suptitle(f"M1: order {env['key']}, last {env['lookback_ms'] / 3_600_000:g} h of {env['events']:,} events "
                 f"({env['profile']}) — scan vs windowed read", fontsize=12)
    fig.text(0.5, 0.01,
             f"median of {reps} runs, whiskers min–max; every run {'returned' if correct else 'did NOT return'} "
             f"exactly the order's events in order. Brokers {brokers}; Docker "
             f"{env['docker_cpus']} CPU / {env['docker_mem_bytes'] / 2**30:.0f} GiB ({env['docker_arch']}); "
             f"repo {shas}."
             + ("\nsn-platform runs emulated (amd64 on arm64): compare time and CPU only between the Kafka arms."
                if env["docker_arch"] == "aarch64" and any("sn-platform" in e["broker_image"] for e in envs) else ""),
             ha="center", fontsize=8, color="#555")
    fig.tight_layout(rect=(0, 0.03, 1, 0.95))
    out = args.out or os.path.splitext(args.results[0])[0] + ".png"
    fig.savefig(out, dpi=150)
    print(out)


if __name__ == "__main__":
    main()
