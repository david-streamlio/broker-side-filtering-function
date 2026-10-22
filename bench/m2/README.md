# M2: where the windowed read wins and where it loses

On the M1 topic, the same selector is read by a client scan and by a windowed read, for each window
size and selectivity:

| | 1 entity `__key__ = '8675309'` | ~1% `region = 'r07'` | all `region LIKE 'r%'` |
|---|---|---|---|
| last 1 min | | | |
| last 1 h | | | |
| last 24 h | | | |

The scan (`pulsar-scan` with `--selector` and `--window=between:…`) seeks to the window start, reads to
its end, and evaluates the selector on the client with the same engine the filter uses. Each windowed
read must return the scan's match count.

The generator's bulk mode squeezes a simulated day into a minute or two of broker time, so "last W
minutes" is the last W/1440 of the load's span on the log's clock. The windows therefore hold however
many events the load wrote in that slice; the result file records the exact counts.

```bash
# with the M1 topic loaded on the Apache bench broker (bench/m1/run_m1.py)
caffeinate -ims python3 bench/m2/run_m2.py --reps 3
python3 bench/m2/chart.py bench/out/m2/m2-<timestamp>.json
```
