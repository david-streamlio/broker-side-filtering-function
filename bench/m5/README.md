# M5: failure modes of a windowed read

Demos on the M1 topic, each reading the whole topic with the key selector `__key__ LIKE '%07'` (about 1%
of orders, several events each) and checking the result against the generator's per-order counts:

| Demo | What happens | What the client sees |
|---|---|---|
| a | receives return nothing before the completion record arrives | empty receives don't mean done; only the completion record does |
| b | the consumer pauses 90 s with a 10 s private-topic TTL | the broker expires the results and the completion record: only what the client had already prefetched (about 1,000 records) arrives, and the window never completes. With the windowed default TTL (1 h) the same pause loses nothing |
| c | the filter's JVM is killed (`kill -9`) a quarter of the way through its scan; the functions worker restarts it | matches whose acknowledgement hadn't been flushed arrive twice; deduplicating on `__src_msgid__` gives exactly the expected count. (A graceful restart flushes acknowledgements and produces none.) |
| d | (from a) per-key order on one partition | no order's `seq` goes backwards (duplicates by source id excluded) |

The 10 s TTL stands in for the 300 s default of live queries' private topics. Demo b needs the broker to
enforce TTLs often: `bench/docker-compose.yml` checks every minute instead of every five.

```bash
docker compose -f bench/docker-compose.yml up -d     # the M1 topic loaded (bench/m1/run_m1.py)
python3 bench/m5/run_m5.py
```

The bench client options these demos use: `--pause-ms` (wait after subscribing), `--private-ttl-seconds`
(the windowed private topics' TTL), `--give-up-ms` (stop waiting for the completion record and report
what arrived). Its result reports `distinctSourceIds`, `orderViolations` and
`emptyReceivesBeforeComplete`.
