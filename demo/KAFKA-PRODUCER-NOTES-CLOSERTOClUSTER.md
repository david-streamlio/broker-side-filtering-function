# Kafka producer notes — toolset pod & local

Goal: produce stock-quote messages into the **source** topic
`public/default/stock-quotes` with a distinctive key (`LOCAL`), then watch a
broker-side filter redirect exactly those messages to a private output topic.

Two producers, same message schema (mirrors `shadowtraffic/stock-quotes.json`):

| Script | Runs on | Bootstrap | Protocol | Kafka lib |
|---|---|---|---|---|
| `kafka-producer.py` | this laptop | `pc-fa160a40.gcp-shared-usce1.g.snio.cloud:9093` | `SASL_SSL` | `confluent-kafka` |
| `kafka-producer-pod.py` | `stock-demo-toolset-0` | `stock-demo-broker:9092` | `SASL_PLAINTEXT` | `kafka-python-ng` |

Both key every record `LOCAL` (override `--key`), so they stand out from the
shadowtraffic ticker-keyed traffic (AAPL, MSFT, …).

---

## TL;DR — launch another producer on the toolset

Use the wrapper `launch-toolset-producer.sh`. It picks the pod, ensures
`kafka-python` + the script are on it, then runs the producer.

```bash
./launch-toolset-producer.sh                       # auto-pick toolset pod, 10 msgs, key=LOCAL
./launch-toolset-producer.sh -n 0 -t 200           # continuous @ 5 msg/s (Ctrl-C to stop)
./launch-toolset-producer.sh -k DESK7 -n 0 -t 50   # custom key, faster
./launch-toolset-producer.sh -d -n 0               # detached inside the pod (survives your shell)
```

**Choosing the toolset pod** (highest precedence first):

```bash
./launch-toolset-producer.sh -p stock-demo-toolset-0     # 1. explicit flag
export TOOLSET_POD=stock-demo-toolset-0                  # 2. env var
export POD_PATTERN=toolset                               # 3. auto-detect substring (default)
```

Also configurable: `-c CONTAINER` or `$TOOLSET_CONTAINER` (default `pulsar`).
If several pods match the pattern and none is pinned, the script prompts you to pick.

Flags: `-p` pod · `-c` container · `-n` count (0=forever) · `-k` key · `-t` throttle-ms · `-d` detached · `-h` help.

### Raw equivalent (no wrapper)

```bash
# 1. (only if the pod was recreated) install lib + copy the script
kubectl exec stock-demo-toolset-0 -c pulsar -- \
  pip3 install --break-system-packages kafka-python-ng
kubectl cp kafka-producer-pod.py stock-demo-toolset-0:/tmp/kafka-producer-pod.py -c pulsar

# 2. run it — continuous LOCAL stream at 5 msg/s
kubectl exec stock-demo-toolset-0 -c pulsar -- \
  python3 /tmp/kafka-producer-pod.py -n 0 --throttle-ms 200 --key LOCAL
```

Run it under `kubectl exec` in a dedicated terminal (it streams until Ctrl-C),
or detach it inside the pod so it survives your shell:

```bash
kubectl exec stock-demo-toolset-0 -c pulsar -- \
  bash -c 'nohup python3 /tmp/kafka-producer-pod.py -n 0 --throttle-ms 200 --key LOCAL \
           > /tmp/producer-LOCAL.log 2>&1 & echo "pid $!"'
```

Check / stop a detached producer on the pod:

```bash
kubectl exec stock-demo-toolset-0 -c pulsar -- bash -c 'ps -ef | grep [k]afka-producer-pod'
kubectl exec stock-demo-toolset-0 -c pulsar -- pkill -f kafka-producer-pod   # stop all
kubectl exec stock-demo-toolset-0 -c pulsar -- tail -f /tmp/producer-LOCAL.log
```

You can run **several producers at once** with different `--key` values
(e.g. `LOCAL`, `DESK7`) and register a filter per key to watch each stream
land in its own output topic.

---

## Message schema (value JSON + headers)

```
value: { seq, exchange, sector, price, bid, ask, bidSize, askSize, spread, volume, side }
headers: exchange, sector, side
key: "LOCAL"
```

`exchange ∈ {NYSE,NASDAQ}`, `sector ∈ {tech,finance,healthcare,consumer,energy,industrial}`,
`side ∈ {B,S}`.

---

## A. Run the producer ON the toolset pod (in-cluster KoP)

The pod uses its mounted broker token at `/mnt/client/token` and the internal
KoP listener `stock-demo-broker:9092` (SASL_PLAINTEXT, no TLS in-cluster).

One-time setup on the pod:

```bash
kubectl exec stock-demo-toolset-0 -c pulsar -- \
  pip3 install --break-system-packages kafka-python-ng    # confluent-kafka has no wheel here
kubectl cp kafka-producer-pod.py stock-demo-toolset-0:/tmp/kafka-producer-pod.py -c pulsar
```

Produce:

```bash
# 10 messages
kubectl exec stock-demo-toolset-0 -c pulsar -- python3 /tmp/kafka-producer-pod.py -n 10

# continuous (5 msg/s), Ctrl-C / kill to stop
kubectl exec stock-demo-toolset-0 -c pulsar -- \
  python3 /tmp/kafka-producer-pod.py -n 0 --throttle-ms 200 --key LOCAL
```

Env overrides honoured by the pod script: `KAFKA_BOOTSTRAP`,
`KAFKA_SECURITY_PROTOCOL`, `KAFKA_USERNAME`, `PULSAR_TOKEN_PATH`,
`DEMO_SOURCE_TOPIC_STOCKS`. Defaults already target the in-cluster broker.

> Verified working: `SASL_PLAINTEXT` + `sasl.username=stock-demo-app` +
> `sasl.password=token:<token>`, 3/3 delivered on first try.

## B. Run the producer LOCALLY (public cloud KoP endpoint)

Reads connection from `demo/.env` (`KAFKA_BOOTSTRAP=…:9093`, `KAFKA_USERNAME`,
`PULSAR_TOKEN_PATH=infra/terraform/app.token`) — same wiring as
`kafka-consumer.py`. Default protocol `SASL_SSL`.

```bash
pip install confluent-kafka          # already present locally (v2.14.0)
python3 kafka-producer.py -n 10                       # 10 msgs, key=LOCAL
python3 kafka-producer.py -n 0 --throttle-ms 200      # continuous
python3 kafka-producer.py --key LOCAL --topic public/default/stock-quotes
```

---

## C. Create the broker-side filter for key=LOCAL (run on this laptop)

`live-query-demo.sh` registers a selector with the orchestrator (needs the
observability service at `http://localhost:3200`) and returns a private output
topic that receives only matching messages.

```bash
./live-query-demo.sh "__key__ = 'LOCAL'"
```

Returns e.g. `outputTopic: persistent://public/default/live-demo-<id>` and
reports time-to-first-message. Clean up with the printed
`curl -X DELETE http://localhost:3200/api/query/<queryId>`.

## D. Watch the filtered output (run on this laptop)

```bash
python3 kafka-consumer.py public/default/live-demo-<id> 10
```

---

## End-to-end verified run (2026-06-03)

1. Continuous LOCAL producer on `stock-demo-toolset-0` → `public/default/stock-quotes`.
2. `./live-query-demo.sh "__key__ = 'LOCAL'"` → output topic
   `public/default/live-demo-ecbc0424`, filter active in 875ms,
   first message in 1856ms.
3. `kafka-consumer.py public/default/live-demo-ecbc0424` → all records key=`LOCAL`,
   full schema + headers, ~68ms mean transit.
```
key=LOCAL  side=S exchange=NYSE sector=consumer  seq=243 price=397.47 volume=34.6M bid=455.65 ask=172.54
```

## Gotchas

- `confluent-kafka` won't `pip install` on the toolset pod (no manylinux wheel
  for its Python; would compile librdkafka from source) — use `kafka-python-ng`.
- pip on the pod needs `--break-system-packages` (PEP 668 externally-managed env).
- In-cluster listener is **plaintext+SASL** (`SASL_PLAINTEXT`), not `SASL_SSL`.
  The public endpoint (`:9093`) is `SASL_SSL`.
- KoP topic naming is the full `tenant/namespace/topic` (e.g.
  `public/default/stock-quotes`), no `persistent://` prefix for the Kafka client.
