# GitHub Challenge

<img src="https://octodex.github.com/images/Professortocat_v2.png" align="right" height="200px" />

This project is a simple AIOps exercise built around a payment service that is generating synthetic operational data. The goal is to look at the metrics and logs, decide what looks normal, flag what looks wrong, and then make sure those anomalies flow through a basic event pipeline the way a real monitoring system would.

It is intentionally small, but it covers the core pattern you want to see: healthy service behaviour, a clear degradation window, and a working event flow that carries the anomaly through the system.

## What the data shows

The operational data is stored in [data/service_data.json](data/service_data.json). Each record represents one minute of service activity for the `payment-service`.

Each entry includes:

- `timestamp`: when the reading was captured
- `service`: which service emitted it
- `response_time_ms`: how long a request took
- `cpu_percent`: CPU usage
- `memory_percent`: memory usage
- `log_level`: the log severity (`INFO` or `ERROR`)
- `message`: the text describing what happened

The timestamps are ordered by minute, so the timeline is easy to follow. The first few records look healthy, the middle records show a clear issue, and the later records recover back to normal.

## Normal vs unusual behaviour

From the beginning of the timeline and then again near the end, the service looks healthy:

- response times sit around 120-150 ms
- CPU usage stays around 42-50%
- memory stays around 51-57%
- the logs are `INFO` with messages like "Payment request processed successfully"

That is the normal baseline.

The unusual behaviour is easy to spot in the middle of the dataset:

- `2026-09-20T10:05:00`: response time hits 610 ms, CPU reaches 75%, memory reaches 70%, and the log is `ERROR` with "Payment service timeout"
- `2026-09-20T10:06:00`: response time hits 640 ms, CPU reaches 94%, memory reaches 91%, and the log says `ERROR` with "Database connection timeout"

Those two moments are clearly not normal. They show degraded service health and a likely backend issue.

## Which fields are metrics vs logs

The metric fields are:

- `response_time_ms`
- `cpu_percent`
- `memory_percent`

The log-related fields are:

- `log_level`
- `message`
- `service`

Together they tell the same story from two angles: the service is slowing down and the logs confirm the failure mode.

## Anomaly detection result

The detector in [src/anomaly_detector.py](src/anomaly_detector.py) is designed to flag records when a metric exceeds the configured threshold or a concerning error log appears. In this dataset, it detects two anomalies.

### Detected anomalies

1. `2026-09-20T10:05:00` — `payment-service`
   - response time: 610 ms
   - CPU: 75%
   - memory: 70%
   - log: `ERROR` — "Payment service timeout"
   - reasons: `High response time`, `Error log detected`

2. `2026-09-20T10:06:00` — `payment-service`
   - response time: 640 ms
   - CPU: 94%
   - memory: 91%
   - log: `ERROR` — "Database connection timeout"
   - reasons: `High response time`, `High CPU utilization`, `High memory utilization`, `Error log detected`

There were no missed expected anomalies in this dataset, and no normal records were incorrectly flagged.

## Event-flow check

The event pipeline in [src/aiops_pipeline.py](src/aiops_pipeline.py) follows the basic AIOps flow:

1. read operational data
2. detect anomalies
3. generate anomaly events
4. publish them with the producer
5. send them to the topic
6. consume them with the consumer
7. surface the result in the downstream output

The components involved are:

- `EventProducer`: publishes the anomaly event
- `EventTopic`: stores the event in memory
- `EventConsumer`: reads the event from the topic
- `Event/message`: the payload being passed through the pipeline

This part of the project is working as intended in the final version.

## Final workflow result

I ran the pipeline and got this output:

- `Records processed: 10`
- `Anomalies detected: 2`
- `Events consumed: 2`

That confirms the complete flow is working end to end: operational data is processed, the abnormal behaviour is detected, an anomaly event is emitted, the event is published, the consumer receives it, and the downstream workflow reports the issue.

## Issues I found and corrected

I fixed a few problems while validating the workflow:

1. The project import path was not set up correctly for pytest.
   - fix: added [src/__init__.py](src/__init__.py) and [pytest.ini](pytest.ini)

2. The detector was checking for `WARNING` instead of `ERROR`.
   - fix: normalize the log value and flag `ERROR` correctly in [src/anomaly_detector.py](src/anomaly_detector.py)

3. The producer and consumer were not using the same topic.
   - fix: align them to the same event stream in [src/aiops_pipeline.py](src/aiops_pipeline.py)

4. The script was not resolving the data file reliably from the repo root.
   - fix: resolve paths relative to the project root so direct execution works consistently

## One limitation

The approach uses fixed thresholds for latency, CPU, and memory. That is fine for obvious spikes, but it can miss gradual degradation that does not immediately cross a threshold. A good next step would be to add baseline-based detection, like rolling averages or z-scores, so the service can spot drift before it becomes a full outage.

## How to reproduce it

Run these commands from the project root:

1. `pytest -q`
2. `python src/aiops_pipeline.py`

The expected result is that the tests pass and the pipeline reports 2 anomalies for the timeout and high-resource period.

---

&copy; 2025 GitHub &bull; [Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md) &bull; [MIT License](https://gh.io/mit)

