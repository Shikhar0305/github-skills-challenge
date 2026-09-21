# GitHub Challenge

<img src="https://octodex.github.com/images/Professortocat_v2.png" align="right" height="200px" />

Hey there!

Your challenge is ready.
Follow the instructions provided for this challenge and complete the required tasks in this repository.

Make sure your work is committed and pushed to your repository before submission.

Good luck!

## Operational Data Analysis

The repository includes a small synthetic operational dataset in [data/service_data.json](data/service_data.json). Each record represents one minute of service activity for the `payment-service`.

### 1. Fields that represent metrics

The numerical fields that represent operational metrics are:

- `response_time_ms`: request latency in milliseconds
- `cpu_percent`: CPU usage percentage
- `memory_percent`: memory consumption percentage

These values indicate runtime performance and resource consumption over time.

### 2. Fields that represent log information

The textual fields that represent log information are:

- `log_level`: severity (`INFO` or `ERROR`)
- `message`: the log message text describing the event
- `service`: indicates which service produced the event

Together, these fields describe what happened and how serious it was.

### 3. How timestamps are used

The `timestamp` field uses ISO 8601 timestamps in the format `YYYY-MM-DDTHH:MM:SS` and is recorded once per minute.

This gives a consistent sequence of events:

- observations occur at 10:00 through 10:09
- the timestamps allow us to identify a time-ordered timeline
- the pattern shows a normal period, a brief abnormal period, and then a recovery period

### 4. Observations that appear normal

The normal behaviour appears in the records from `2026-09-20T10:00:00` through `2026-09-20T10:04:00`, and again from `2026-09-20T10:07:00` through `2026-09-20T10:09:00`.

Typical normal signals include:

- `log_level` = `INFO`
- `message` = "Payment request processed successfully"
- `response_time_ms` stays between 120 and 150 ms
- `cpu_percent` stays roughly between 42% and 50%
- `memory_percent` stays roughly between 51% and 57%

This pattern reflects stable request handling with moderate resource use.

### 5. Observations that appear unusual

The unusual behaviour is concentrated in the two error events at:

- `2026-09-20T10:05:00`: `response_time_ms` = 610, `cpu_percent` = 75, `memory_percent` = 70, `log_level` = `ERROR`, message = "Payment service timeout"
- `2026-09-20T10:06:00`: `response_time_ms` = 640, `cpu_percent` = 94, `memory_percent` = 91, `log_level` = `ERROR`, message = "Database connection timeout"

These observations stand out because they show:

- a sharp jump in latency from around 120-150 ms to 610-640 ms
- elevated CPU and memory usage
- error-level log entries indicating service degradation or backend connection failure
- a clear incident window followed by a return to normal operation in the next minutes

In summary, the data shows a stable baseline followed by a short but significant outage or performance spike, then recovery to normal operating conditions.

## Anomaly Detection Validation

I validated the provided detection workflow using the repository’s service data and the built-in detector in [src/anomaly_detector.py](src/anomaly_detector.py). The pipeline was run via [src/aiops_pipeline.py](src/aiops_pipeline.py), which processes the dataset, emits anomaly events to the in-memory topic, and consumes the same events back for reporting.

### Detection result

The pipeline processed 10 records and detected 2 anomalies.

Detected anomalies:

1. `2026-09-20T10:05:00` — `payment-service`
   - Metrics: `response_time_ms` = 610, `cpu_percent` = 75, `memory_percent` = 70
   - Log: `log_level` = `ERROR`, message = "Payment service timeout"
   - Reasons: `High response time`, `Error log detected`

2. `2026-09-20T10:06:00` — `payment-service`
   - Metrics: `response_time_ms` = 640, `cpu_percent` = 94, `memory_percent` = 91
   - Log: `log_level` = `ERROR`, message = "Database connection timeout"
   - Reasons: `High response time`, `High CPU utilization`, `High memory utilization`, `Error log detected`

### Normal vs anomalous observations

The detector correctly distinguished the stable baseline from the incident window:

- Normal observations were the records with low latency, moderate CPU/memory usage, and `INFO` log entries such as "Payment request processed successfully".
- The anomalous records were those with sharply elevated latency and resource metrics plus `ERROR` logs.

### Missed or false positives

- Expected anomalies missed: none in the provided dataset. The clear threshold breach and error log patterns were both detected.
- Normal events incorrectly flagged: none. The records before the incident and after recovery remained below the anomaly thresholds and were not emitted as anomalies.

### Readability and evidence

The result is readable and contains enough context for diagnosis because each emitted anomaly includes:

- timestamp
- service name
- anomaly type
- explicit reasons for the flag
- the original source record

This makes it easy to understand both the metric-driven and log-driven cause of the anomaly.

### Limitation / improvement

A limitation of this approach is that it relies on fixed thresholds. It can reliably catch obvious spikes, but it may miss subtler drift or cross-signal anomalies that do not individually exceed a threshold yet still indicate a problem. A possible improvement would be to add rolling baselines or statistical thresholds (for example, z-score or percentile-based detection) and to correlate metric spikes with error log patterns more explicitly.

## Event Flow Validation

The repository contains a lightweight in-memory event-streaming simulation. The workflow is implemented through the following components:

- `EventProducer`: accepts a detected anomaly event and publishes it to a topic.
- `EventTopic`: stores the event messages in an in-memory topic buffer.
- `EventConsumer`: reads the messages from the topic.
- `Event/message`: the structured anomaly payload that travels through the pipeline.

### Execution verification

I executed the workflow with:

- `cd /workspaces/github-skills-challenge && python src/aiops_pipeline.py`

This produced the following result:

- `Records processed: 10`
- `Anomalies detected: 2`
- `Events consumed: 2`

The detected anomaly events were:

1. `2026-09-20T10:05:00` — service `payment-service`
   - Reasons: `High response time`, `Error log detected`
2. `2026-09-20T10:06:00` — service `payment-service`
   - Reasons: `High response time`, `High CPU utilization`, `High memory utilization`, `Error log detected`

### Workflow validation checks

The workflow satisfies the required checks:

1. An anomaly identified by detection results in an event.
   - The detector returns an anomaly object with `type` = `ANOMALY` and a `source` record.
2. The event is passed to the producer.
   - The pipeline calls `producer.publish(event)` for each flagged record.
3. The producer publishes the event to the appropriate topic.
   - `EventProducer.publish()` calls `self.topic.publish(event)`.
4. The consumer receives the event from the topic.
   - `EventConsumer.consume()` returns `self.topic.get_messages()`.
5. The consumer processes the received event.
   - The pipeline collects the consumed list and reports it as the downstream processed events.
6. The processed event reaches the downstream AIOps component.
   - The pipeline returns the consumed events through `result["events_consumed"]`, which is then printed in the execution output.

This confirms that an anomaly can travel through the complete event-processing pipeline and be surfaced as a readable downstream result.

## AIOps Scenario Overview

This project simulates a simple AIOps workflow for a payment service. The service emits operational data consisting of minute-by-minute measurements and log events. The goal is to detect abnormal behaviour, turn that abnormality into an event, and route it through a lightweight streaming pipeline so an operations component can identify and understand the issue.

The dataset is intentionally small but realistic: it contains a stable baseline, a short incident period, and a return to normal operation. This makes it suitable for validating the logic that distinguishes healthy telemetry from operational degradation.

## Operational Data Description

The operational data is stored in [data/service_data.json](data/service_data.json). Each record includes:

- `timestamp`: timestamp of the observation in ISO 8601 format
- `service`: service name
- `response_time_ms`: response latency for a request
- `cpu_percent`: CPU usage percentage
- `memory_percent`: memory usage percentage
- `log_level`: message severity (`INFO` or `ERROR`)
- `message`: textual event message

The data follows a timeline from `2026-09-20T10:00:00` to `2026-09-20T10:09:00` and represents repeated service checks over ten minutes.

## Observations from Logs and Metrics

The normal period is represented by low response times and moderate resource usage, with messages such as "Payment request processed successfully" and `INFO` logs. The abnormal period is marked by a large response-time spike and elevated CPU and memory usage, accompanied by `ERROR` logs such as "Payment service timeout" and "Database connection timeout".

The overall signal is consistent with a short incident in which the service became slow or degraded, followed by recovery to baseline levels.

## Anomaly Detection Findings

The provided detector in [src/anomaly_detector.py](src/anomaly_detector.py) flags records when metrics or logs exceed expected ranges. In this dataset, the detector identifies two anomalies:

1. `2026-09-20T10:05:00` — `response_time_ms` = 610, `log_level` = `ERROR`, message = "Payment service timeout"
2. `2026-09-20T10:06:00` — `response_time_ms` = 640, `cpu_percent` = 94, `memory_percent` = 91, `log_level` = `ERROR`, message = "Database connection timeout"

These anomalies are clearly distinguishable from normal behaviour because they include elevated latency, high utilisation, and error events.

## Event-Processing Flow

The event-processing pipeline in [src/aiops_pipeline.py](src/aiops_pipeline.py) follows the standard flow below:

1. Load operational records from the data file.
2. Run each record through the anomaly detector.
3. If a record is anomalous, create an anomaly event object.
4. Publish the event using the producer.
5. Store the event in the in-memory topic.
6. Consume the event through the consumer.
7. Report the consumed and processed anomaly event in the AIOps output.

The main components are:

- `EventProducer`: publishes the anomaly event
- `EventTopic`: in-memory broker/topic buffer
- `EventConsumer`: consumes events from the topic
- `Event/message`: payload passed through the pipeline

## Final Workflow Execution Result

The final execution of the corrected workflow produced:

- `Records processed: 10`
- `Anomalies detected: 2`
- `Events consumed: 2`

The final output clearly identifies the operational issue as the payment service experiencing timeout and degradation conditions during the incident window.

## Issues Identified and Corrected

I identified and corrected several issues in the provided workflow:

1. Import path problem in the test environment
   - Cause: the project root was not on Python’s import path and the `src` package was not recognized.
   - Fix: added [src/__init__.py](src/__init__.py) and [pytest.ini](pytest.ini).

2. Log-level detection bug
   - Cause: the detector incorrectly checked for `WARNING` instead of `ERROR` when evaluating concerning log events.
   - Fix: normalize the log level to uppercase and treat `ERROR` as a relevant anomaly signal.

3. Topic wiring mismatch
   - Cause: the producer and consumer were connected to different topics instead of the same event stream.
   - Fix: align the producer and consumer to the same `EventTopic` instance so the event can be consumed after publication.

4. File path issue for direct script execution
   - Cause: data files were resolved relative to the current working directory rather than the project root.
   - Fix: resolve file paths relative to the repository root to ensure consistent execution.

## Limitation / Possible Improvement

The current detection approach relies on fixed thresholds for latency, CPU, and memory. This works well for obvious spikes, but it may miss gradual degradation or anomalies that only become visible when several metrics move together. A possible improvement is to add baseline-based or statistical detection (for example, rolling averages or z-scores) so the system can identify more subtle departures from normal behaviour.

## Reproduction Steps

To reproduce this demonstration in another environment:

1. Open a terminal in the repository root.
2. Ensure Python dependencies are installed (the project uses the standard library only, so no additional package installation is required beyond the provided environment).
3. Run the test suite:
   - `pytest -q`
4. Run the end-to-end workflow:
   - `python src/aiops_pipeline.py`
5. Review the output to confirm that the dataset is processed, anomalies are detected, anomalies are published to the event topic, and the consumer surfaces the detected operational issue.

The expected result is that the pipeline processes 10 records and reports 2 anomaly events for the timeout and resource saturation period.

---

&copy; 2025 GitHub &bull; [Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md) &bull; [MIT License](https://gh.io/mit)

