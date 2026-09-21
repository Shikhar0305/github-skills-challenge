import json
from pathlib import Path

try:
    from src.anomaly_detector import AnomalyDetector
    from src.event_consumer import EventConsumer
    from src.event_producer import EventProducer
    from src.event_topic import EventTopic
except ImportError:  # pragma: no cover - fallback for direct script execution
    from anomaly_detector import AnomalyDetector
    from event_consumer import EventConsumer
    from event_producer import EventProducer
    from event_topic import EventTopic


def load_data(file_path):
    data_path = Path(file_path)
    if not data_path.is_absolute():
        data_path = Path(__file__).resolve().parents[1] / data_path
    with data_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def run_pipeline(file_path):
    data = load_data(file_path)

    producer_topic = EventTopic("anomaly-events")

    detector = AnomalyDetector()
    producer = EventProducer(producer_topic)
    consumer = EventConsumer(producer_topic)

    detected_events = []

    for record in data:
        event = detector.detect(record)

        if event:
            producer.publish(event)
            detected_events.append(event)

    consumed_events = consumer.consume()

    return {
        "records_processed": len(data),
        "anomalies_detected": detected_events,
        "events_consumed": consumed_events
    }


if __name__ == "__main__":
    result = run_pipeline("data/service_data.json")

    print("=" * 50)
    print("AIOps Pipeline Result")
    print("=" * 50)

    print(f"Records processed: {result['records_processed']}")
    print(f"Anomalies detected: {len(result['anomalies_detected'])}")
    print(f"Events consumed: {len(result['events_consumed'])}")

    print("\nDetected Events:")

    for event in result["events_consumed"]:
        print(f"\nService: {event['service']}")
        print(f"Timestamp: {event['timestamp']}")
        print(f"Type: {event['type']}")
        print(f"Reasons: {', '.join(event['reasons'])}")