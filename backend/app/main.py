import time
import random
import json
import os
from datetime import datetime, timezone
from collections import deque


class KafkaCluster:
    """
    Simulates a local Kafka cluster.
    In production: uses confluent-kafka or kafka-python
    to connect to real Apache Kafka brokers.
    """

    def __init__(self, config: dict):
        self.config = config
        self.brokers = [
            {"id": 1, "host": "localhost:9092", "status": "leader"},
            {"id": 2, "host": "localhost:9093", "status": "follower"},
            {"id": 3, "host": "localhost:9094", "status": "follower"},
        ]
        self.topics = {}
        self.total_messages = 0

    def start(self) -> bool:
        print("\n[KAFKA] Starting Kafka cluster...")
        for broker in self.brokers:
            print(f"  Broker {broker['id']} | {broker['host']} | "
                  f"Role: {broker['status']} ✅")
            time.sleep(0.2)
        return True

    def create_topic(self, topic: str, partitions: int,
                     replication: int) -> bool:
        print(f"\n[KAFKA] Creating topic: '{topic}'")
        print(f"  Partitions:         {partitions}")
        print(f"  Replication Factor: {replication}")
        print(f"  Retention:          7 days")
        time.sleep(0.3)
        self.topics[topic] = {
            "partitions": partitions,
            "replication_factor": replication,
            "messages": 0,
            "bytes": 0,
        }
        print(f"  ✅ Topic '{topic}' created!")
        return True

    def send(self, topic: str, key: str, value: dict) -> dict:
        """Sends one message to a Kafka topic partition."""
        if topic not in self.topics:
            return {"success": False, "error": "Topic not found"}

        partition = hash(key) % self.config["num_partitions"]
        msg_bytes = len(json.dumps(value))
        self.topics[topic]["messages"] += 1
        self.topics[topic]["bytes"] += msg_bytes
        self.total_messages += 1

        return {
            "success": True,
            "topic": topic,
            "partition": partition,
            "offset": self.topics[topic]["messages"],
            "key": key,
            "bytes": msg_bytes,
        }

    def get_stats(self) -> dict:
        topic_stats = self.topics.get(self.config["topic"], {})
        return {
            "total_messages": self.total_messages,
            "topic_messages": topic_stats.get("messages", 0),
            "topic_bytes_mb": round(
                topic_stats.get("bytes", 0) / (1024 * 1024), 2),
            "active_brokers": len(self.brokers),
            "partitions": self.config["num_partitions"],
        }

def main():
    print("=" * 65)
    print("  StreamForge - Week 1: Kafka Foundation")
    print("  Axlero Solutions | Prince Mittal R")
    print("=" * 65)

    # Start Kafka cluster
    kafka = KafkaCluster(KAFKA_CONFIG)
    kafka.start()
    kafka.create_topic(
        KAFKA_CONFIG["topic"],
        KAFKA_CONFIG["num_partitions"],
        KAFKA_CONFIG["replication_factor"],
    )

    # Start producer
    producer = TruckTelemetryProducer(kafka)

    print(f"\n[PRODUCER] Starting high-throughput production...")
    print(f"  Target: {KAFKA_CONFIG['target_events_per_sec']:,} events/sec\n")

    num_batches = 8
    for i in range(1, num_batches + 1):
        result = producer.produce_batch(KAFKA_CONFIG["batch_size"])
        print(f"  Batch {i}/{num_batches} | "
              f"Sent: {result['success']:,} | "
              f"Time: {result['elapsed_ms']:.1f}ms | "
              f"Throughput: {result['throughput_per_sec']:,}/sec")
        time.sleep(0.2)

    # Display topology
    kafka_stats = kafka.get_stats()
    producer_stats = {
        "produced": producer.produced,
        "errors": producer.errors,
        "avg_throughput": producer.get_avg_throughput(),
    }

    visualizer = TopologyVisualizer()
    visualizer.display(kafka_stats, producer_stats)

    # Save report
    os.makedirs("outputs", exist_ok=True)
    report = {
        "project": "StreamForge - Week 1",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "kafka_config": KAFKA_CONFIG,
        "kafka_stats": kafka_stats,
        "producer_stats": producer_stats,
    }
    with open("outputs/week1_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n  📄 Report saved to outputs/week1_report.json")
    print(f"  ✅ Week 1 Complete: Kafka Foundation Ready!")


if __name__ == "__main__":
    main()