import time
import random
import json
import os
from datetime import datetime, timezone
from collections import deque
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