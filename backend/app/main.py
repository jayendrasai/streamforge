import time
import random
import json
import os
from datetime import datetime, timezone
from collections import deque

# ── Kafka Configuration ──────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9092",
    "topic": "truck-telemetry",
    "num_partitions": 20,
    "replication_factor": 3,
    "num_trucks": 50000,
    "batch_size": 1000,
    "target_events_per_sec": 100000,
}

TRUCK_REGIONS = ["North", "South", "East", "West", "Central"]
TRUCK_TYPES   = ["Heavy", "Medium", "Light", "Refrigerated", "Tanker"]


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

    

class TruckTelemetryProducer:
    """
    High-throughput Kafka producer for IoT truck telemetry.
    Generates realistic sensor data for 50,000 trucks.
    In production: uses confluent_kafka.Producer with batching.
    """

    def __init__(self, kafka: KafkaCluster):
        self.kafka = kafka
        self.topic  = kafka.config["topic"]
        self.produced = 0
        self.errors = 0
        self.throughput_history = deque(maxlen=20)

        # Pre-generate truck fleet
        self.trucks = self._generate_fleet(kafka.config["num_trucks"])

    def _generate_fleet(self, count: int) -> list:
        """Generates a fleet of virtual trucks with metadata."""
        print(f"\n[PRODUCER] Generating fleet of {count:,} trucks...")
        fleet = []
        sample = min(count, 20)
        for i in range(1, sample + 1):
            fleet.append({
                "truck_id": f"TRK-{i:05d}",
                "region": random.choice(TRUCK_REGIONS),
                "type": random.choice(TRUCK_TYPES),
                "base_temp": random.uniform(15, 35),
                "route": f"Route-{random.randint(1, 500)}",
            })
        # Simulate remaining trucks
        for i in range(sample + 1, count + 1):
            fleet.append({
                "truck_id": f"TRK-{i:05d}",
                "region": random.choice(TRUCK_REGIONS),
                "type": random.choice(TRUCK_TYPES),
                "base_temp": random.uniform(15, 35),
                "route": f"Route-{random.randint(1, 500)}",
            })
        print(f"  ✅ Fleet ready: {count:,} trucks across "
              f"{len(TRUCK_REGIONS)} regions")
        return fleet

    def generate_event(self, truck: dict) -> dict:
        """Generates one telemetry event for a truck."""
        temp_variation = random.uniform(-2, 2)
        return {
            "truck_id": truck["truck_id"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature_c": round(truck["base_temp"] + temp_variation, 2),
            "humidity_pct": round(random.uniform(30, 80), 1),
            "speed_kmh": round(random.uniform(0, 120), 1),
            "fuel_level_pct": round(random.uniform(10, 100), 1),
            "gps_lat": round(random.uniform(8.0, 37.0), 6),
            "gps_lon": round(random.uniform(68.0, 97.0), 6),
            "region": truck["region"],
            "truck_type": truck["type"],
            "route": truck["route"],
            "engine_rpm": random.randint(600, 3000),
            "alert": temp_variation > 1.5,
        }

    def produce_batch(self, batch_size: int) -> dict:
        """Sends a batch of telemetry events to Kafka."""
        start = time.time()
        batch_trucks = random.choices(self.trucks, k=batch_size)

        success = 0
        for truck in batch_trucks:
            event = self.generate_event(truck)
            result = self.kafka.send(
                topic=self.topic,
                key=truck["truck_id"],
                value=event,
            )
            if result["success"]:
                success += 1
                self.produced += 1
            else:
                self.errors += 1

        elapsed = time.time() - start
        throughput = round(success / elapsed) if elapsed > 0 else 0
        self.throughput_history.append(throughput)

        return {
            "batch_size": batch_size,
            "success": success,
            "errors": batch_size - success,
            "elapsed_ms": round(elapsed * 1000, 2),
            "throughput_per_sec": throughput,
        }

    def get_avg_throughput(self) -> int:
        if not self.throughput_history:
            return 0
        return int(sum(self.throughput_history) /
                   len(self.throughput_history))

class TopologyVisualizer:
    """
    Visualizes the streaming DAG topology in the terminal.
    In production: renders as React Flow diagram in browser.
    """

    def display(self, kafka_stats: dict, producer_stats: dict):
        print(f"\n{'=' * 65}")
        print(f"  StreamForge Topology — Directed Acyclic Graph (DAG)")
        print(f"{'=' * 65}")
        print(f"""
  ┌─────────────────┐
  │  50,000 Trucks  │  IoT Sensors → Telemetry Events
  │  (IoT Sources)  │
  └────────┬────────┘
           │ {producer_stats['produced']:,} events produced
           ▼
  ┌─────────────────┐
  │  Apache Kafka   │  {kafka_stats['active_brokers']} Brokers | 
  │  Message Broker │  {kafka_stats['partitions']} Partitions
  │  (topic:        │  {kafka_stats['topic_bytes_mb']} MB ingested
  │  truck-telemetry│
  └────────┬────────┘
           │ Partitioned by Truck ID
           ▼
  ┌─────────────────┐
  │  20 Python      │  Faust/Bytewax Workers
  │  Worker Nodes   │  Windowed Aggregations
  │  (StreamForge)  │  5-min Rolling Avg Temp
  └────────┬────────┘
           │ Processed results
           ▼
  ┌─────────────────┐
  │  RocksDB State  │  Fault-Tolerant State Store
  │  Store          │  Changelog → Kafka
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │  React Flow     │  Live Dashboard
  │  Dashboard      │  Metrics & Alerts
  └─────────────────┘
""")
        print(f"  Kafka Stats:")
        print(f"  Total Messages:  {kafka_stats['topic_messages']:,}")
        print(f"  Data Ingested:   {kafka_stats['topic_bytes_mb']} MB")
        print(f"  Avg Throughput:  "
              f"{producer_stats['avg_throughput']:,} events/sec")
        print(f"{'=' * 65}")


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