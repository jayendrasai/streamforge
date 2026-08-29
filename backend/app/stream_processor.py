def main():
    print("=" * 65)
    print("  StreamForge - Week 2: Stream Topology & Windowing")
    print("  Axlero Solutions | Prince Mittal R")
    print("=" * 65)

    # Initialize 20 worker nodes
    print(f"\n[WORKERS] Spawning {STREAM_CONFIG['num_workers']} "
          f"Faust worker nodes...")
    workers = []
    partitions_per_worker = (STREAM_CONFIG["num_partitions"] //
                             STREAM_CONFIG["num_workers"])

    for i in range(STREAM_CONFIG["num_workers"]):
        start_p = i * partitions_per_worker
        end_p = start_p + partitions_per_worker
        worker = FaustWorkerNode(
            worker_id=i + 1,
            partitions=list(range(start_p, end_p))
        )
        workers.append(worker)

    print(f"  ✅ {len(workers)} workers ready | "
          f"{partitions_per_worker} partitions each\n")

    # Process stream batches
    print(f"[PIPELINE] Processing stream: "
          f"Consume → Filter → Map → Window\n")

    total_processed = 0
    total_alerts = 0
    num_batches = 6

    for batch_num in range(1, num_batches + 1):
        events = generate_truck_events(1000)

        # Distribute events across workers
        worker_results = []
        chunk_size = len(events) // len(workers)

        for i, worker in enumerate(workers):
            chunk = events[i * chunk_size:(i + 1) * chunk_size]
            if chunk:
                result = worker.process_events(chunk)
                worker_results.append(result)
                total_processed += result["events_passed"]
                total_alerts += result["alerts"]

        # Show batch summary (first 5 workers only)
        avg_eps = sum(r["events_per_sec"] for r in worker_results)
        print(f"  Batch {batch_num}/{num_batches} | "
              f"Events: {len(events):,} | "
              f"Passed Filter: {total_processed:,} | "
              f"Alerts: {total_alerts} | "
              f"Throughput: {avg_eps:,}/sec")
        time.sleep(0.2)

    # Window aggregation results
    print(f"\n[WINDOWING] Computing 5-minute rolling averages...")
    sample_worker = workers[0]
    window_keys = list(sample_worker.aggregator.windows.keys())

    if window_keys:
        sample_window = window_keys[0]
        window_results = sample_worker.aggregator.compute_window_averages(
            sample_window)
        print(f"  Window: {sample_window}")
        print(f"  Trucks in window: {len(window_results)}")
        if window_results:
            sample = window_results[:3]
            for r in sample:
                print(f"  {r['truck_id']} | "
                      f"Avg: {r['avg_temp_c']}°C | "
                      f"Min: {r['min_temp_c']}°C | "
                      f"Max: {r['max_temp_c']}°C | "
                      f"Readings: {r['num_readings']} | "
                      f"Alert: {'🚨' if r['alert'] else '✅'}")

    # Throughput audit
    total_eps = sum(w.get_avg_eps() for w in workers)
    meets_target = total_eps >= 100000

    print(f"\n[AUDIT] Throughput Check:")
    print(f"  Total Events/sec:    {total_eps:,}")
    print(f"  Target:              100,000/sec")
    print(f"  Result:              "
          f"{'✅ PASSED' if meets_target else '❌ FAILED'}")
    print(f"  Filter Stats:        "
          f"{sample_worker.filter.get_stats()}")
    print(f"  Window Stats:        "
          f"{sample_worker.aggregator.get_stats()}")

    # Save report
    os.makedirs("outputs", exist_ok=True)
    report = {
        "project": "StreamForge - Week 2",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "config": STREAM_CONFIG,
        "total_processed": total_processed,
        "total_alerts": total_alerts,
        "throughput_eps": total_eps,
        "meets_100k_target": meets_target,
        "worker_count": len(workers),
        "window_stats": sample_worker.aggregator.get_stats(),
    }
    with open("outputs/week2_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n  📄 Report saved to outputs/week2_report.json")
    print(f"  ✅ Week 2 Complete: Stream Topology & Windowing Ready!")


if __name__ == "__main__":
    main()