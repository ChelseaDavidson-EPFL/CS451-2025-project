#!/usr/bin/env python3
import sys
from pathlib import Path

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 testOutputSendAll.py <config_file> <hosts_file> <logs_folder>")
        sys.exit(1)

    config_file = Path(sys.argv[1])
    hosts_file = Path(sys.argv[2])
    logs_folder = Path(sys.argv[3])

    # --- Read config file (contains only M) ---
    with config_file.open() as f:
        parts = f.read().strip().split()
        if len(parts) != 1:
            print("Config file must contain exactly one integer: <M>")
            sys.exit(1)
        M = int(parts[0])

    # --- Read hosts file ---
    hosts = []
    with hosts_file.open() as f:
        for line in f:
            line = line.strip()
            if line:
                hosts.append(int(line.split()[0]))

    P = len(hosts)

    # --- Expected broadcasts for each process ---
    expected_broadcasts = sorted([m for _ in range(P - 1) for m in range(1, M + 1)])
    expected_broadcast_count = (P - 1) * M

    # --- Expected deliveries for each process ---
    expected_deliveries = set()
    for sender in hosts:
        for m in range(1, M + 1):
            expected_deliveries.add((sender, m))

    overall_passed = True

    # --- Validate each process ---
    for pid in hosts:
        print(f"\n=== Checking process {pid} ===")

        # Expected deliveries EXCLUDE own messages
        expected_deliveries_pid = {dm for dm in expected_deliveries if dm[0] != pid}

        output_file = logs_folder / f"proc{pid:02d}.output"
        if not output_file.exists():
            print(f"❌ Missing file: {output_file}")
            overall_passed = False
            continue

        broadcasts = []
        deliveries = []

        # --- Parse output file ---
        with output_file.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()

                if parts[0] == "b":
                    if len(parts) != 2:
                        print(f"❌ Invalid broadcast line: {line}")
                        overall_passed = False
                        continue
                    broadcasts.append(int(parts[1]))

                elif parts[0] == "d":
                    if len(parts) != 3:
                        print(f"❌ Invalid delivery line: {line}")
                        overall_passed = False
                        continue
                    deliveries.append((int(parts[1]), int(parts[2])))

                else:
                    print(f"❌ Invalid line: {line}")
                    overall_passed = False

        # --- Check broadcasts ---
        if len(broadcasts) != expected_broadcast_count:
            print(f"❌ Incorrect number of broadcasts. Expected {expected_broadcast_count}, found {len(broadcasts)}")
            overall_passed = False

        if sorted(broadcasts) != expected_broadcasts:
            print("❌ Broadcasts do not match expected pattern (unordered repetition of 1..M).")
            print("   Expected:", expected_broadcasts)
            print("   Found:   ", sorted(broadcasts))
            overall_passed = False

        # --- Check deliveries ---
        delivered_set = set(deliveries)

        missing = expected_deliveries_pid - delivered_set
        if missing:
            print("❌ Missing deliveries:")
            for (s, m) in sorted(missing):
                print(f"   d {s} {m}")
            overall_passed = False

        extra = delivered_set - expected_deliveries_pid
        if extra:
            print("❌ Unexpected deliveries:")
            for (s, m) in sorted(extra):
                print(f"   d {s} {m}")
            overall_passed = False

        if overall_passed:
            print(f"✅ Process {pid} passed.")

    print("\n=== OVERALL RESULT ===")
    if overall_passed:
        print("✅ All processes passed all broadcast and delivery checks.")
    else:
        print("❌ Some processes failed.")

if __name__ == "__main__":
    main()
