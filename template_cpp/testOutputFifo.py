#!/usr/bin/env python3
import sys
from pathlib import Path

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 test_output.py <config_file> <hosts_file> <logs_folder>")
        sys.exit(1)

    config_file = Path(sys.argv[1])
    hosts_file = Path(sys.argv[2])
    logs_folder = Path(sys.argv[3])

    # --- Read config: contains only M ---
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
            if not line:
                continue
            pid = int(line.split()[0])
            hosts.append(pid)

    # --- Expected global events ---
    expected_broadcasts = {m for m in range(1, M + 1)}

    expected_deliveries = set()
    for sender in hosts:
        for msg_id in range(1, M + 1):
            expected_deliveries.add((sender, msg_id))

    overall_passed = True

    # --- Check each process ---
    for pid in hosts:
        print(f"\n=== Checking process {pid} ===")

        output_file = logs_folder / f"proc{pid:02d}.output"
        if not output_file.exists():
            print(f"❌ Missing log file: {output_file}")
            overall_passed = False
            continue

        broadcasted = []
        delivered = []

        with output_file.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()

                if parts[0] == "b":
                    if len(parts) != 2:
                        print(f"❌ Invalid broadcast line: '{line}'")
                        overall_passed = False
                        continue
                    try:
                        msg_id = int(parts[1])
                        broadcasted.append(msg_id)
                    except ValueError:
                        print(f"❌ Invalid broadcast message id in: '{line}'")
                        overall_passed = False

                elif parts[0] == "d":
                    if len(parts) != 3:
                        print(f"❌ Invalid delivery line: '{line}'")
                        overall_passed = False
                        continue
                    try:
                        sender = int(parts[1])
                        msg_id = int(parts[2])
                        delivered.append((sender, msg_id))
                    except ValueError:
                        print(f"❌ Invalid delivery line: '{line}'")
                        overall_passed = False
                else:
                    print(f"❌ Invalid line prefix: '{line}'")
                    overall_passed = False

        broadcast_set = set(broadcasted)
        delivered_set = set(delivered)

        passed = True

        # --- Broadcast checks ---
        missing_b = expected_broadcasts - broadcast_set
        extra_b = broadcast_set - expected_broadcasts

        if missing_b:
            print("❌ Missing broadcast messages:")
            for m in sorted(missing_b):
                print(f"   Missing: b {m}")
            passed = False

        if extra_b:
            print("❌ Unexpected broadcast messages:")
            for m in sorted(extra_b):
                print(f"   Unexpected: b {m}")
            passed = False

        # --- Delivery checks ---
        if len(delivered) != len(delivered_set):
            print("❌ Duplicate delivered messages detected.")
            duplicates = {msg for msg in delivered if delivered.count(msg) > 1}
            print("   Duplicates:", duplicates)
            passed = False

        missing_d = expected_deliveries - delivered_set
        if missing_d:
            print("❌ Missing delivered messages:")
            for sender, msg in sorted(missing_d):
                print(f"   Missing: d {sender} {msg}")
            passed = False

        extra_d = delivered_set - expected_deliveries
        if extra_d:
            print("❌ Unexpected delivered messages:")
            for sender, msg in sorted(extra_d):
                print(f"   Unexpected: d {sender} {msg}")
            passed = False

        if passed:
            print(f"✅ Process {pid} passed ✔️")
        else:
            print(f"❌ Process {pid} FAILED")
            overall_passed = False

    # --- Final summary ---
    print("\n=== OVERALL RESULT ===")
    if overall_passed:
        print("✅ All processes passed.")
    else:
        print("❌ Some processes failed.")

if __name__ == "__main__":
    main()
