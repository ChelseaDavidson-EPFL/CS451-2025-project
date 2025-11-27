#!/usr/bin/env python3
import sys
from pathlib import Path

def main():
    if len(sys.argv) != 5:
        print("Usage: python3 testOutputIfCrashed.py <config_file> <hosts_file> <crashed_file> <logs_folder>")
        sys.exit(1)

    config_file = Path(sys.argv[1])
    hosts_file = Path(sys.argv[2])
    crashed_file = Path(sys.argv[3])
    logs_folder = Path(sys.argv[4])

    # --- Read config: contains only M ---
    with config_file.open() as f:
        parts = f.read().strip().split()
        if len(parts) != 1:
            print("Config file must contain exactly one integer: <M>")
            sys.exit(1)
        M = int(parts[0])

    # --- Read hosts ---
    hosts = []
    with hosts_file.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            pid = int(line.split()[0])
            hosts.append(pid)

    hosts_set = set(hosts)

    # --- Read crashed ---
    crashed = set()
    with crashed_file.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            crashed.add(int(line))

    correct = hosts_set - crashed

    # --- Expected broadcasts for correct processes ---
    expected_broadcasts_full = {m for m in range(1, M + 1)}

    # Will fill after reading logs
    broadcasts = {}
    deliveries = {}

    # --- Read logs for each process ---
    for pid in hosts:
        output_file = logs_folder / f"proc{pid:02d}.output"
        b_list = []
        d_list = []

        if not output_file.exists():
            print(f"❌ Missing log: {output_file}")
            broadcasts[pid] = set()
            deliveries[pid] = []
            continue

        with output_file.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()

                if parts[0] == "b" and len(parts) == 2:
                    b_list.append(int(parts[1]))
                elif parts[0] == "d" and len(parts) == 3:
                    d_list.append((int(parts[1]), int(parts[2])))
                else:
                    print(f"❌ Invalid line in proc {pid}: '{line}'")

        broadcasts[pid] = set(b_list)
        deliveries[pid] = d_list

    # -------------------------------
    # --- Compute required sets -----
    # -------------------------------

    # For each crashed process: messages it delivered
    delivered_by_crashed = set()
    for pid in crashed:
        for tup in deliveries.get(pid, []):
            delivered_by_crashed.add(tup)

    # For each sender S, determine which messages must have been sent
    # For correct senders: all messages 1..M must be delivered by all correct processes
    # For crashed senders: only messages they actually broadcast are required
    required_deliveries = {}
    for sender in hosts:
        if sender in correct:
            required_deliveries[sender] = expected_broadcasts_full
        else:
            # crashed sender: reliable delivery only for messages they actually broadcast
            required_deliveries[sender] = broadcasts[sender]

    overall_passed = True

    # ================================
    # === Validate each process ======
    # ================================
    for pid in hosts:
        print(f"\n=== Checking process {pid} ===")

        bset = broadcasts[pid]
        dlist = deliveries[pid]
        dset = set(dlist)

        passed = True

        # --- Broadcast checks ---
        if pid in correct:
            # Must broadcast all M
            if bset != expected_broadcasts_full:
                missing_b = expected_broadcasts_full - bset
                extra_b = bset - expected_broadcasts_full
                if missing_b:
                    print("❌ Missing broadcasts:", sorted(missing_b))
                if extra_b:
                    print("❌ Unexpected broadcasts:", sorted(extra_b))
                passed = False

        # --- Duplicate deliveries ---
        if len(dlist) != len(dset):
            print("❌ Duplicate delivered messages found.")
            duplicates = {msg for msg in dlist if dlist.count(msg) > 1}
            print("   Duplicates:", duplicates)
            passed = False

        # --- Required deliveries from Reliability ---
        missing = set()
        for sender in hosts:
            required = required_deliveries[sender]
            for m in required:
                if (sender, m) not in dset and pid in correct:
                    missing.add((sender, m))

        if missing:
            print("❌ Missing required deliveries:")
            for sender, msg in sorted(missing):
                print(f"   Missing: d {sender} {msg}")
            passed = False

        # --- Uniform Reliable Broadcast checks ---
        # Anything delivered by a crashed process must be delivered by all correct ones
        if pid in correct:
            for (s, m) in delivered_by_crashed:
                if (s, m) not in dset:
                    print(f"❌ Uniformity violation:")
                    print(f"   Crashed process delivered d {s} {m}, but process {pid} did not.")
                    passed = False

        # --- FIFO check per sender ---
        by_sender = {}
        for (s, m) in dlist:
            by_sender.setdefault(s, []).append(m)

        for s, msgs in by_sender.items():
            if msgs != sorted(msgs):
                print(f"❌ FIFO violation for sender {s}:")
                print(f"   Delivered: {msgs}")
                print(f"   Expected:  {sorted(msgs)}")
                passed = False

        if passed:
            print(f"✅ Process {pid} passed.")
        else:
            print(f"❌ Process {pid} FAILED.")
            overall_passed = False

    # --- Final summary ---
    print("\n=== OVERALL RESULT ===")
    if overall_passed:
        print("✅ All processes passed.")
    else:
        print("❌ Some processes failed.")


if __name__ == "__main__":
    main()
