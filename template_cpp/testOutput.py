#!/usr/bin/env python3
import sys
from pathlib import Path

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 test_receiver_output.py <config_file> <hosts_file> <logs_folder>")
        sys.exit(1)

    config_file = Path(sys.argv[1])
    hosts_file = Path(sys.argv[2])
    logs_folder = Path(sys.argv[3])

    # --- Read config file ---
    with config_file.open() as f:
        parts = f.read().strip().split()
        if len(parts) != 2:
            print("Config file must contain two integers: <M> <receiver_id>")
            sys.exit(1)
        M, receiver_id = map(int, parts)

    # --- Read hosts file ---
    hosts = []
    with hosts_file.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            pid = int(parts[0])
            hosts.append(pid)

    n = len(hosts)
    if receiver_id not in hosts:
        print(f"Receiver id {receiver_id} not found in hosts file.")
        sys.exit(1)

    # --- Determine expected messages ---
    expected_messages = set()
    for pid in hosts:
        if pid == receiver_id:
            continue
        for m in range(1, M + 1):
            expected_messages.add((pid, m))

    # --- Read receiver output ---
    receiver_file = logs_folder / f"proc{receiver_id:02d}.output"
    if not receiver_file.exists():
        print(f"Receiver output file not found: {receiver_file}")
        sys.exit(1)

    delivered_messages = []
    with receiver_file.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) != 3 or parts[0] != "d":
                print(f"Invalid line format in {receiver_file}: '{line}'")
                sys.exit(1)
            try:
                sender_id = int(parts[1])
                msg_id = int(parts[2])
            except ValueError:
                print(f"Invalid message indices in line: '{line}'")
                sys.exit(1)
            delivered_messages.append((sender_id, msg_id))

    delivered_set = set(delivered_messages)

    # --- Check conditions ---
    passed = True

    # Check duplicates
    if len(delivered_messages) != len(delivered_set):
        print("❌ Duplicate messages found in output.")
        duplicates = [msg for msg in delivered_messages if delivered_messages.count(msg) > 1]
        print("   Duplicates:", set(duplicates))
        passed = False

    # Check missing messages
    missing = expected_messages - delivered_set
    if missing:
        print("❌ Missing messages:")
        for sender, msg in sorted(missing):
            print(f"   Missing: d {sender} {msg}")
        passed = False

    # Check unexpected extra messages
    unexpected = delivered_set - expected_messages
    if unexpected:
        print("❌ Unexpected messages in output:")
        for sender, msg in sorted(unexpected):
            print(f"   Unexpected: d {sender} {msg}")
        passed = False

    if passed:
        print("✅ Test passed: Receiver delivered all expected messages exactly once.")
    else:
        print("❌ Test failed.")

if __name__ == "__main__":
    main()
