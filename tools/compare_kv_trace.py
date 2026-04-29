#!/usr/bin/env python3
import os
import sys
import time


def parse_line(line):
    fields = {}
    for part in line.rstrip("\n").split("\t"):
        if "=" in part:
            key, value = part.split("=", 1)
            fields[key] = value
    return fields


def comparable(fields):
    return {key: value for key, value in fields.items() if key != "seq"}


def main():
    if len(sys.argv) != 3:
        print("usage: compare_kv_trace.py <native.trace> <psitri.trace>", file=sys.stderr)
        return 2

    progress_interval = int(os.environ.get("KV_TRACE_PROGRESS_INTERVAL", "1000000"))
    last_progress = time.monotonic()

    left_path, right_path = sys.argv[1], sys.argv[2]
    with open(left_path, "r", encoding="utf-8", errors="replace") as left, open(
        right_path, "r", encoding="utf-8", errors="replace"
    ) as right:
        for line_no, (left_line, right_line) in enumerate(zip(left, right), 1):
            left_fields = parse_line(left_line)
            right_fields = parse_line(right_line)
            if comparable(left_fields) != comparable(right_fields):
                print(f"first mismatch at event {line_no}")
                print(f"native: {left_line.rstrip()}")
                print(f"psitri: {right_line.rstrip()}")
                return 1
            if progress_interval > 0 and line_no % progress_interval == 0:
                now = time.monotonic()
                elapsed = now - last_progress
                rate = progress_interval / elapsed if elapsed > 0 else 0
                print(f"progress events={line_no} rate={rate:.0f}/s", flush=True)
                last_progress = now

        left_extra = left.readline()
        right_extra = right.readline()
        if left_extra or right_extra:
            print("trace length mismatch")
            if left_extra:
                print(f"native extra: {left_extra.rstrip()}")
            if right_extra:
                print(f"psitri extra: {right_extra.rstrip()}")
            return 1

    print("traces match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
