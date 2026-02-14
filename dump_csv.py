#!/usr/bin/env python3
"""Dump stabping data to CSV for a given date range."""

import argparse
import csv
import struct
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

SENTINEL_ERROR = -2_100_000_000
SENTINEL_NODATA = -2_000_000_000
RECORD_SIZE = 12  # 3 x i32 = 12 bytes


def parse_datetime(s: str) -> datetime:
    """Parse a datetime string in ISO-ish format, treating it as UTC."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Invalid datetime: {s!r}. Use YYYY-MM-DD [HH:MM[:SS]]"
    )


def find_data_dir(config_path: Path | None) -> Path:
    """Locate the stabping_data directory using the same search order as stabping."""
    if config_path:
        data_dir = config_path.parent / "stabping_data"
        if data_dir.is_dir():
            return data_dir
        sys.exit(f"Data directory not found: {data_dir}")

    candidates = [
        Path.cwd() / "stabping_data",
        Path.home() / ".config" / "stabping_data",
        Path("/etc/stabping_data"),
    ]
    for d in candidates:
        if d.is_dir():
            return d
    sys.exit(
        "Could not find stabping_data directory. "
        "Use --config to specify the stabping_config.json location."
    )


def read_index(data_dir: Path) -> list[str]:
    """Read the address index file (one address per line)."""
    index_path = data_dir / "tcpping.index.json"
    if not index_path.exists():
        sys.exit(f"Index file not found: {index_path}")
    return [line for line in index_path.read_text().splitlines() if line]


def read_data(data_dir: Path) -> list[tuple[int, int, int]]:
    """Read all (timestamp, addr_index, value) triplets from the data file."""
    data_path = data_dir / "tcpping.data.dat"
    if not data_path.exists():
        sys.exit(f"Data file not found: {data_path}")

    raw = data_path.read_bytes()
    if len(raw) % RECORD_SIZE != 0:
        print(f"Warning: data file size ({len(raw)}) is not a multiple of {RECORD_SIZE}", file=sys.stderr)

    count = len(raw) // RECORD_SIZE
    records = struct.unpack(f"<{count * 3}i", raw[: count * RECORD_SIZE])
    return [(records[i], records[i + 1], records[i + 2]) for i in range(0, len(records), 3)]


def dump_csv(
    data_dir: Path,
    start: datetime | None,
    end: datetime | None,
    output: Path | None,
):
    addrs = read_index(data_dir)
    records = read_data(data_dir)

    start_ts = int(start.timestamp()) if start else 0
    end_ts = int(end.timestamp()) if end else 2**31 - 1

    # Group records by timestamp, building {timestamp: {addr_index: value}}
    grouped: dict[int, dict[int, int]] = defaultdict(dict)
    for ts, idx, val in records:
        if start_ts <= ts <= end_ts:
            grouped[ts][idx] = val

    if not grouped:
        print("No data found in the specified range.", file=sys.stderr)
        return

    # Determine which address indices appear in the data
    all_indices = sorted({idx for row in grouped.values() for idx in row})
    addr_headers = [addrs[i] if i < len(addrs) else f"unknown_{i}" for i in all_indices]

    fh = open(output, "w", newline="") if output else sys.stdout
    try:
        writer = csv.writer(fh)
        writer.writerow(["timestamp", "datetime_utc"] + addr_headers)

        for ts in sorted(grouped):
            row_vals = []
            for idx in all_indices:
                val = grouped[ts].get(idx)
                if val is None or val == SENTINEL_NODATA or val == SENTINEL_ERROR:
                    row_vals.append("")
                else:
                    row_vals.append(f"{val / 1000:.3f}")  # microseconds -> milliseconds

            dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([ts, dt_str] + row_vals)
    finally:
        if output:
            fh.close()

    dest = str(output) if output else "stdout"
    count = len(grouped)
    print(f"Wrote {count} rows to {dest}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Dump stabping data to CSV.")
    parser.add_argument("--start", type=parse_datetime, help="Start datetime (UTC): YYYY-MM-DD [HH:MM[:SS]]")
    parser.add_argument("--end", type=parse_datetime, help="End datetime (UTC): YYYY-MM-DD [HH:MM[:SS]]")
    parser.add_argument("-o", "--output", type=Path, help="Output CSV file (default: stdout)")
    parser.add_argument("--config", type=Path, help="Path to stabping_config.json")
    args = parser.parse_args()

    data_dir = find_data_dir(args.config)
    dump_csv(data_dir, args.start, args.end, args.output)


if __name__ == "__main__":
    main()
