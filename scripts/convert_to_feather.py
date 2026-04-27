#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pyarrow>=24.0.0,<25",
# ]
# ///

from __future__ import annotations

import argparse
import json
from calendar import timegm
from pathlib import Path

import pyarrow as pa
import pyarrow.feather as feather

UNDEF_PRICE = 2**63 - 1
RTYPE_MBO = 0xA0
NANOS_PER_SECOND = 1_000_000_000

SCHEMA = pa.schema(
    [
        ("ts_recv", pa.uint64()),
        ("ts_event", pa.uint64()),
        ("rtype", pa.uint8()),
        ("publisher_id", pa.uint16()),
        ("instrument_id", pa.uint32()),
        ("order_id", pa.uint64()),
        ("action", pa.uint8()),
        ("side", pa.uint8()),
        ("price", pa.int64()),
        ("size", pa.uint32()),
        ("channel_id", pa.uint8()),
        ("flags", pa.uint8()),
        ("ts_in_delta", pa.int32()),
        ("sequence", pa.uint32()),
        ("symbol", pa.string()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Databento NDJSON MBO files into typed Feather tables."
    )
    parser.add_argument("inputs", nargs="+", help="Input .json/.mbo.json files")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory for .feather files (default: beside each input)",
    )
    parser.add_argument(
        "--compression",
        default="lz4",
        choices=("lz4", "zstd", "uncompressed"),
        help="Feather compression codec",
    )
    return parser.parse_args()


def parse_uint(raw: object, *, default: int = 0) -> int:
    if raw is None:
        return default
    if isinstance(raw, int):
        if raw < 0:
            raise ValueError(f"expected unsigned integer, got {raw}")
        return raw
    if isinstance(raw, str):
        return parse_uint(int(raw, 10), default=default)
    raise ValueError(f"expected unsigned integer-like value, got {raw!r}")


def parse_int(raw: object, *, default: int = 0) -> int:
    if raw is None:
        return default
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        return int(raw, 10)
    raise ValueError(f"expected integer-like value, got {raw!r}")


def parse_iso8601_timestamp(raw: str) -> int:
    if not raw.endswith("Z"):
        raise ValueError(f"timestamp must end with Z, got {raw!r}")
    core = raw[:-1]
    if "." in core:
        base, frac = core.split(".", 1)
    else:
        base, frac = core, ""
    year = int(base[0:4], 10)
    month = int(base[5:7], 10)
    day = int(base[8:10], 10)
    hour = int(base[11:13], 10)
    minute = int(base[14:16], 10)
    second = int(base[17:19], 10)
    nanos = int((frac + "000000000")[:9], 10) if frac else 0
    seconds = timegm((year, month, day, hour, minute, second, 0, 0, 0))
    return seconds * NANOS_PER_SECOND + nanos


def parse_timestamp(raw: object) -> int:
    if raw is None:
        raise ValueError("timestamp is required")
    if isinstance(raw, int):
        if raw < 0:
            raise ValueError(f"timestamp must be >= 0, got {raw}")
        return raw
    if isinstance(raw, str):
        if raw.endswith("Z"):
            return parse_iso8601_timestamp(raw)
        return parse_uint(raw)
    raise ValueError(f"expected timestamp-like value, got {raw!r}")


def parse_price(raw: object) -> int:
    if raw is None:
        return UNDEF_PRICE
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        if "." not in raw:
            return int(raw, 10)
        negative = raw.startswith("-")
        if raw[0] in "+-":
            raw = raw[1:]
        whole, frac = raw.split(".", 1)
        scaled = int(whole, 10) * NANOS_PER_SECOND + int((frac + "000000000")[:9], 10)
        return -scaled if negative else scaled
    raise ValueError(f"expected price-like value, got {raw!r}")


def parse_char(raw: object, *, default: str) -> int:
    if raw is None:
        return ord(default)
    if isinstance(raw, str) and raw:
        return ord(raw[0])
    raise ValueError(f"expected character-like value, got {raw!r}")


def normalise_record(record: dict) -> dict[str, object]:
    header = record.get("hd", {})
    return {
        "ts_recv": parse_timestamp(record.get("ts_recv")),
        "ts_event": parse_timestamp(header.get("ts_event", record.get("ts_event"))),
        "rtype": parse_uint(header.get("rtype", record.get("rtype", RTYPE_MBO))),
        "publisher_id": parse_uint(
            header.get("publisher_id", record.get("publisher_id"))
        ),
        "instrument_id": parse_uint(
            header.get("instrument_id", record.get("instrument_id"))
        ),
        "order_id": parse_uint(record.get("order_id")),
        "action": parse_char(record.get("action"), default="N"),
        "side": parse_char(record.get("side"), default="N"),
        "price": parse_price(record.get("price")),
        "size": parse_uint(record.get("size")),
        "channel_id": parse_uint(record.get("channel_id")),
        "flags": parse_uint(record.get("flags")),
        "ts_in_delta": parse_int(record.get("ts_in_delta")),
        "sequence": parse_uint(record.get("sequence")),
        "symbol": str(record.get("symbol") or ""),
    }


def convert_file(input_path: Path, output_path: Path, compression: str) -> int:
    columns = {field.name: [] for field in SCHEMA}
    with input_path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                row = normalise_record(record)
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValueError(f"{input_path}:{line_no}: invalid row: {exc}") from exc
            for field in SCHEMA:
                columns[field.name].append(row[field.name])

    arrays = [pa.array(columns[field.name], type=field.type) for field in SCHEMA]
    table = pa.Table.from_arrays(arrays, schema=SCHEMA)
    feather.write_feather(
        table,
        output_path,
        compression=None if compression == "uncompressed" else compression,
    )
    return table.num_rows


def main() -> int:
    args = parse_args()
    total_rows = 0

    for raw_input in args.inputs:
        input_path = Path(raw_input)
        if not input_path.is_file():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        output_dir = args.out_dir if args.out_dir is not None else input_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{input_path.stem}.feather"

        rows = convert_file(input_path, output_path, args.compression)
        total_rows += rows
        print(f"{input_path} -> {output_path} rows={rows}")

    print(f"total_rows={total_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
