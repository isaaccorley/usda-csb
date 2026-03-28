from __future__ import annotations

import argparse
from collections.abc import Sequence

from .build_pmtiles import add_arguments as add_pmtiles_arguments
from .build_pmtiles import run as run_pmtiles
from .convert_csb_to_geoparquet import add_arguments as add_convert_arguments
from .convert_csb_to_geoparquet import run as run_convert


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="usda-csb", description="USDA CSB tooling.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert USDA CSB FileGDB data to GeoParquet.",
    )
    add_convert_arguments(convert_parser)
    convert_parser.set_defaults(func=run_convert)

    pmtiles_parser = subparsers.add_parser(
        "build-pmtiles",
        help="Build PMTiles archives from GeoParquet shards.",
    )
    add_pmtiles_arguments(pmtiles_parser)
    pmtiles_parser.set_defaults(func=run_pmtiles)

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
