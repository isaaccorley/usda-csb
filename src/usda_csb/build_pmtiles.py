from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from collections.abc import Sequence  # noqa: TC003
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyogrio
import shapely
from pyproj import CRS, Transformer
from tqdm.auto import tqdm


def add_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "--source-dir",
        default="geoparquet",
        help="Path to the flat wide GeoParquet dataset.",
    )
    parser.add_argument(
        "--output-dir",
        default="pmtiles",
        help="Directory where year-specific PMTiles archives will be written.",
    )
    parser.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=list(range(2018, 2026)),
        help="Subset of years to build. Defaults to 2018-2025.",
    )
    parser.add_argument(
        "--layer-name",
        default="fields",
        help="Vector tile layer name inside each PMTiles archive.",
    )
    parser.add_argument(
        "--name-prefix",
        default="csb",
        help="Filename prefix for the output PMTiles archives.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Number of rows to stream from GeoParquet per Arrow batch.",
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=None,
        help="Optional row limit for testing.",
    )
    parser.add_argument(
        "--tippecanoe-max-zoom",
        type=int,
        default=15,
        help="Maximum zoom to preserve in the vector tiles.",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    return add_arguments(
        argparse.ArgumentParser(
            description="Build one PMTiles archive per year from the wide USDA CSB GeoParquet dataset."
        )
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def run_tippecanoe(
    *,
    output_path: Path,
    input_path: Path,
    layer_name: str,
    max_zoom: int,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "tippecanoe",
        "-f",
        "-o",
        str(output_path),
        "-l",
        layer_name,
        "-Z0",
        f"-z{max_zoom}",
        "--projection=EPSG:4326",
        "--extend-zooms-if-still-dropping",
        "--drop-densest-as-needed",
        "--detect-shared-borders",
        "--read-parallel",
        str(input_path),
    ]
    completed = subprocess.run(cmd, check=False)
    if completed.returncode != 0:
        raise RuntimeError(
            f"tippecanoe failed for {output_path} with exit code {completed.returncode}"
        )


def build_year_pmtiles(
    dataset: ds.Dataset,
    *,
    output_path: Path,
    year: int,
    layer_name: str,
    batch_size: int,
    max_features: int | None,
    max_zoom: int,
) -> None:
    year_column = f"CDL{year}"
    scanner = dataset.scanner(
        columns=[year_column, "geometry"],
        batch_size=batch_size,
    )

    schema_meta = dataset.schema.metadata or {}
    geo_json = schema_meta.get(b"geo")
    if geo_json is None:
        raise RuntimeError("Could not determine CRS from GeoParquet geometry metadata.")
    geo_meta = json.loads(geo_json.decode("utf-8"))
    geometry_column = geo_meta["primary_column"]
    column_meta = geo_meta["columns"][geometry_column]
    source_crs = CRS.from_user_input(column_meta["crs"])
    transformer = Transformer.from_crs(source_crs, CRS.from_epsg(4326), always_xy=True)
    geometry_type = column_meta.get("geometry_types", [None])[0]
    if geometry_type is None:
        raise RuntimeError("Could not determine geometry type from GeoParquet metadata.")

    rows_written = 0
    progress = tqdm(total=max_features, desc=f"pmtiles {year}", unit="row", dynamic_ncols=True)
    with tempfile.TemporaryDirectory(
        prefix=f"{output_path.stem}-fgb-", dir=output_path.parent
    ) as tempdir:
        staging_path = Path(tempdir) / f"{output_path.stem}.fgb"
        first_batch = True
        try:
            for batch in scanner.to_batches():
                if max_features is not None and rows_written >= max_features:
                    break

                limit = batch.num_rows
                if max_features is not None:
                    limit = min(limit, max_features - rows_written)
                if limit == 0:
                    continue

                if limit != batch.num_rows:
                    batch = batch.slice(0, limit)

                geoms = shapely.from_wkb(batch[geometry_column])
                geoms = shapely.transform(geoms, transformer.transform, interleaved=False)

                table = pa.Table.from_batches([batch])
                geometry_index = table.schema.get_field_index(geometry_column)
                table = table.set_column(
                    geometry_index,
                    "geometry",
                    pa.array(shapely.to_wkb(geoms), type=pa.binary()),
                )

                pyogrio.write_arrow(
                    table,
                    staging_path,
                    driver="FlatGeobuf",
                    geometry_name="geometry",
                    geometry_type=geometry_type,
                    crs="EPSG:4326",
                    append=not first_batch,
                )
                first_batch = False

                rows_written += limit
                progress.update(limit)

            if rows_written == 0:
                raise RuntimeError(f"No rows were written for {output_path}")

            run_tippecanoe(
                output_path=output_path,
                input_path=staging_path,
                layer_name=layer_name,
                max_zoom=max_zoom,
            )
        finally:
            progress.close()


def run(args: argparse.Namespace) -> None:
    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset = ds.dataset(source_dir, format="parquet")

    for year in args.years:
        output_path = output_dir / f"{args.name_prefix}_{year}.pmtiles"
        build_year_pmtiles(
            dataset,
            output_path=output_path,
            year=year,
            layer_name=args.layer_name,
            batch_size=args.batch_size,
            max_features=args.max_features,
            max_zoom=args.tippecanoe_max_zoom,
        )


def main(argv: Sequence[str] | None = None) -> None:
    run(parse_args(argv))


if __name__ == "__main__":
    main()
