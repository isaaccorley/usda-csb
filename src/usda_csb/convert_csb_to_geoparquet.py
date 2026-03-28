from __future__ import annotations

import argparse
import json
import math
import shutil
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import shapely
from hilbertcurve.hilbertcurve import HilbertCurve
from pyproj import CRS, Transformer
from tqdm.auto import tqdm


@dataclass(frozen=True)
class Chunk:
    chunk_id: int
    bbox: tuple[float, float, float, float]
    where: str


def normalize_geometry_type(geometry_type: str | None) -> str | None:
    if not geometry_type:
        return None
    return geometry_type.replace(" ", "")


def crs_to_json(crs: object | None) -> object | None:
    if crs is None:
        return None
    if isinstance(crs, dict):
        return crs
    try:
        return CRS.from_user_input(crs).to_json_dict()
    except Exception:
        return crs


def resolve_crs(crs: object | None) -> CRS | None:
    if crs is None:
        return None
    return CRS.from_user_input(crs)


def build_geo_metadata(
    *,
    geometry_column: str,
    geometry_type: str | None,
    crs: object | None,
) -> bytes:
    column_meta: dict[str, object] = {
        "encoding": "WKB",
        "crs": crs_to_json(crs),
        "covering": {
            "bbox": {
                "xmin": ["bbox", "xmin"],
                "ymin": ["bbox", "ymin"],
                "xmax": ["bbox", "xmax"],
                "ymax": ["bbox", "ymax"],
            }
        },
    }
    if geometry_type:
        column_meta["geometry_types"] = [geometry_type]

    return json.dumps(
        {
            "version": "1.1.0",
            "primary_column": geometry_column,
            "columns": {geometry_column: column_meta},
        },
        separators=(",", ":"),
    ).encode("utf-8")


def quantize_coordinates(
    values: np.ndarray, *, lower: float, upper: float, bits: int
) -> np.ndarray:
    scale = (1 << bits) - 1
    normalized = (values - lower) / (upper - lower)
    normalized = np.clip(normalized, 0.0, 1.0)
    return np.rint(normalized * scale).astype(np.uint32)


def hilbert_indices(x: np.ndarray, y: np.ndarray, *, bits: int = 16) -> np.ndarray:
    curve = HilbertCurve(bits, 2)
    points = np.column_stack((x.tolist(), y.tolist()))
    return np.asarray(curve.distances_from_points(points), dtype=np.uint64)


def bbox_struct_array(bounds: np.ndarray) -> pa.StructArray:
    arrays = [
        pa.array(bounds[:, 0], type=pa.float64()),
        pa.array(bounds[:, 1], type=pa.float64()),
        pa.array(bounds[:, 2], type=pa.float64()),
        pa.array(bounds[:, 3], type=pa.float64()),
    ]
    return pa.StructArray.from_arrays(arrays, names=["xmin", "ymin", "xmax", "ymax"])


def chunk_where_clause(
    xmin: float,
    xmax: float,
    ymin: float,
    ymax: float,
    *,
    include_xmax: bool,
    include_ymax: bool,
) -> str:
    x_upper_op = "<=" if include_xmax else "<"
    y_upper_op = "<=" if include_ymax else "<"
    return (
        f"INSIDE_X >= {xmin:.6f} AND INSIDE_X {x_upper_op} {xmax:.6f} "
        f"AND INSIDE_Y >= {ymin:.6f} AND INSIDE_Y {y_upper_op} {ymax:.6f}"
    )


def default_chunk_grid(jobs: int) -> tuple[int, int]:
    cols = math.ceil(math.sqrt(jobs))
    rows = math.ceil(jobs / cols)
    return cols, rows


def parse_chunk_grid(value: str | None, jobs: int) -> tuple[int, int]:
    if value is None:
        return default_chunk_grid(jobs)
    parts = value.lower().split("x")
    if len(parts) != 2:
        raise SystemExit("--chunk-grid must be in COLSxROWS format, for example 4x2.")
    cols, rows = (int(part) for part in parts)
    if cols < 1 or rows < 1:
        raise SystemExit("--chunk-grid dimensions must be positive integers.")
    return cols, rows


def build_chunks(bounds: tuple[float, float, float, float], cols: int, rows: int) -> list[Chunk]:
    xmin, ymin, xmax, ymax = bounds
    x_edges = np.linspace(xmin, xmax, cols + 1)
    y_edges = np.linspace(ymin, ymax, rows + 1)
    chunks: list[Chunk] = []
    chunk_id = 0
    for row in range(rows):
        for col in range(cols):
            chunk = Chunk(
                chunk_id=chunk_id,
                bbox=(
                    float(x_edges[col]),
                    float(y_edges[row]),
                    float(x_edges[col + 1]),
                    float(y_edges[row + 1]),
                ),
                where=chunk_where_clause(
                    float(x_edges[col]),
                    float(x_edges[col + 1]),
                    float(y_edges[row]),
                    float(y_edges[row + 1]),
                    include_xmax=col == cols - 1,
                    include_ymax=row == rows - 1,
                ),
            )
            chunks.append(chunk)
            chunk_id += 1
    return chunks


def sort_and_attach_spatial_columns(
    batch: pa.RecordBatch,
    *,
    geometry_column: str,
    transformer: Transformer | None,
) -> pa.Table:
    table = pa.Table.from_batches([batch])

    geometries = shapely.from_wkb(batch[geometry_column])
    if transformer is not None:
        geometries = shapely.transform(geometries, transformer.transform, interleaved=False)

    geom_wkb = pa.array(shapely.to_wkb(geometries), type=pa.binary())
    bounds = np.asarray(shapely.bounds(geometries), dtype="float64")
    centers_x = (bounds[:, 0] + bounds[:, 2]) / 2.0
    centers_y = (bounds[:, 1] + bounds[:, 3]) / 2.0
    hilbert = hilbert_indices(
        quantize_coordinates(centers_x, lower=-180.0, upper=180.0, bits=16),
        quantize_coordinates(centers_y, lower=-90.0, upper=90.0, bits=16),
    )

    sort_idx = np.argsort(hilbert, kind="stable")
    sort_idx_array = pa.array(sort_idx, type=pa.int64())
    table = table.take(sort_idx_array)
    geom_wkb = geom_wkb.take(sort_idx_array)
    bbox_column = bbox_struct_array(bounds).take(sort_idx_array)
    hilbert_column = pa.array(hilbert[sort_idx], type=pa.uint64())

    geometry_index = table.schema.get_field_index(geometry_column)
    table = table.set_column(geometry_index, "geometry", geom_wkb)
    table = table.append_column("bbox", bbox_column)
    table = table.append_column("__hilbert", hilbert_column)
    return table


@contextmanager
def iter_batches(
    source: str,
    *,
    layer: str,
    batch_size: int,
    where: str | None = None,
):
    with pyogrio.open_arrow(
        source,
        layer=layer,
        batch_size=batch_size,
        where=where,
        use_pyarrow=True,
    ) as source_and_reader:
        yield source_and_reader


def write_partitioned_dataset(
    source: str,
    *,
    layer: str,
    output_dir: Path,
    output_stem: str,
    batch_size: int,
    max_features: int | None,
    target_file_size_mb: int,
    target_row_group_size_mb: int,
    where: str | None = None,
    file_prefix: str = "",
    show_progress: bool = True,
    clear_output: bool = True,
    require_nonempty: bool = True,
) -> bool:
    with iter_batches(source, layer=layer, batch_size=batch_size, where=where) as (meta, reader):
        geometry_column = meta["geometry_name"] or "wkb_geometry"
        geometry_type = normalize_geometry_type(meta.get("geometry_type"))
        source_crs = resolve_crs(meta.get("crs"))
        target_crs = CRS.from_epsg(4326)
        transformer = None
        if source_crs is not None and not source_crs.equals(target_crs):
            transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        geo_metadata = build_geo_metadata(
            geometry_column="geometry",
            geometry_type=geometry_type,
            crs=target_crs.to_json_dict(),
        )

        if clear_output:
            shutil.rmtree(output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        rows_remaining = max_features

        features_total = meta.get("features")
        if max_features is not None:
            features_total = (
                min(features_total, max_features) if features_total is not None else max_features
            )

        feature_bar = (
            tqdm(
                total=features_total,
                desc="features",
                unit="row",
                dynamic_ncols=True,
            )
            if show_progress
            else None
        )
        file_bar = (
            tqdm(
                total=None,
                desc="files",
                unit="file",
                leave=False,
                dynamic_ncols=True,
            )
            if show_progress
            else None
        )

        chunk_tables: list[pa.Table] = []
        for batch in reader:
            if rows_remaining == 0:
                break

            if batch.num_rows == 0:
                continue

            if rows_remaining is not None and batch.num_rows > rows_remaining:
                batch = batch.slice(0, rows_remaining)

            chunk_tables.append(
                sort_and_attach_spatial_columns(
                    batch,
                    geometry_column=geometry_column,
                    transformer=transformer,
                )
            )

            if feature_bar is not None:
                feature_bar.update(batch.num_rows)
            if rows_remaining is not None:
                rows_remaining -= batch.num_rows

        if not chunk_tables:
            if feature_bar is not None:
                feature_bar.close()
            if file_bar is not None:
                file_bar.close()
            if not require_nonempty:
                return False
            raise RuntimeError(f"No rows were written for {output_dir}")

        table = pa.concat_tables(chunk_tables, promote_options="default")
        table = table.sort_by([("__hilbert", "ascending")])
        hilbert_index = table.schema.get_field_index("__hilbert")
        table = table.remove_column(hilbert_index)

        metadata = dict(table.schema.metadata or {})
        metadata[b"geo"] = geo_metadata
        table = table.replace_schema_metadata(metadata)

        row_size_bytes = max(1, int(table.nbytes / max(1, table.num_rows)))
        target_file_rows = max(1, int((target_file_size_mb * 1024 * 1024) / row_size_bytes))
        target_row_group_rows = max(
            1, int((target_row_group_size_mb * 1024 * 1024) / row_size_bytes)
        )

        file_part = 0
        for start in range(0, table.num_rows, target_file_rows):
            end = min(start + target_file_rows, table.num_rows)
            file_path = output_dir / f"{output_stem}{file_prefix}{file_part:05d}.parquet"
            pq.write_table(
                table.slice(start, end - start),
                file_path,
                compression="zstd",
                row_group_size=target_row_group_rows,
            )
            file_part += 1
            if file_bar is not None:
                file_bar.update(1)

        if feature_bar is not None:
            feature_bar.close()
        if file_bar is not None:
            file_bar.close()
        return True


def run_chunk_export(
    *,
    source: str,
    layer: str,
    output_dir: str,
    output_stem: str,
    batch_size: int,
    target_file_size_mb: int,
    target_row_group_size_mb: int,
    chunk_id: int,
    where: str,
) -> int:
    write_partitioned_dataset(
        source,
        layer=layer,
        output_dir=Path(output_dir),
        output_stem=output_stem,
        batch_size=batch_size,
        max_features=None,
        target_file_size_mb=target_file_size_mb,
        target_row_group_size_mb=target_row_group_size_mb,
        where=where,
        file_prefix=f"-c{chunk_id:03d}-",
        show_progress=False,
        clear_output=False,
        require_nonempty=False,
    )
    return chunk_id


def add_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "--source",
        default="data/NationalCSB_2018-2025_rev23/CSB1825.gdb",
        help="Path to the source FileGDB directory.",
    )
    parser.add_argument(
        "--layer",
        default="national1825",
        help="Layer name to read from the geodatabase.",
    )
    parser.add_argument(
        "--output-dir",
        default="geoparquet",
        help="Directory where flat GeoParquet shard files will be written.",
    )
    parser.add_argument(
        "--output-stem",
        default="part",
        help="Filename prefix for parquet fragments.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=65536,
        help="Arrow batch size used while streaming from the GDB.",
    )
    parser.add_argument(
        "--target-file-size-mb",
        type=int,
        default=512,
        help="Target shard size in MB. Files will be split around this size.",
    )
    parser.add_argument(
        "--target-row-group-size-mb",
        type=int,
        default=192,
        help="Target Parquet row-group size in MB.",
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=None,
        help="Optional limit for testing smaller extracts.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=16,
        help="Number of worker processes. Values greater than 1 use coarse spatial chunks.",
    )
    parser.add_argument(
        "--chunk-grid",
        default=None,
        help="Optional coarse chunk grid in COLSxROWS format, for example 4x2.",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    return add_arguments(
        argparse.ArgumentParser(
            description="Convert USDA CSB FileGDB data to a flat, Hilbert-sorted GeoParquet dataset."
        )
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def run(args: argparse.Namespace) -> None:
    source = str(Path(args.source))
    output_dir = Path(args.output_dir)
    if args.jobs < 1:
        raise SystemExit("--jobs must be at least 1.")
    if args.jobs > 1 and args.max_features is not None:
        raise SystemExit("--max-features is only supported in serial mode.")

    if args.jobs == 1:
        write_partitioned_dataset(
            source,
            layer=args.layer,
            output_dir=output_dir,
            output_stem=args.output_stem,
            batch_size=args.batch_size,
            max_features=args.max_features,
            target_file_size_mb=args.target_file_size_mb,
            target_row_group_size_mb=args.target_row_group_size_mb,
        )
        return

    info = pyogrio.read_info(source, layer=args.layer, force_total_bounds=True)
    bounds = cast(
        "tuple[float, float, float, float]",
        tuple(float(v) for v in info["total_bounds"]),
    )
    cols, rows = parse_chunk_grid(args.chunk_grid, args.jobs)
    chunks = build_chunks(bounds, cols, rows)

    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    chunk_bar = tqdm(total=len(chunks), desc="chunks", unit="chunk", dynamic_ncols=True)
    max_workers = min(args.jobs, len(chunks))
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                run_chunk_export,
                source=source,
                layer=args.layer,
                output_dir=str(output_dir),
                output_stem=args.output_stem,
                batch_size=args.batch_size,
                target_file_size_mb=args.target_file_size_mb,
                target_row_group_size_mb=args.target_row_group_size_mb,
                chunk_id=chunk.chunk_id,
                where=chunk.where,
            ): chunk.chunk_id
            for chunk in chunks
        }
        for future in as_completed(futures):
            chunk_bar.set_postfix_str(f"chunk {future.result()}")
            chunk_bar.update(1)
    chunk_bar.close()


def main(argv: Sequence[str] | None = None) -> None:
    run(parse_args(argv))


if __name__ == "__main__":
    main()
