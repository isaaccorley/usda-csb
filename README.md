# USDA CSB Cloud Native

Convert USDA Crop Sequence Boundaries data into flat GeoParquet shards and PMTiles.

## Quick Start

```bash
./scripts/download-csb.sh
uv run usda-csb convert --output-dir geoparquet
uv run usda-csb build-pmtiles --source-dir geoparquet --output-dir pmtiles --years 2025
```

## Output

- GeoParquet shards in `geoparquet/`
- PMTiles archives in `pmtiles/`
- Web app source in `app/`

## Conversion Notes

- GeoParquet uses `geometry`, `bbox`, GeoParquet 1.1 metadata, ZSTD, and EPSG:4326.
- `usda-csb build-pmtiles` stages each year through FlatGeobuf before calling `tippecanoe`.
- The web app loads PMTiles from `https://data.source.coop/ftw/usda-csb/` by default.
- Override that with `VITE_PM_TILES_BASE_URL` if needed.

## Web App

```bash
npm install
npm run dev
```
