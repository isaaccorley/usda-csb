import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import "maplibre-gl/dist/maplibre-gl.css";
import "./style.css";

const datasetYear = 2025;
const popupMinZoom = 9;
const pmtilesBaseUrl = (
  import.meta.env.VITE_PM_TILES_BASE_URL ?? "https://data.source.coop/ftw/usda-csb/"
).replace(/\/?$/, "/");
const geoparquetSource =
  import.meta.env.VITE_CSB_GEOPARQUET_URL ?? "s3://ftw/usda-csb/csb.parquet/";
const minimumBoxPixels = 4;
const sourceCrs = "EPSG:5070";
const exportCrs = "EPSG:4326";
const viewStateStorageKey = "usda-csb:view-state";

const geoParquetMetadata = JSON.stringify({
  version: "1.1.0",
  primary_column: "geometry",
  columns: {
    geometry: {
      encoding: "WKB",
      crs: {
        $schema: "https://proj.org/schemas/v0.7/projjson.schema.json",
        type: "ProjectedCRS",
        name: "NAD83 / Conus Albers",
        base_crs: {
          name: "NAD83",
          datum: {
            type: "GeodeticReferenceFrame",
            name: "North American Datum 1983",
            ellipsoid: {
              name: "GRS 1980",
              semi_major_axis: 6378137,
              inverse_flattening: 298.257222101,
            },
          },
          coordinate_system: {
            subtype: "ellipsoidal",
            axis: [
              {
                name: "Geodetic latitude",
                abbreviation: "Lat",
                direction: "north",
                unit: "degree",
              },
              {
                name: "Geodetic longitude",
                abbreviation: "Lon",
                direction: "east",
                unit: "degree",
              },
            ],
          },
          id: { authority: "EPSG", code: 4269 },
        },
        conversion: {
          name: "Conus Albers",
          method: {
            name: "Albers Equal Area",
            id: { authority: "EPSG", code: 9822 },
          },
          parameters: [
            {
              name: "Latitude of false origin",
              value: 23,
              unit: "degree",
              id: { authority: "EPSG", code: 8821 },
            },
            {
              name: "Longitude of false origin",
              value: -96,
              unit: "degree",
              id: { authority: "EPSG", code: 8822 },
            },
            {
              name: "Latitude of 1st standard parallel",
              value: 29.5,
              unit: "degree",
              id: { authority: "EPSG", code: 8823 },
            },
            {
              name: "Latitude of 2nd standard parallel",
              value: 45.5,
              unit: "degree",
              id: { authority: "EPSG", code: 8824 },
            },
            {
              name: "Easting at false origin",
              value: 0,
              unit: "metre",
              id: { authority: "EPSG", code: 8826 },
            },
            {
              name: "Northing at false origin",
              value: 0,
              unit: "metre",
              id: { authority: "EPSG", code: 8827 },
            },
          ],
        },
        coordinate_system: {
          subtype: "Cartesian",
          axis: [
            {
              name: "Easting",
              abbreviation: "X",
              direction: "east",
              unit: "metre",
            },
            {
              name: "Northing",
              abbreviation: "Y",
              direction: "north",
              unit: "metre",
            },
          ],
        },
        scope: "Data analysis and small scale data presentation for contiguous lower 48 states.",
        area: "United States (USA) - CONUS onshore - Alabama; Arizona; Arkansas; California; Colorado; Connecticut; Delaware; Florida; Georgia; Idaho; Illinois; Indiana; Iowa; Kansas; Kentucky; Louisiana; Maine; Maryland; Massachusetts; Michigan; Minnesota; Mississippi; Missouri; Montana; Nebraska; Nevada; New Hampshire; New Jersey; New Mexico; New York; North Carolina; North Dakota; Ohio; Oklahoma; Oregon; Pennsylvania; Rhode Island; South Carolina; South Dakota; Tennessee; Texas; Utah; Vermont; Virginia; Washington; West Virginia; Wisconsin; Wyoming.",
        bbox: {
          south_latitude: 24.41,
          west_longitude: -124.79,
          north_latitude: 49.38,
          east_longitude: -66.91,
        },
        id: { authority: "EPSG", code: 5070 },
      },
      covering: {
        bbox: {
          xmin: ["bbox", "xmin"],
          ymin: ["bbox", "ymin"],
          xmax: ["bbox", "xmax"],
          ymax: ["bbox", "ymax"],
        },
      },
    },
  },
});

const duckdbBundles = {
  mvp: {
    mainModule: duckdbMvpWasm,
    mainWorker: duckdbMvpWorker,
  },
  eh: {
    mainModule: duckdbEhWasm,
    mainWorker: duckdbEhWorker,
  },
};

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const esriWorldImageryStyle = {
  version: 8,
  sources: {
    esri: {
      type: "raster",
      tiles: [
        "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
      ],
      tileSize: 256,
      attribution: "Tiles &copy; Esri, Maxar, Earthstar Geographics, and the GIS User Community",
    },
  },
  layers: [
    {
      id: "esri-imagery",
      type: "raster",
      source: "esri",
      minzoom: 0,
      maxzoom: 22,
    },
  ],
};

const cartoPositronStyle = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

const basemapStyles = {
  satellite: esriWorldImageryStyle,
  carto: cartoPositronStyle,
};

function loadViewState() {
  try {
    const raw = window.localStorage.getItem(viewStateStorageKey);
    if (!raw) {
      return null;
    }

    const parsed = JSON.parse(raw);
    const center = parsed?.center;
    if (
      !Array.isArray(center) ||
      center.length !== 2 ||
      !Number.isFinite(Number(center[0])) ||
      !Number.isFinite(Number(center[1]))
    ) {
      return null;
    }

    const zoom = Number(parsed?.zoom);
    const bearing = Number(parsed?.bearing ?? 0);
    const pitch = Number(parsed?.pitch ?? 0);

    if (!Number.isFinite(zoom)) {
      return null;
    }

    return {
      center: [Number(center[0]), Number(center[1])],
      zoom,
      bearing: Number.isFinite(bearing) ? bearing : 0,
      pitch: Number.isFinite(pitch) ? pitch : 0,
    };
  } catch {
    return null;
  }
}

function saveViewState() {
  try {
    const center = map.getCenter();
    window.localStorage.setItem(
      viewStateStorageKey,
      JSON.stringify({
        center: [center.lng, center.lat],
        zoom: map.getZoom(),
        bearing: map.getBearing(),
        pitch: map.getPitch(),
      }),
    );
  } catch {
    // no-op
  }
}

const initialViewState = loadViewState();

const map = new maplibregl.Map({
  container: "map",
  style: basemapStyles.satellite,
  center: initialViewState?.center ?? [-96, 37],
  zoom: initialViewState?.zoom ?? 3.6,
  pitch: initialViewState?.pitch ?? 0,
  bearing: initialViewState?.bearing ?? 0,
  attributionControl: false,
});

map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), "top-right");
map.boxZoom.disable();

const state = {
  basemap: "satellite",
  overlayVisible: true,
  cdlColored: true,
  legendVisible: false,
  shiftDown: false,
  selectingAoi: false,
  aoiStartPoint: null,
  aoiCurrentPoint: null,
  aoiBbox: null,
  duckdbReady: false,
  duckdbDb: null,
  duckdbConn: null,
  duckdbWorker: null,
  exporting: false,
  popup: new maplibregl.Popup({
    closeButton: true,
    closeOnClick: true,
    maxWidth: "320px",
  }),
};

const cdlClassLegend = [
  { value: 1, label: "Corn", color: "#ffd400" },
  { value: 2, label: "Cotton", color: "#ff2626" },
  { value: 3, label: "Rice", color: "#00a9e6" },
  { value: 4, label: "Sorghum", color: "#ff9e0f" },
  { value: 5, label: "Soybeans", color: "#267300" },
  { value: 6, label: "Sunflower", color: "#ffff00" },
  { value: 10, label: "Peanuts", color: "#70a800" },
  { value: 22, label: "Durum Wheat", color: "#8a6453" },
  { value: 24, label: "Winter Wheat", color: "#a87000" },
  { value: 27, label: "Rye", color: "#ae017e" },
  { value: 28, label: "Oats", color: "#a15889" },
  { value: 36, label: "Alfalfa", color: "#ffa8e3" },
  { value: 41, label: "Sugarbeets", color: "#a900e6" },
  { value: 42, label: "Dry Beans", color: "#a80000" },
  { value: 43, label: "Potatoes", color: "#732600" },
  { value: 45, label: "Sugarcane", color: "#b380ff" },
  { value: 47, label: "Misc Vegs & Fruits", color: "#ff6666" },
  { value: 53, label: "Peas", color: "#55ff00" },
  { value: 68, label: "Apples", color: "#b90050" },
  { value: 72, label: "Citrus", color: "#ffff80" },
  { value: 75, label: "Almonds", color: "#00a884" },
  { value: 212, label: "Oranges", color: "#e67525" },
];
const cdlClassLabels = new Map([
  ...cdlClassLegend.map((entry) => [entry.value, entry.label]),
  [0, "Background"],
  [11, "Tobacco"],
  [12, "Sweet Corn"],
  [13, "Pop or Orn Corn"],
  [14, "Mint"],
  [21, "Barley"],
  [23, "Spring Wheat"],
  [25, "Other Small Grains"],
  [26, "Dbl Crop WinWht/Soybeans"],
  [29, "Millet"],
  [30, "Speltz"],
  [31, "Canola"],
  [32, "Flaxseed"],
  [33, "Safflower"],
  [34, "Rape Seed"],
  [35, "Mustard"],
  [37, "Other Hay/Non Alfalfa"],
  [38, "Camelina"],
  [39, "Buckwheat"],
  [44, "Other Crops"],
  [46, "Sweet Potatoes"],
  [48, "Watermelons"],
  [49, "Onions"],
  [50, "Cucumbers"],
  [51, "Chick Peas"],
  [52, "Lentils"],
  [54, "Tomatoes"],
  [55, "Caneberries"],
  [56, "Hops"],
  [57, "Herbs"],
  [58, "Clover/Wildflowers"],
  [59, "Sod/Grass Seed"],
  [60, "Switchgrass"],
  [61, "Fallow/Idle Cropland"],
  [63, "Forest"],
  [64, "Shrubland"],
  [65, "Barren"],
  [66, "Cherries"],
  [67, "Peaches"],
  [69, "Grapes"],
  [70, "Christmas Trees"],
  [71, "Other Tree Crops"],
  [74, "Pecans"],
  [76, "Walnuts"],
  [77, "Pears"],
  [81, "Clouds/No Data"],
  [82, "Developed"],
  [83, "Water"],
  [87, "Wetlands"],
  [88, "Nonag/Undefined"],
  [92, "Aquaculture"],
  [111, "Open Water"],
  [112, "Perennial Ice/Snow"],
  [121, "Developed/Open Space"],
  [122, "Developed/Low Intensity"],
  [123, "Developed/Med Intensity"],
  [124, "Developed/High Intensity"],
  [131, "Barren"],
  [141, "Deciduous Forest"],
  [142, "Evergreen Forest"],
  [143, "Mixed Forest"],
  [152, "Shrubland"],
  [176, "Grass/Pasture"],
  [190, "Woody Wetlands"],
  [195, "Herbaceous Wetlands"],
  [204, "Pistachios"],
  [205, "Triticale"],
  [206, "Carrots"],
  [207, "Asparagus"],
  [208, "Garlic"],
  [209, "Cantaloupes"],
  [210, "Prunes"],
  [211, "Olives"],
  [213, "Honeydew Melons"],
  [214, "Broccoli"],
  [215, "Avocados"],
  [216, "Peppers"],
  [217, "Pomegranates"],
  [218, "Nectarines"],
  [219, "Greens"],
  [220, "Plums"],
  [221, "Strawberries"],
  [222, "Squash"],
  [223, "Apricots"],
  [224, "Vetch"],
  [225, "Dbl Crop WinWht/Corn"],
  [226, "Dbl Crop Oats/Corn"],
  [227, "Lettuce"],
  [228, "Dbl Crop Triticale/Corn"],
  [229, "Pumpkins"],
  [230, "Dbl Crop Lettuce/Durum Wht"],
  [231, "Dbl Crop Lettuce/Cantaloupe"],
  [232, "Dbl Crop Lettuce/Cotton"],
  [233, "Dbl Crop Lettuce/Barley"],
  [234, "Dbl Crop Durum Wht/Sorghum"],
  [235, "Dbl Crop Barley/Sorghum"],
  [236, "Dbl Crop WinWht/Sorghum"],
  [237, "Dbl Crop Barley/Corn"],
  [238, "Dbl Crop WinWht/Cotton"],
  [239, "Dbl Crop Soybeans/Cotton"],
  [240, "Dbl Crop Soybeans/Oats"],
  [241, "Dbl Crop Corn/Soybeans"],
  [242, "Blueberries"],
  [243, "Cabbage"],
  [244, "Cauliflower"],
  [245, "Celery"],
  [246, "Radishes"],
  [247, "Turnips"],
  [248, "Eggplants"],
  [249, "Gourds"],
  [250, "Cranberries"],
  [254, "Dbl Crop Barley/Soybeans"],
]);

const panelToggleEl = document.getElementById("panel-toggle");
const sidebarToggleEl = document.getElementById("sidebar-toggle");
const overlayToggleEl = document.getElementById("overlay-toggle");
const basemapToggleEl = document.getElementById("basemap-toggle");
const legendPanelEl = document.getElementById("legend-panel");
const legendSectionEl = document.getElementById("legend-section");
const legendListEl = document.getElementById("legend-list");
const cdlLegendEl = document.getElementById("cdl-legend");
const legendToggleEl = document.getElementById("legend-toggle");
const downloadFormatEl = document.getElementById("download-format");
const downloadAoiEl = document.getElementById("download-aoi");
const aoiSummaryEl = document.getElementById("aoi-summary");
const mapEl = document.getElementById("map");
const selectionBoxEl = document.createElement("div");
selectionBoxEl.className = "aoi-selection-box";
selectionBoxEl.hidden = true;
mapEl?.parentElement?.append(selectionBoxEl);

function sourcePathForDuckDB() {
  const path = geoparquetSource.trim();

  if (path.endsWith("/")) {
    const trimmedPath = path.replace(/\/+$/, "");
    if (trimmedPath.endsWith(".parquet")) {
      return `${trimmedPath}/part-c*.parquet`;
    }
    return `${trimmedPath}/*.parquet`;
  }

  if (path.endsWith(".parquet")) {
    return `${path}/part-c*.parquet`;
  }

  return `${path}/*.parquet`;
}

const parquetPath = sourcePathForDuckDB();

function sqlEscape(value) {
  return value.replaceAll("'", "''");
}

function bboxToFilterSql(bbox) {
  const [xmin, ymin, xmax, ymax] = bbox;
  const x0 = xmin.toFixed(12);
  const y0 = ymin.toFixed(12);
  const x1 = xmax.toFixed(12);
  const y1 = ymax.toFixed(12);
  return `
    WITH aoi AS (
      SELECT ST_Transform(
        ST_MakeEnvelope(${x0}, ${y0}, ${x1}, ${y1}),
        '${exportCrs}',
        '${sourceCrs}',
        true
      ) AS geom
    )
    SELECT *
    FROM read_parquet('${sqlEscape(parquetPath)}', union_by_name = true), aoi
    WHERE bbox.xmax >= ST_XMin(aoi.geom)
      AND bbox.xmin <= ST_XMax(aoi.geom)
      AND bbox.ymax >= ST_YMin(aoi.geom)
      AND bbox.ymin <= ST_YMax(aoi.geom)
      AND ST_Intersects(geometry, aoi.geom)
  `;
}

function normalizeScalar(value) {
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizeScalar(item));
  }
  if (value && typeof value === "object") {
    if (typeof value.toJSON === "function") {
      return normalizeScalar(value.toJSON());
    }

    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [key, normalizeScalar(child)]),
    );
  }

  return value;
}

function tableToObjects(table) {
  return table.toArray().map((row) => {
    if (typeof row?.toJSON === "function") {
      return normalizeScalar(row.toJSON());
    }
    return normalizeScalar(row);
  });
}

function formatAoiSummary(bbox) {
  const [xmin, ymin, xmax, ymax] = bbox;
  return `${xmin.toFixed(4)}, ${ymin.toFixed(4)} -> ${xmax.toFixed(4)}, ${ymax.toFixed(4)}`;
}

function setAoiSummary(text) {
  if (aoiSummaryEl) {
    aoiSummaryEl.textContent = text;
  }
}

function setDownloadEnabled(enabled) {
  if (downloadAoiEl) {
    downloadAoiEl.disabled = !enabled || state.exporting;
  }
}

async function initDuckDb() {
  if (state.duckdbReady && state.duckdbDb && state.duckdbConn) {
    return { db: state.duckdbDb, conn: state.duckdbConn };
  }

  const initStartedAt = performance.now();
  const bundle = await duckdb.selectBundle(duckdbBundles);
  if (!bundle.mainWorker) {
    throw new Error("Unable to initialize DuckDB worker bundle.");
  }

  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  const conn = await db.connect();

  // Some bundles include extension loading while others do not, so load best-effort.
  try {
    await conn.query("LOAD httpfs;");
  } catch {
    // no-op
  }
  try {
    await conn.query(`
      CREATE OR REPLACE SECRET sourcecoop (
        TYPE S3,
        REGION 'us-west-2',
        ENDPOINT 'data.source.coop',
        URL_STYLE 'path',
        USE_SSL true
      );
    `);
  } catch {
    // no-op
  }
  try {
    await conn.query("LOAD spatial;");
  } catch (error) {
    throw new Error(
      `DuckDB spatial extension is unavailable in this browser context: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  state.duckdbReady = true;
  state.duckdbDb = db;
  state.duckdbConn = conn;
  state.duckdbWorker = worker;
  console.debug(`[aoi-export] duckdb-init ${Math.round(performance.now() - initStartedAt)}ms`);

  return { db, conn };
}

function pointFromMouseEvent(event) {
  const container = map.getCanvasContainer();
  const rect = container.getBoundingClientRect();
  return {
    x: event.clientX - rect.left,
    y: event.clientY - rect.top,
  };
}

function drawSelectionBox(start, end) {
  if (!selectionBoxEl) {
    return;
  }

  const left = Math.min(start.x, end.x);
  const top = Math.min(start.y, end.y);
  const width = Math.abs(start.x - end.x);
  const height = Math.abs(start.y - end.y);

  selectionBoxEl.hidden = false;
  selectionBoxEl.style.left = `${left}px`;
  selectionBoxEl.style.top = `${top}px`;
  selectionBoxEl.style.width = `${width}px`;
  selectionBoxEl.style.height = `${height}px`;
}

function clearSelectionBox() {
  if (selectionBoxEl) {
    selectionBoxEl.hidden = true;
    selectionBoxEl.style.width = "0";
    selectionBoxEl.style.height = "0";
  }
}

function normalizeBounds(startPoint, endPoint) {
  const lowerLeft = map.unproject({
    x: Math.min(startPoint.x, endPoint.x),
    y: Math.max(startPoint.y, endPoint.y),
  });
  const upperRight = map.unproject({
    x: Math.max(startPoint.x, endPoint.x),
    y: Math.min(startPoint.y, endPoint.y),
  });

  return [lowerLeft.lng, lowerLeft.lat, upperRight.lng, upperRight.lat];
}

function downloadBytes(bytes, filename, mimeType) {
  const blob = new Blob([bytes], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function exportFilename(ext) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `csb_${datasetYear}_aoi_${stamp}.${ext}`;
}

async function exportGeoParquet({ db, conn, bbox }) {
  const exportStartedAt = performance.now();
  const filterSql = bboxToFilterSql(bbox);
  const outputPath = "/aoi_export.parquet";
  const metadata = sqlEscape(geoParquetMetadata);
  const copyStartedAt = performance.now();
  await conn.query(`
    COPY (
      ${filterSql}
    ) TO '${outputPath}'
    (FORMAT PARQUET, COMPRESSION ZSTD, KV_METADATA { geo: '${metadata}' })
  `);
  console.debug(`[aoi-export] parquet-copy ${Math.round(performance.now() - copyStartedAt)}ms`);
  const bufferStartedAt = performance.now();
  const bytes = await db.copyFileToBuffer(outputPath);
  console.debug(`[aoi-export] parquet-buffer ${Math.round(performance.now() - bufferStartedAt)}ms`);
  console.debug(
    `[aoi-export] parquet-total ${Math.round(performance.now() - exportStartedAt)}ms size=${bytes.byteLength}`,
  );
  return {
    bytes,
    filename: exportFilename("parquet"),
    mimeType: "application/vnd.apache.parquet",
  };
}

async function exportGeoJson({ conn, bbox }) {
  const exportStartedAt = performance.now();
  const filterSql = bboxToFilterSql(bbox);
  const queryStartedAt = performance.now();
  const resultTable = await conn.query(`
    SELECT
      * EXCLUDE (geometry),
      ST_AsGeoJSON(ST_Transform(geometry, '${sourceCrs}', '${exportCrs}', true)) AS geometry_json
    FROM (${filterSql})
  `);
  console.debug(`[aoi-export] geojson-query ${Math.round(performance.now() - queryStartedAt)}ms`);
  const rows = tableToObjects(resultTable);
  const features = rows.map((row) => {
    const { geometry_json: geometryJson, ...properties } = row;
    return {
      type: "Feature",
      geometry: geometryJson ? JSON.parse(geometryJson) : null,
      properties,
    };
  });
  const payload = {
    type: "FeatureCollection",
    features,
  };
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  console.debug(
    `[aoi-export] geojson-total ${Math.round(performance.now() - exportStartedAt)}ms size=${bytes.byteLength}`,
  );
  return {
    bytes,
    filename: exportFilename("geojson"),
    mimeType: "application/geo+json",
  };
}

async function runAoiExport(format) {
  if (!state.aoiBbox) {
    throw new Error("No AOI selected. Hold Shift and drag to draw a bounding box.");
  }

  const { db, conn } = await initDuckDb();

  if (format === "geojson") {
    return exportGeoJson({ conn, bbox: state.aoiBbox });
  }

  return exportGeoParquet({ db, conn, bbox: state.aoiBbox });
}

function updateShiftCursor() {
  const canvas = map.getCanvas();
  const container = map.getCanvasContainer();
  if (!mapEl || !canvas || !container) {
    return;
  }
  mapEl.classList.toggle("aoi-ready", state.shiftDown);
  const cursor = state.shiftDown ? "default" : "grab";
  canvas.style.cursor = cursor;
  container.style.cursor = cursor;
}

function setAoiFromBounds(bbox) {
  state.aoiBbox = bbox;
  setAoiSummary(formatAoiSummary(bbox));
  setDownloadEnabled(true);
  setStatus("AOI selected");
}

function startAoiSelection(event) {
  if (!event.shiftKey || event.button !== 0) {
    return;
  }

  event.preventDefault();
  event.stopPropagation();
  state.selectingAoi = true;
  state.aoiStartPoint = pointFromMouseEvent(event);
  state.aoiCurrentPoint = state.aoiStartPoint;
  map.dragPan.disable();
  if (
    typeof event.currentTarget?.setPointerCapture === "function" &&
    event.pointerId !== undefined
  ) {
    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // no-op
    }
  }
  drawSelectionBox(state.aoiStartPoint, state.aoiCurrentPoint);
}

function moveAoiSelection(event) {
  if (!state.selectingAoi || !state.aoiStartPoint) {
    return;
  }

  state.aoiCurrentPoint = pointFromMouseEvent(event);
  drawSelectionBox(state.aoiStartPoint, state.aoiCurrentPoint);
}

function finishAoiSelection(event) {
  if (
    typeof event?.currentTarget?.releasePointerCapture === "function" &&
    event.pointerId !== undefined
  ) {
    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // no-op
    }
  }

  if (!state.selectingAoi || !state.aoiStartPoint || !state.aoiCurrentPoint) {
    state.selectingAoi = false;
    state.aoiStartPoint = null;
    state.aoiCurrentPoint = null;
    clearSelectionBox();
    map.dragPan.enable();
    return;
  }

  const width = Math.abs(state.aoiStartPoint.x - state.aoiCurrentPoint.x);
  const height = Math.abs(state.aoiStartPoint.y - state.aoiCurrentPoint.y);
  const shouldApply = width >= minimumBoxPixels && height >= minimumBoxPixels;

  if (shouldApply) {
    setAoiFromBounds(normalizeBounds(state.aoiStartPoint, state.aoiCurrentPoint));
  }

  state.selectingAoi = false;
  state.aoiStartPoint = null;
  state.aoiCurrentPoint = null;
  clearSelectionBox();
  map.dragPan.enable();
}

function setStatus(text) {
  const statusEl = document.getElementById("status");
  if (statusEl) {
    statusEl.textContent = text;
  }
}

function pmtilesUrl() {
  return `${pmtilesBaseUrl}csb_${datasetYear}.pmtiles`;
}

function sourceSpec() {
  return {
    type: "vector",
    url: `pmtiles://${pmtilesUrl()}`,
  };
}

function firstSymbolLayerId() {
  for (const layer of map.getStyle().layers ?? []) {
    if (layer.type === "symbol") {
      return layer.id;
    }
  }
  return undefined;
}

function setOverlayToggleState(visible) {
  overlayToggleEl.textContent = visible ? "On" : "Off";
  overlayToggleEl.setAttribute("aria-pressed", String(visible));
  overlayToggleEl.dataset.active = String(visible);
}

function setBasemapToggleState(mode) {
  basemapToggleEl.textContent = mode === "satellite" ? "Satellite" : "CARTO";
  basemapToggleEl.setAttribute("aria-pressed", String(mode === "satellite"));
  basemapToggleEl.dataset.active = String(mode === "satellite");
}

function setLegendToggleState(visible) {
  legendToggleEl.setAttribute("aria-expanded", String(visible));
}

function syncLegendVisibility() {
  legendSectionEl.hidden = false;
  legendListEl.hidden = !state.legendVisible;
  setLegendToggleState(state.legendVisible);
}

function setPanelState(expanded) {
  legendPanelEl.dataset.collapsed = String(!expanded);
  panelToggleEl.textContent = expanded ? "Hide" : "Show";
  panelToggleEl.setAttribute("aria-expanded", String(expanded));
  sidebarToggleEl.hidden = expanded;
  sidebarToggleEl.setAttribute("aria-expanded", String(expanded));
}

function setOverlayVisibility(visible) {
  state.overlayVisible = visible;
  setOverlayToggleState(visible);

  if (map.getLayer("csb-fill")) {
    map.setPaintProperty("csb-fill", "fill-opacity", visible ? 0.42 : 0);
  }
  if (map.getLayer("csb-outline-halo")) {
    map.setPaintProperty("csb-outline-halo", "line-opacity", visible ? 1 : 0);
  }
  if (map.getLayer("csb-outline")) {
    map.setPaintProperty("csb-outline", "line-opacity", visible ? 0.78 : 0);
  }
}

function setBasemapVisibility(mode) {
  const satelliteVisible = mode === "satellite" ? "visible" : "none";
  const cartoVisible = mode === "carto" ? "visible" : "none";

  if (map.getLayer("esri-imagery")) {
    map.setLayoutProperty("esri-imagery", "visibility", satelliteVisible);
  }
  if (map.getLayer("carto-imagery")) {
    map.setLayoutProperty("carto-imagery", "visibility", cartoVisible);
  }
}

function addBasemapLayers() {
  if (!map.getSource("carto")) {
    map.addSource("carto", {
      type: "raster",
      tiles: ["https://basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "&copy; OpenStreetMap contributors &copy; CARTO",
    });
  }

  if (!map.getLayer("carto-imagery")) {
    const beforeId = firstSymbolLayerId();
    map.addLayer(
      {
        id: "carto-imagery",
        type: "raster",
        source: "carto",
        minzoom: 0,
        maxzoom: 22,
        layout: {
          visibility: state.basemap === "carto" ? "visible" : "none",
        },
      },
      beforeId,
    );
  }
}

function removeOverlay() {
  for (const layerId of ["csb-fill", "csb-outline-halo", "csb-outline"]) {
    if (map.getLayer(layerId)) {
      map.removeLayer(layerId);
    }
  }
  if (map.getSource("csb")) {
    map.removeSource("csb");
  }
}

function addOverlay() {
  if (map.getSource("csb")) {
    removeOverlay();
  }

  map.addSource("csb", sourceSpec());

  const beforeId = firstSymbolLayerId();
  map.addLayer(
    {
      id: "csb-fill",
      type: "fill",
      source: "csb",
      "source-layer": "fields",
      paint: {
        "fill-color": cdlFillColorPaint(),
        "fill-opacity": state.overlayVisible ? (state.cdlColored ? 0.42 : 0.24) : 0,
        "fill-outline-color": "#16190f",
      },
      layout: {
        visibility: "visible",
      },
    },
    beforeId,
  );

  map.addLayer(
    {
      id: "csb-outline-halo",
      type: "line",
      source: "csb",
      "source-layer": "fields",
      paint: {
        "line-color": "rgba(9, 12, 7, 0.72)",
        "line-width": ["interpolate", ["linear"], ["zoom"], 4, 0.9, 8, 1.4, 12, 2.4],
        "line-opacity": state.overlayVisible ? 1 : 0,
      },
      layout: {
        visibility: "visible",
      },
    },
    beforeId,
  );

  map.addLayer(
    {
      id: "csb-outline",
      type: "line",
      source: "csb",
      "source-layer": "fields",
      paint: {
        "line-color": "#f2e8b6",
        "line-width": ["interpolate", ["linear"], ["zoom"], 4, 0.35, 8, 0.7, 12, 1.1],
        "line-opacity": state.overlayVisible ? 0.78 : 0,
      },
      layout: {
        visibility: "visible",
      },
    },
    beforeId,
  );
}

function cdlFillColorPaint() {
  if (!state.cdlColored) {
    return "#a99744";
  }

  return [
    "match",
    ["coalesce", ["to-number", ["get", "cdl"]], ["to-number", ["get", `CDL${datasetYear}`]], -1],
    ...cdlClassLegend.flatMap((entry) => [entry.value, entry.color]),
    "#a99744",
  ];
}

function renderLegend() {
  cdlLegendEl.innerHTML = cdlClassLegend
    .map(
      (entry) => `
        <div class="legend-item">
          <span class="legend-swatch" style="background:${entry.color}"></span>
          <a
            class="legend-label legend-link"
            href="https://www.google.com/search?q=${encodeURIComponent(entry.label)}"
            target="_blank"
            rel="noreferrer"
            title="Search Google for ${entry.label}"
          >
            ${entry.label}
          </a>
        </div>
      `,
    )
    .join("");
}

function getCdlValue(properties) {
  const cdlValue = properties.cdl ?? properties[`CDL${datasetYear}`];
  const parsedValue = Number(cdlValue);

  if (Number.isFinite(parsedValue)) {
    return parsedValue;
  }

  for (const [key, value] of Object.entries(properties)) {
    if (/^cdl\d*$/i.test(key)) {
      const fallbackValue = Number(value);
      if (Number.isFinite(fallbackValue)) {
        return fallbackValue;
      }
    }
  }

  return Number.NaN;
}

function formatPopupContent(properties) {
  const cdlValue = getCdlValue(properties);
  const cdlLabel = Number.isFinite(cdlValue)
    ? (cdlClassLabels.get(cdlValue) ?? `CDL ${cdlValue}`)
    : "n/a";
  const rows = `<div class="popup-row"><span>CDL class</span><strong>${cdlLabel}</strong></div>`;

  return `
    <div class="popup">
      <div class="popup-title">Field</div>
      <div class="popup-grid">${rows}</div>
    </div>
  `;
}

function getOverlayFeatureAtPoint(point) {
  const features = map.queryRenderedFeatures(point, { layers: ["csb-fill"] });
  return features[0];
}

function wireInteractivity() {
  map.on("mousemove", (event) => {
    if (state.selectingAoi || state.shiftDown) {
      state.popup.remove();
      updateShiftCursor();
      return;
    }

    if (map.getZoom() < popupMinZoom) {
      updateShiftCursor();
      state.popup.remove();
      return;
    }

    const feature = getOverlayFeatureAtPoint(event.point);
    updateShiftCursor();
    if (!feature?.properties) {
      state.popup.remove();
      return;
    }
    state.popup.setLngLat(event.lngLat).setHTML(formatPopupContent(feature.properties)).addTo(map);
  });

  map.on("mouseleave", () => {
    updateShiftCursor();
    state.popup.remove();
  });
}

function applyBasemap(mode) {
  state.basemap = mode;
  setBasemapToggleState(mode);
  setBasemapVisibility(mode);
}

function renderOverlay() {
  addBasemapLayers();
  addOverlay();
  setOverlayVisibility(state.overlayVisible);
  setBasemapVisibility(state.basemap);
  setStatus(`Ready ${datasetYear}`);
}

function togglePanel() {
  const collapsed = legendPanelEl.dataset.collapsed === "true";
  setPanelState(collapsed);
}

function wireBasemapToggle() {
  basemapToggleEl.addEventListener("click", () => {
    applyBasemap(state.basemap === "satellite" ? "carto" : "satellite");
  });
}

function wireOverlayToggle() {
  overlayToggleEl.addEventListener("click", () => {
    setOverlayVisibility(!state.overlayVisible);
  });
}

function wireLegendToggle() {
  legendToggleEl.addEventListener("click", () => {
    state.legendVisible = !state.legendVisible;
    syncLegendVisibility();
  });
}

function wireAoiSelection() {
  document.addEventListener(
    "pointerdown",
    (event) => {
      if (event.target instanceof Element && event.target.closest(".maplibregl-canvas-container")) {
        startAoiSelection(event);
      }
    },
    true,
  );
  document.addEventListener("pointermove", moveAoiSelection, true);
  document.addEventListener("pointerup", finishAoiSelection, true);
  document.addEventListener("pointercancel", finishAoiSelection, true);
  window.addEventListener("keydown", (event) => {
    if (event.key === "Shift") {
      state.shiftDown = true;
      updateShiftCursor();
    }
  });
  window.addEventListener("keyup", (event) => {
    if (event.key === "Shift") {
      state.shiftDown = false;
      updateShiftCursor();
    }
  });
}

function wireAoiDownload() {
  if (!downloadAoiEl || !downloadFormatEl) {
    return;
  }

  downloadAoiEl.addEventListener("click", async () => {
    if (!state.aoiBbox || state.exporting) {
      return;
    }

    const format = downloadFormatEl.value === "geojson" ? "geojson" : "geoparquet";
    state.exporting = true;
    setDownloadEnabled(false);
    setStatus(`Exporting ${format}...`);

    try {
      const result = await runAoiExport(format);
      downloadBytes(result.bytes, result.filename, result.mimeType);
      setStatus(`Downloaded ${format}`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "AOI export failed");
    } finally {
      state.exporting = false;
      setDownloadEnabled(true);
    }
  });
}

if (import.meta.env.DEV) {
  window.__aoiDebug = {
    map,
    get db() {
      return state.duckdbDb;
    },
    get conn() {
      return state.duckdbConn;
    },
    state,
    duckdbDataProtocol: duckdb.DuckDBDataProtocol,
    setAoiBbox(bbox) {
      state.aoiBbox = bbox;
      setAoiSummary(formatAoiSummary(bbox));
      setDownloadEnabled(true);
    },
    clearAoiBbox() {
      state.aoiBbox = null;
      setAoiSummary("None selected");
      setDownloadEnabled(false);
    },
    runAoiExport,
  };
}

setOverlayToggleState(true);
setBasemapToggleState("satellite");
setPanelState(true);
setAoiSummary("None selected");
setDownloadEnabled(false);
setStatus("Connecting");
renderLegend();
syncLegendVisibility();

wireInteractivity();
panelToggleEl.addEventListener("click", togglePanel);
sidebarToggleEl.addEventListener("click", togglePanel);
wireOverlayToggle();
wireLegendToggle();
wireBasemapToggle();
wireAoiSelection();
wireAoiDownload();

map.on("load", () => {
  renderOverlay();
});

map.on("moveend", saveViewState);

map.on("error", () => {
  setStatus("Map error");
});
