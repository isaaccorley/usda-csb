import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import "maplibre-gl/dist/maplibre-gl.css";
import "./style.css";

const datasetYear = 2025;
const popupMinZoom = 12;
const pmtilesBaseUrl = (
  import.meta.env.VITE_PM_TILES_BASE_URL ?? "https://data.source.coop/ftw/usda-csb/"
).replace(/\/?$/, "/");

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
      attribution:
        "Tiles &copy; Esri, Maxar, Earthstar Geographics, and the GIS User Community",
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

const map = new maplibregl.Map({
  container: "map",
  style: basemapStyles.satellite,
  center: [-96, 37],
  zoom: 3.6,
  pitch: 0,
  bearing: 0,
  attributionControl: false,
});

map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), "top-right");

const state = {
  basemap: "satellite",
  overlayVisible: true,
  cdlColored: true,
  legendVisible: false,
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

  for (const layerId of ["csb-fill", "csb-outline-halo", "csb-outline"]) {
    if (map.getLayer(layerId)) {
      map.setLayoutProperty(layerId, "visibility", visible ? "visible" : "none");
    }
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
        "fill-opacity": state.cdlColored ? 0.42 : 0.24,
        "fill-outline-color": "#16190f",
      },
      layout: {
        visibility: state.overlayVisible ? "visible" : "none",
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
        "line-width": [
          "interpolate",
          ["linear"],
          ["zoom"],
          4,
          0.9,
          8,
          1.4,
          12,
          2.4,
        ],
        "line-opacity": 1,
      },
      layout: {
        visibility: state.overlayVisible ? "visible" : "none",
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
        "line-width": [
          "interpolate",
          ["linear"],
          ["zoom"],
          4,
          0.35,
          8,
          0.7,
          12,
          1.1,
        ],
        "line-opacity": 0.78,
      },
      layout: {
        visibility: state.overlayVisible ? "visible" : "none",
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
    [
      "coalesce",
      ["to-number", ["get", "cdl"]],
      ["to-number", ["get", `CDL${datasetYear}`]],
      -1,
    ],
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
    ? cdlClassLabels.get(cdlValue) ?? `CDL ${cdlValue}`
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
    if (map.getZoom() < popupMinZoom) {
      map.getCanvas().style.cursor = "";
      state.popup.remove();
      return;
    }

    const feature = getOverlayFeatureAtPoint(event.point);
    map.getCanvas().style.cursor = feature?.properties ? "pointer" : "";

    if (!feature?.properties) {
      state.popup.remove();
      return;
    }
    state.popup
      .setLngLat(event.lngLat)
      .setHTML(formatPopupContent(feature.properties))
      .addTo(map);
  });

  map.on("mouseleave", () => {
    map.getCanvas().style.cursor = "";
    state.popup.remove();
  });
}

function applyBasemap(mode) {
  state.basemap = mode;
  setBasemapToggleState(mode);
  setStatus(`Loading ${datasetYear}`);
  map.setStyle(basemapStyles[mode], { diff: false });
}

function renderOverlay() {
  addOverlay();
  setOverlayVisibility(state.overlayVisible);
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

setOverlayToggleState(true);
setBasemapToggleState("satellite");
setPanelState(true);
setStatus("Connecting");
renderLegend();
syncLegendVisibility();

wireInteractivity();
panelToggleEl.addEventListener("click", togglePanel);
sidebarToggleEl.addEventListener("click", togglePanel);
wireOverlayToggle();
wireLegendToggle();
wireBasemapToggle();

map.on("load", () => {
  renderOverlay();
});

map.on("style.load", () => {
  renderOverlay();
});

map.on("error", () => {
  setStatus("Map error");
});
