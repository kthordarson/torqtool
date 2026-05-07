"""Trip Statistics tab aggregated analysis of torqtrips data."""
from __future__ import annotations

import io
from typing import Any

import folium
from folium.plugins import HeatMap
import matplotlib
import matplotlib.dates as mdates
import matplotlib.figure
import numpy as np
import pandas as pd
from loguru import logger
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from sqlalchemy.orm import sessionmaker

from PySide6.QtCore import QObject, QThread, Qt, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QTabWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from .map_canvas import FoliumMapView
from .pandas_model import PandasModel

# ── sentinel threshold (float32 max ≈ 3.4e38) ────────────────────────────────
_SENTINEL_THRESHOLD = 1e30


def _clean(series: pd.Series) -> pd.Series:
    """Replace sentinel/overflow float values with NaN."""
    return series.where(series.abs() < _SENTINEL_THRESHOLD)


# ── background data loader ────────────────────────────────────────────────────

class _TripStatsLoader(QObject):
    finished: Signal = Signal(pd.DataFrame)
    error: Signal = Signal(str)

    def __init__(self, engine: Any) -> None:
        super().__init__()
        self.engine = engine

    def run(self) -> None:
        try:
            Session = sessionmaker(bind=self.engine)
            session = Session()
            try:
                conn = session.connection()
                df = pd.read_sql(
                    """
                    SELECT
                        tt.*,
                        tf.startlat,
                        tf.startlon,
                        tf.endlat,
                        tf.endlon,
                        tf.trip_start,
                        tf.trip_end
                    FROM torqtrips tt
                    LEFT JOIN torqfiles tf ON tf.fileid = tt.fileid
                    ORDER BY tt.tripdate
                    """,
                    con=conn,
                )
            finally:
                session.close()
            logger.debug(f"Loaded {len(df)} trips with {df.shape[1]} columns for TripStatsWindow")
            self.finished.emit(df)
        except Exception as exc:
            logger.error(f"TripStatsLoader error: {exc} ({type(exc)})")
            self.error.emit(str(exc))


# ── chart helpers ─────────────────────────────────────────────────────────────

def _make_figure(rows: int = 1, cols: int = 1, **kwargs: Any) -> tuple[matplotlib.figure.Figure, Any]:
    fig = matplotlib.figure.Figure(figsize=(10, 3.5 * rows), tight_layout=True, **kwargs)
    axes = fig.subplots(rows, cols, squeeze=False)
    return fig, axes


def _ax_date_fmt(ax: Any, df: pd.DataFrame, date_col: str = "tripdate") -> None:
    """Apply readable date tick labels to an axes."""
    span_days = (df[date_col].max() - df[date_col].min()).days if len(df) > 1 else 1
    if span_days < 90:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    elif span_days < 730:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.figure.autofmt_xdate(rotation=30, ha="right")


def _build_timeline_figure(df: pd.DataFrame, group_by: str) -> matplotlib.figure.Figure:
    """Trips per period bar + cumulative distance line."""
    fig, axes = _make_figure(2, 1)
    ax_count = axes[0][0]
    ax_dist = axes[1][0]

    if df.empty or "tripdate" not in df.columns:
        ax_count.set_title("No data")
        return fig

    dated = df.dropna(subset=["tripdate"]).copy()
    dated["tripdate"] = pd.to_datetime(dated["tripdate"], errors="coerce")
    dated = dated.dropna(subset=["tripdate"])
    if dated.empty:
        ax_count.set_title("No dated trips")
        return fig

    freq_map = {"Month": "ME", "Week": "W", "Day": "D"}
    freq = freq_map.get(group_by, "ME")
    dated = dated.set_index("tripdate").sort_index()

    counts = dated.resample(freq).size()
    dist_km = _clean(dated.get("trip_distance", pd.Series(dtype=float))).resample(freq).sum() / 1000.0 if "trip_distance" in dated.columns else pd.Series(dtype=float)

    ax_count.bar(counts.index, counts.values, width={"ME": 20, "W": 5, "D": 0.8}.get(group_by, 20), color="#4C72B0", alpha=0.8)
    ax_count.set_title(f"Trips per {group_by.lower()}")
    ax_count.set_ylabel("Trip count")
    ax_count.grid(axis="y", linestyle="--", alpha=0.4)
    _ax_date_fmt(ax_count, counts.reset_index().rename(columns={"tripdate": "tripdate"}), "tripdate")

    if not dist_km.empty:
        cumulative = dist_km.cumsum()
        ax_dist.fill_between(cumulative.index, cumulative.values, alpha=0.35, color="#55A868")
        ax_dist.plot(cumulative.index, cumulative.values, color="#2d6a4f", linewidth=1.5)
        ax_dist.set_title("Cumulative distance (km)")
        ax_dist.set_ylabel("km")
        ax_dist.grid(axis="y", linestyle="--", alpha=0.4)
        _ax_date_fmt(ax_dist, cumulative.reset_index().rename(columns={"tripdate": "tripdate"}), "tripdate")
    else:
        ax_dist.set_title("Distance data not available")
    return fig


def _build_speed_engine_figure(df: pd.DataFrame) -> matplotlib.figure.Figure:
    fig, axes = _make_figure(2, 2)

    dated = df.copy()
    dated["tripdate"] = pd.to_datetime(dated.get("tripdate"), errors="coerce")  # type: ignore
    dated = dated.dropna(subset=["tripdate"]).sort_values("tripdate")

    def _scatter(ax: Any, col: str, label: str, color: str) -> None:
        if col not in dated.columns:
            ax.set_title(f"{label} – not available")
            return
        y = _clean(dated[col]).dropna()
        x = dated.loc[y.index, "tripdate"]
        ax.scatter(x, y, s=14, alpha=0.6, color=color, linewidths=0)
        ax.set_title(label)
        ax.grid(linestyle="--", alpha=0.3)
        _ax_date_fmt(ax, pd.DataFrame({"tripdate": x}))

    def _hist(ax: Any, col: str, label: str, color: str, bins: int = 30) -> None:
        if col not in dated.columns:
            ax.set_title(f"{label} – not available")
            return
        vals = _clean(dated[col]).dropna()
        ax.hist(vals, bins=bins, color=color, alpha=0.75, edgecolor="white", linewidth=0.4)
        ax.set_title(label)
        ax.set_xlabel(label)
        ax.set_ylabel("Trips")
        ax.grid(axis="y", linestyle="--", alpha=0.3)

    _scatter(axes[0][0], "speedobdkmh_avg", "Avg OBD speed (km/h) over time", "#4C72B0")
    _scatter(axes[0][1], "speedobdkmh_max", "Max OBD speed (km/h) over time", "#C44E52")
    _hist(axes[1][0], "enginerpmrpm_avg", "Avg RPM distribution", "#8172B2")
    _hist(axes[1][1], "engineload_avg", "Avg engine load (%)", "#CCB974")

    return fig


def _build_fuel_figure(df: pd.DataFrame) -> matplotlib.figure.Figure:
    fig, axes = _make_figure(2, 2)

    dated = df.copy()
    dated["tripdate"] = pd.to_datetime(dated.get("tripdate"), errors="coerce")  # type: ignore
    dated = dated.dropna(subset=["tripdate"]).sort_values("tripdate")

    def _scatter(ax: Any, col: str, label: str, color: str) -> None:
        if col not in dated.columns:
            ax.set_title(f"{label} – not available")
            return
        y = _clean(dated[col]).dropna()
        x = dated.loc[y.index, "tripdate"]
        ax.scatter(x, y, s=14, alpha=0.6, color=color, linewidths=0)
        ax.set_title(label)
        ax.grid(linestyle="--", alpha=0.3)
        _ax_date_fmt(ax, pd.DataFrame({"tripdate": x}))

    def _hist(ax: Any, col: str, label: str, color: str, bins: int = 25) -> None:
        if col not in dated.columns:
            ax.set_title(f"{label} – not available")
            return
        vals = _clean(dated[col]).dropna()
        ax.hist(vals, bins=bins, color=color, alpha=0.75, edgecolor="white", linewidth=0.4)
        ax.set_title(label)
        ax.set_xlabel(label)
        ax.set_ylabel("Trips")
        ax.grid(axis="y", linestyle="--", alpha=0.3)

    _scatter(axes[0][0], "tripaveragekplkpl_avg", "Trip avg efficiency (km/L) over time", "#55A868")
    _scatter(axes[0][1], "fuelusedtripl_avg", "Avg fuel used per trip (L) over time", "#C44E52")
    _hist(axes[1][0], "enginecoolanttemperaturec_avg", "Avg coolant temp (°C)", "#4C72B0")
    _hist(axes[1][1], "ambientairtempc_avg", "Avg ambient temp (°C)", "#CCB974")

    return fig


def _build_distance_figure(df: pd.DataFrame) -> matplotlib.figure.Figure:
    fig, axes = _make_figure(1, 2)

    def _hist(ax: Any, col: str, label: str, unit: str, color: str, bins: int = 30) -> None:
        if col not in df.columns:
            ax.set_title(f"{label} – not available")
            return
        vals = _clean(df[col]).dropna()
        ax.hist(vals, bins=bins, color=color, alpha=0.75, edgecolor="white", linewidth=0.4)
        ax.set_title(label)
        ax.set_xlabel(unit)
        ax.set_ylabel("Trips")
        ax.grid(axis="y", linestyle="--", alpha=0.3)

    dist_col = "trip_distance" if "trip_distance" in df.columns else None
    if dist_col:
        dist_km = _clean(df[dist_col]) / 1000.0
        axes[0][0].hist(dist_km.dropna(), bins=30, color="#4C72B0", alpha=0.75, edgecolor="white", linewidth=0.4)
        axes[0][0].set_title("Trip distance distribution")
        axes[0][0].set_xlabel("km")
        axes[0][0].set_ylabel("Trips")
        axes[0][0].grid(axis="y", linestyle="--", alpha=0.3)
    else:
        axes[0][0].set_title("trip_distance – not available")

    time_col = "time" if "time" in df.columns else None
    if time_col:
        dur_min = _clean(df[time_col]) / 60.0
        axes[0][1].hist(dur_min.dropna(), bins=30, color="#8172B2", alpha=0.75, edgecolor="white", linewidth=0.4)
        axes[0][1].set_title("Trip duration distribution")
        axes[0][1].set_xlabel("minutes")
        axes[0][1].set_ylabel("Trips")
        axes[0][1].grid(axis="y", linestyle="--", alpha=0.3)
    else:
        axes[0][1].set_title("trip time – not available")

    return fig


def _build_map(df: pd.DataFrame) -> folium.Map:
    """Folium map with start-position heatmap and start/end markers."""
    starts = df.dropna(subset=["startlat", "startlon"])
    if starts.empty:
        center = [0.0, 0.0]
        zoom = 2
    else:
        center = [float(starts["startlat"].mean()), float(starts["startlon"].mean())]
        zoom = 7

    fmap = folium.Map(location=center, zoom_start=zoom, tiles="CartoDB positron")

    # Heatmap of trip start positions
    heat_data = [
        [float(r.startlat), float(r.startlon)]  # type: ignore
        for r in starts.itertuples()
        if -90 <= float(r.startlat) <= 90 and -180 <= float(r.startlon) <= 180  # type: ignore
    ]
    if heat_data:
        HeatMap(heat_data, radius=12, blur=18, min_opacity=0.3).add_to(fmap)

    # Circle markers coloured by trip_distance
    if "trip_distance" in df.columns:
        dist_vals = _clean(df["trip_distance"]).fillna(0)
        vmax = float(dist_vals.quantile(0.95)) or 1.0
        colormap = matplotlib.colormaps.get_cmap("RdYlGn")
        for row in df.dropna(subset=["startlat", "startlon"]).itertuples():
            lat, lon = float(row.startlat), float(row.startlon)  # type: ignore
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            dist = float(getattr(row, "trip_distance", 0) or 0)
            dist_km = dist / 1000.0
            norm = min(dist / vmax, 1.0)
            r, g, b, _ = colormap(norm)
            color = "#{:02x}{:02x}{:02x}".format(int(r * 255), int(g * 255), int(b * 255))
            tripdate_str = str(getattr(row, "tripdate", ""))[:10]
            folium.CircleMarker(
                location=[lat, lon],
                radius=4,
                color=color,
                fill=True,
                fill_opacity=0.7,
                tooltip=f"{tripdate_str} – {dist_km:.1f} km",
            ).add_to(fmap)

    return fmap


# ── main window class ─────────────────────────────────────────────────────────

class TripStatsWindow(QMainWindow):
    def __init__(self, args: Any, engine: Any, parent: Any = None) -> None:
        super().__init__(parent)
        self.args = args
        self.engine = engine
        self.setWindowTitle("Trip Statistics")
        self.resize(1400, 860)

        self._df: pd.DataFrame = pd.DataFrame()
        self._loader_thread: QThread | None = None
        self._loader_worker: _TripStatsLoader | None = None

        self.Session = sessionmaker(bind=self.engine)

        # ── central widget & top-level layout ─────────────────────────────
        central = QWidget()
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(4, 4, 4, 4)
        root_layout.setSpacing(4)

        # ── toolbar ───────────────────────────────────────────────────────
        toolbar = QWidget()
        toolbar.setMaximumHeight(32)
        tb_layout = QHBoxLayout(toolbar)
        tb_layout.setContentsMargins(2, 2, 2, 2)
        tb_layout.setSpacing(6)

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setFixedHeight(26)
        self.refresh_btn.clicked.connect(self._load_data)

        tb_layout.addWidget(QLabel("Group by:"))
        self.group_combo = QComboBox()
        self.group_combo.addItems(["Month", "Week", "Day"])
        self.group_combo.setFixedWidth(80)
        self.group_combo.currentTextChanged.connect(self._on_group_changed)
        tb_layout.addWidget(self.group_combo)
        tb_layout.addWidget(self.refresh_btn)

        self.status_label = QLabel("Loading…")
        tb_layout.addStretch()
        tb_layout.addWidget(self.status_label)
        root_layout.addWidget(toolbar)

        # ── main splitter (left summary | right charts+map) ───────────────
        main_splitter = QSplitter(Qt.Orientation.Horizontal)

        # ── left panel: summary cards + table ────────────────────────────
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(2, 2, 2, 2)
        left_layout.setSpacing(4)

        self.summary_label = QLabel("No data loaded.")
        self.summary_label.setWordWrap(True)
        self.summary_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        self.summary_label.setFont(QFont("Monospace", 8))
        self.summary_label.setMaximumHeight(160)
        left_layout.addWidget(self.summary_label)

        self.trips_table = QTableView()
        self.trips_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.trips_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.trips_table.setSortingEnabled(True)
        self.trips_table.verticalHeader().setVisible(False)
        self.trips_table.setFont(QFont("Monospace", 7))
        left_layout.addWidget(self.trips_table)

        main_splitter.addWidget(left_panel)

        # ── right panel: tab widget (charts + map) ────────────────────────
        self.chart_tabs = QTabWidget()

        # Timeline tab
        self._timeline_fig, _ = _make_figure(2, 1)
        self._timeline_canvas = FigureCanvas(self._timeline_fig)
        self._timeline_canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.chart_tabs.addTab(self._timeline_canvas, "Timeline")

        # Distance / Duration tab
        self._dist_fig, _ = _make_figure(1, 2)
        self._dist_canvas = FigureCanvas(self._dist_fig)
        self._dist_canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.chart_tabs.addTab(self._dist_canvas, "Distance & Duration")

        # Speed & Engine tab
        self._speed_fig, _ = _make_figure(2, 2)
        self._speed_canvas = FigureCanvas(self._speed_fig)
        self._speed_canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.chart_tabs.addTab(self._speed_canvas, "Speed & Engine")

        # Fuel tab
        self._fuel_fig, _ = _make_figure(2, 2)
        self._fuel_canvas = FigureCanvas(self._fuel_fig)
        self._fuel_canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.chart_tabs.addTab(self._fuel_canvas, "Fuel & Temp")

        # Map tab
        self._stats_map_canvas = FoliumMapView()
        self.chart_tabs.addTab(self._stats_map_canvas, "Map")

        main_splitter.addWidget(self.chart_tabs)
        main_splitter.setSizes([340, 1060])

        root_layout.addWidget(main_splitter)
        self.setCentralWidget(central)

        # load on open
        self._load_data()

    # ── data loading ──────────────────────────────────────────────────────────

    def _load_data(self) -> None:
        self.refresh_btn.setEnabled(False)
        self.status_label.setText("Loading…")

        if self._loader_thread is not None:
            try:
                if self._loader_thread.isRunning():
                    self.status_label.setText("Loading…")
                    return
            except RuntimeError:
                # Qt already deleted the underlying C++ object; clear stale refs.
                self._loader_thread = None
                self._loader_worker = None

        thread = QThread(self)
        worker = _TripStatsLoader(self.engine)
        worker.moveToThread(thread)

        thread.started.connect(worker.run)
        worker.finished.connect(self._on_data_loaded)
        worker.error.connect(self._on_load_error)
        worker.finished.connect(thread.quit)
        worker.error.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.error.connect(worker.deleteLater)
        thread.finished.connect(self._on_loader_thread_finished)
        thread.finished.connect(thread.deleteLater)

        self._loader_thread = thread
        self._loader_worker = worker
        thread.start()

    def _on_load_error(self, msg: str) -> None:
        self.status_label.setText(f"Error: {msg}")
        self.refresh_btn.setEnabled(True)

    def _on_loader_thread_finished(self) -> None:
        self._loader_thread = None
        self._loader_worker = None

    def _on_data_loaded(self, df: pd.DataFrame) -> None:
        self._df = df
        self.refresh_btn.setEnabled(True)
        self._refresh_all()

    def _on_group_changed(self, _: str) -> None:
        if not self._df.empty:
            self._refresh_timeline()

    # ── rendering ─────────────────────────────────────────────────────────────

    def _refresh_all(self) -> None:
        df = self._df
        if df.empty:
            self.status_label.setText("No data.")
            return

        self._refresh_summary(df)
        self._refresh_table(df)
        self._refresh_timeline()
        self._refresh_dist(df)
        self._refresh_speed(df)
        self._refresh_fuel(df)
        self._refresh_map(df)

        self.status_label.setText(f"{len(df)} trips loaded.")

    def _refresh_summary(self, df: pd.DataFrame) -> None:
        total = len(df)
        dist_km = (_clean(df["trip_distance"]) / 1000.0).sum() if "trip_distance" in df.columns else float("nan")
        dur_h = (_clean(df["time"]) / 3600.0).sum() if "time" in df.columns else float("nan")
        avg_dist = dist_km / total if total else float("nan")
        avg_spd = _clean(df.get("speedobdkmh_avg", pd.Series(dtype=float))).mean()
        avg_kpl = _clean(df.get("tripaveragekplkpl_avg", pd.Series(dtype=float))).mean()
        max_spd = _clean(df.get("speedobdkmh_max", pd.Series(dtype=float))).max()

        lines = [
            f"Trips:          {total:>8,}",
            f"Total distance: {dist_km:>8.1f} km",
            f"Total duration: {dur_h:>8.1f} h",
            f"Avg dist/trip:  {avg_dist:>8.1f} km",
            f"Avg speed:      {avg_spd:>8.1f} km/h",
            f"Avg efficiency: {avg_kpl:>8.2f} km/L",
            f"Max speed ever: {max_spd:>8.1f} km/h",
        ]
        self.summary_label.setText("\n".join(lines))

    def _refresh_table(self, df: pd.DataFrame) -> None:
        display_cols = [
            c for c in [
                "fileid", "tripdate", "trip_distance", "time",
                "speedobdkmh_avg", "speedobdkmh_max",
                "enginerpmrpm_avg", "engineload_avg",
                "tripaveragekplkpl_avg", "fuelusedtripl_avg",
                "enginecoolanttemperaturec_avg", "ambientairtempc_avg",
            ] if c in df.columns
        ]
        display_df = df[display_cols].copy()

        if "trip_distance" in display_df.columns:
            display_df["trip_distance"] = (_clean(display_df["trip_distance"]) / 1000.0).round(2)
        if "time" in display_df.columns:
            display_df["time"] = (_clean(display_df["time"]) / 60.0).round(1)

        for col in display_df.select_dtypes(include="number").columns:
            if col not in ("fileid",):
                display_df[col] = _clean(display_df[col]).round(2)

        display_df = display_df.rename(columns={
            "trip_distance": "dist_km",
            "time": "dur_min",
            "speedobdkmh_avg": "spd_avg",
            "speedobdkmh_max": "spd_max",
            "enginerpmrpm_avg": "rpm_avg",
            "engineload_avg": "load_avg",
            "tripaveragekplkpl_avg": "kpl_avg",
            "fuelusedtripl_avg": "fuel_L",
            "enginecoolanttemperaturec_avg": "coolant_C",
            "ambientairtempc_avg": "ambient_C",
        })

        model = PandasModel(display_df)
        self.trips_table.setModel(model)
        self.trips_table.resizeColumnsToContents()

    def _refresh_timeline(self) -> None:
        group_by = self.group_combo.currentText()
        new_fig = _build_timeline_figure(self._df, group_by)
        self._replace_figure(self._timeline_canvas, new_fig)
        self._timeline_fig = new_fig

    def _refresh_dist(self, df: pd.DataFrame) -> None:
        new_fig = _build_distance_figure(df)
        self._replace_figure(self._dist_canvas, new_fig)
        self._dist_fig = new_fig

    def _refresh_speed(self, df: pd.DataFrame) -> None:
        new_fig = _build_speed_engine_figure(df)
        self._replace_figure(self._speed_canvas, new_fig)
        self._speed_fig = new_fig

    def _refresh_fuel(self, df: pd.DataFrame) -> None:
        new_fig = _build_fuel_figure(df)
        self._replace_figure(self._fuel_canvas, new_fig)
        self._fuel_fig = new_fig

    def _refresh_map(self, df: pd.DataFrame) -> None:
        try:
            fmap = _build_map(df)
            html = fmap._repr_html_()
            self._stats_map_canvas.setHtml(html)
        except Exception as exc:
            logger.warning(f"TripStatsWindow map render failed: {exc} ({type(exc)})")

    @staticmethod
    def _replace_figure(canvas: FigureCanvas, new_fig: matplotlib.figure.Figure) -> None:
        """Swap the figure in an existing FigureCanvas in place."""
        canvas.figure = new_fig
        new_fig.set_canvas(canvas)
        canvas.draw_idle()
