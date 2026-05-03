from __future__ import annotations

from typing import Any

import pandas as pd
import folium
import matplotlib.figure
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from loguru import logger
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QSplitter,
    QComboBox,
    QTableView,
    QAbstractItemView,
    QPushButton,
)

from .map_canvas import FoliumMapView
from .pandas_model import PandasModel


class StartEndWindow(QMainWindow):
    def __init__(self, args, engine, parent=None):
        super().__init__(parent)
        self.args = args
        self.engine = engine
        self.setWindowTitle("Start/End Trip Groups")
        self.resize(1220, 760)

        self._group_mode = "pair"
        self._detail_df = pd.DataFrame()
        self._grouped_df = pd.DataFrame()
        self._group_to_fileids: dict[str, list[int]] = {}
        self._active_fileids: list[int] = []
        self._table_font_size = max(6, min(14, int(getattr(parent, "_trip_table_font_size", 8)))) if parent is not None else 8
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        central = QWidget()
        main_layout = QVBoxLayout(central)

        top_row_widget = QWidget()
        top_row = QHBoxLayout(top_row_widget)
        top_row.setContentsMargins(1, 1, 1, 1)
        top_row.setSpacing(4)
        top_row.addWidget(QLabel("Group by:"))
        self.group_mode_combo = QComboBox()
        self.group_mode_combo.addItem("Start + End pair", "pair")
        self.group_mode_combo.addItem("Start position", "start")
        self.group_mode_combo.addItem("End position", "end")
        self.group_mode_combo.setFixedWidth(170)
        self.group_mode_combo.currentIndexChanged.connect(self._on_group_mode_changed)
        top_row.addWidget(self.group_mode_combo)
        self.plot_selected_btn = QPushButton("Plot selected")
        self.plot_selected_btn.setFixedHeight(24)
        self.plot_selected_btn.clicked.connect(self._plot_selected_groups)
        top_row.addWidget(self.plot_selected_btn)
        top_row.addStretch()
        self.stats_label = QLabel("No group selected")
        top_row.addWidget(self.stats_label)
        top_row_widget.setMaximumHeight(30)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        self.groups_table = QTableView()
        self.groups_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.groups_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.groups_table.setSortingEnabled(True)
        self.groups_table.verticalHeader().setVisible(False)
        self.groups_table.setFont(QFont("Monospace", self._table_font_size))

        left_panel = QWidget()
        self.left_panel = left_panel
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(2, 2, 2, 2)
        left_layout.setSpacing(2)
        left_layout.addWidget(self.groups_table)
        left_layout.addWidget(top_row_widget)

        plot_panel = QWidget()
        plot_layout = QVBoxLayout(plot_panel)
        plot_layout.setContentsMargins(0, 0, 0, 0)
        plot_layout.setSpacing(0)

        # Map (top) — folium
        self.map_canvas = FoliumMapView()

        # Time series (bottom) — matplotlib
        self._timeseries_fig = matplotlib.figure.Figure(figsize=(8, 3))
        self.ax_time = self._timeseries_fig.add_subplot(1, 1, 1)
        self.timeseries_canvas = FigureCanvas(self._timeseries_fig)

        plot_splitter = QSplitter(Qt.Orientation.Vertical)
        plot_splitter.addWidget(self.map_canvas)
        plot_splitter.addWidget(self.timeseries_canvas)
        plot_splitter.setSizes([420, 220])
        plot_layout.addWidget(plot_splitter)

        splitter.addWidget(left_panel)
        splitter.addWidget(plot_panel)
        splitter.setSizes([420, 780])
        main_layout.addWidget(splitter)

        self.setCentralWidget(central)
        self.load_data()

    def set_table_font_size(self, value: int):
        self._table_font_size = max(6, min(14, int(value)))
        self.groups_table.setFont(QFont("Monospace", self._table_font_size))

    def _cancel_parent_background_plot_load(self, reason: str) -> None:
        parent = self.parent()
        if parent is None:
            return
        if hasattr(parent, "_invalidate_and_cancel_active_plot_load"):
            try:
                parent._invalidate_and_cancel_active_plot_load(reason)
            except Exception as e:
                logger.warning(f"Could not cancel parent background load: {e} ({type(e)})")

    def _selected_group_fileids(self) -> list[int]:
        selection_model = self.groups_table.selectionModel()
        model = self.groups_table.model()
        if selection_model is None or model is None:
            return []

        rows = selection_model.selectedRows()
        if not rows:
            rows = selection_model.selectedIndexes()
        if not rows and self.groups_table.currentIndex().isValid():
            rows = [self.groups_table.currentIndex()]
        if not rows:
            return []

        fileids: list[int] = []
        for row in rows:
            group_index = model.index(int(row.row()), 0)
            group_name = str(model.data(group_index, Qt.ItemDataRole.DisplayRole) or "")
            if not group_name:
                continue
            fileids.extend(self._group_to_fileids.get(group_name, []))
        return sorted(set(int(fid) for fid in fileids))

    def _preview_points_for_fileids(self, fileids: list[int]) -> None:
        self._active_fileids = list(fileids)
        self.ax_time.clear()
        if not fileids:
            self.stats_label.setText("No group selected")
            self.map_canvas.show_empty("Select one or more groups")
            self.timeseries_canvas.draw_idle()
            return

        df = self._detail_df[self._detail_df["fileid"].isin(fileids)].copy()
        if df.empty:
            self.map_canvas.show_empty("No data for selected group(s)")
            self.timeseries_canvas.draw_idle()
            return

        m = self._build_start_end_map(df)
        if m is not None:
            self.map_canvas.display_map(m)
        else:
            self.map_canvas.show_empty("No start/end coordinates available")

        distance_km = float(df["trip_distance_m"].fillna(0).sum()) / 1000.0
        self.stats_label.setText(f"Trips: {len(fileids)} | Total distance: {distance_km:.2f} km")
        self.timeseries_canvas.draw_idle()

    def _plot_selected_groups(self):
        if self.groups_table.selectionModel() is None or self._grouped_df.empty:
            self._preview_points_for_fileids([])
            return

        selected_fileids = self._selected_group_fileids()
        if not selected_fileids:
            self._preview_points_for_fileids([])
            return

        self._cancel_parent_background_plot_load("Start/End selection changed")

        parent = self.parent()
        if parent is not None and hasattr(parent, "_select_trips_by_fileids"):
            try:
                selected_ok = bool(
                    parent._select_trips_by_fileids(
                        selected_fileids,
                        "No visible trips match the selected start/end groups.",
                        force_async_plot=True,
                    )
                )
                if selected_ok:
                    self.stats_label.setText(
                        f"Loading {len(selected_fileids)} trip(s) in background..."
                    )
                    return
            except Exception as e:
                logger.warning(f"Could not route Start/End selection to parent: {e} ({type(e)})")

        if parent is not None and hasattr(parent, "_plot_for_start_end_fileids"):
            try:
                parent._plot_for_start_end_fileids(selected_fileids)
                self.stats_label.setText(f"{len(selected_fileids)} trip(s) plotted")
                return
            except Exception as e:
                logger.warning(f"Could not route Start/End plot to parent: {e} ({type(e)})")

        # Standalone fallback: keep the local Start/End plots behavior.
        self._plot_for_fileids(selected_fileids)

    def load_data(self) -> None:
        query = text(
            """
            SELECT fileid, startid, endid, latstart, lonstart, start_label, latend, lonend, end_label, tripdate, trip_time_s, trip_distance_m
            FROM trip_start_end_summary
            """
        )
        try:
            df = pd.read_sql(query, self.engine)
        except Exception as e:
            logger.error(f"Failed to load trip_start_end_summary: {e} ({type(e)})")
            df = pd.DataFrame(
                columns=[
                    "fileid",
                    "startid",
                    "endid",
                    "latstart",
                    "lonstart",
                    "start_label",
                    "latend",
                    "lonend",
                    "end_label",
                    "tripdate",
                    "trip_time_s",
                    "trip_distance_m",
                ]
            )

        df["start_label"] = df.get("start_label", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        df["end_label"] = df.get("end_label", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
        df.loc[df["start_label"] == "", "start_label"] = "(no start label)"
        df.loc[df["end_label"] == "", "end_label"] = "(no end label)"
        df["tripdate"] = pd.to_datetime(df.get("tripdate"), errors="coerce")
        df["trip_distance_m"] = pd.to_numeric(df.get("trip_distance_m"), errors="coerce")
        df["trip_time_s"] = pd.to_numeric(df.get("trip_time_s"), errors="coerce")
        self._detail_df = df
        self._refresh_group_table()

    @staticmethod
    def _safe_group_id(value: Any) -> int:
        if value is None or pd.isna(value):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to convert value to int: {value} ({e})")
            return 0

    def _group_key(self, row: pd.Series) -> str:
        start_id = self._safe_group_id(row.get("startid"))
        end_id = self._safe_group_id(row.get("endid"))
        if self._group_mode == "start":
            return f"S{start_id} {row.get('start_label', '')}"
        if self._group_mode == "end":
            return f"E{end_id} {row.get('end_label', '')}"
        return (
            f"S{start_id} {row.get('start_label', '')}"
            f" -> E{end_id} {row.get('end_label', '')}"
        )

    def _refresh_group_table(self) -> None:
        if self._detail_df.empty:
            self._grouped_df = pd.DataFrame(columns=["group", "trips", "distance_km", "avg_time_min", "latest_trip"])
            self.groups_table.setModel(PandasModel(self._grouped_df))
            self._group_to_fileids = {}
            return

        df = self._detail_df.copy()
        try:
            df["group"] = df.apply(self._group_key, axis=1)
        except ValueError as e:
            logger.error(f"Error applying group key function: {e} ({type(e)}) self._group_key={self._group_key} df columns={df.columns.tolist()} sample row={df.iloc[0].to_dict() if not df.empty else 'N/A'}")
            df["group"] = "(error grouping)"
        rows: list[dict[str, Any]] = []
        self._group_to_fileids = {}
        for group_name, group_df in df.groupby("group", dropna=False):
            fileids = sorted(set(int(v) for v in pd.to_numeric(group_df["fileid"], errors="coerce").dropna().astype(int).tolist()))
            self._group_to_fileids[str(group_name)] = fileids
            rows.append(
                {
                    "group": str(group_name),
                    "trips": len(fileids),
                    "distance_km": round(float(group_df["trip_distance_m"].fillna(0).sum()) / 1000.0, 2),
                    "avg_time_min": round(float(group_df["trip_time_s"].fillna(0).mean()) / 60.0, 2),
                    "latest_trip": group_df["tripdate"].max(),
                }
            )

        grouped_df = pd.DataFrame(rows, columns=["group", "trips", "distance_km", "avg_time_min", "latest_trip"])
        if not grouped_df.empty:
            grouped_df.sort_values(by=["trips", "distance_km"], ascending=[False, False], inplace=True)
            grouped_df["latest_trip"] = pd.to_datetime(grouped_df["latest_trip"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
            grouped_df.reset_index(drop=True, inplace=True)
        self._grouped_df = grouped_df
        self.groups_table.setModel(PandasModel(self._grouped_df))
        self.groups_table.horizontalHeader().setStretchLastSection(True)
        self.groups_table.resizeColumnsToContents()
        if self.groups_table.selectionModel() is not None:
            self.groups_table.selectionModel().selectionChanged.connect(self._on_group_selection_changed)

    def _on_group_mode_changed(self, index: int):
        self._cancel_parent_background_plot_load("Start/End grouping changed")
        self._group_mode = str(self.group_mode_combo.currentData() or "pair")
        self._refresh_group_table()
        self._preview_points_for_fileids([])

    def _on_group_selection_changed(self, selected, deselected):
        if self.groups_table.selectionModel() is None or self._grouped_df.empty:
            return
        self._cancel_parent_background_plot_load("Start/End row click changed")
        self._preview_points_for_fileids(self._selected_group_fileids())

    def _build_start_end_map(self, df: pd.DataFrame) -> folium.Map | None:
        start_df = df[df["latstart"].notna() & df["lonstart"].notna()]
        end_df = df[df["latend"].notna() & df["lonend"].notna()]

        all_lats, all_lons = [], []
        for _, r in start_df.iterrows():
            all_lats.append(float(r["latstart"]))
            all_lons.append(float(r["lonstart"]))
        for _, r in end_df.iterrows():
            all_lats.append(float(r["latend"]))
            all_lons.append(float(r["lonend"]))

        if not all_lats:
            return None

        clat = (min(all_lats) + max(all_lats)) / 2
        clon = (min(all_lons) + max(all_lons)) / 2
        m = folium.Map(location=[clat, clon], zoom_start=10)
        m.fit_bounds([[min(all_lats), min(all_lons)], [max(all_lats), max(all_lons)]])

        for _, r in start_df.iterrows():
            folium.CircleMarker(
                location=[float(r["latstart"]), float(r["lonstart"])],
                radius=5, color="blue", fill=True, fill_color="blue", fill_opacity=0.7,
            ).add_to(m)
        for _, r in end_df.iterrows():
            folium.CircleMarker(
                location=[float(r["latend"]), float(r["lonend"])],
                radius=5, color="red", fill=True, fill_color="red", fill_opacity=0.7,
            ).add_to(m)
        return m

    def _plot_for_fileids(self, fileids: list[int]) -> None:
        self._active_fileids = fileids
        self.ax_time.clear()
        if not fileids:
            self.stats_label.setText("No group selected")
            self.map_canvas.show_empty("Select one or more groups")
            self.timeseries_canvas.draw_idle()
            return

        df = self._detail_df[self._detail_df["fileid"].isin(fileids)].copy()
        if df.empty:
            self.map_canvas.show_empty("No data for selected group(s)")
            self.timeseries_canvas.draw_idle()
            return

        # Map part — folium
        m = self._build_start_end_map(df)
        if m is not None:
            self.map_canvas.display_map(m)
        else:
            self.map_canvas.show_empty("No start/end coordinates available")

        # Time series part — matplotlib
        time_df = df[df["tripdate"].notna()].copy()
        if not time_df.empty:
            time_df.sort_values(by="tripdate", inplace=True)
            self.ax_time.plot(
                time_df["tripdate"],
                time_df["trip_distance_m"].fillna(0) / 1000.0,
                linestyle="-",
                marker="o",
                markersize=2,
                linewidth=0.8,
            )
            self.ax_time.set_title("Trip distance over time")
            self.ax_time.set_ylabel("Distance (km)")
            self.ax_time.set_xlabel("Trip date")
            self._timeseries_fig.autofmt_xdate(rotation=30)

        distance_km = float(df["trip_distance_m"].fillna(0).sum()) / 1000.0
        self.stats_label.setText(f"Trips: {len(fileids)} | Total distance: {distance_km:.2f} km")
        self.timeseries_canvas.draw_idle()
