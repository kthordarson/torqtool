import json
import time
from typing import Any, cast

import folium
from folium.utilities import JsCode
import pandas as pd
from loguru import logger
from sqlalchemy import text
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QSplitter, QHBoxLayout, QLabel,
    QComboBox, QFrame, QTableView, QAbstractItemView, QLineEdit, QPushButton,
    QFormLayout, QSpinBox, QDoubleSpinBox, QMessageBox, QCheckBox, QCompleter, QTabWidget, QHeaderView,
)
from PySide6.QtCore import Qt, QTimer, QThread, QItemSelectionModel, QStringListModel
from PySide6.QtGui import QCloseEvent, QFont

from .map_canvas import FoliumMapView
from .position_load_worker import PositionLoadWorker
from .position_table_model import PositionTableModel
from .pandas_model import PandasModel
from ._helpers import _ORPHAN_QTHREADS, _release_orphan_thread


class PositionManagerWindow(QMainWindow):
    def __init__(self, args, engine, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.setWindowTitle("Position Manager")
        self.resize(1240, 780)
        self.args = args
        if parent:
            self.parent_args = parent.args
        else:
            self.parent_args = type("Args", (), {"debug": False})()

        self._selected_row_index: int | None = None
        self._selected_row_indices: list[int] = []
        self._table_model: PositionTableModel | None = None
        self._full_bounds_latlon: tuple[float, float, float, float] | None = None  # (lat_min, lon_min, lat_max, lon_max)
        self._show_point_labels = True
        self._show_labeled_points = True
        self._visible_row_indices: set[int] = set()
        self._load_thread: QThread | None = None
        self._load_worker: PositionLoadWorker | None = None
        self._pending_close = False
        self._pending_close_started_at: float | None = None
        self._active_threads: set[QThread] = set()
        self._thread_registry: dict[int, dict[str, Any]] = {}
        self._restore_after_reload: dict[str, Any] | None = None
        self._min_count_filter = 0
        self._current_sort_column: int = -1
        self._current_sort_order: Qt.SortOrder = Qt.SortOrder.AscendingOrder
        self._applying_sort: bool = False
        self._label_filter_active: bool = False
        self._label_filter_text: str = ""
        self._hide_labeled_active: bool = False
        self._visible_on_map_filter_active: bool = False
        self._visible_map_bounds: tuple[float, float, float, float] | None = None
        self._show_selected_only_on_map: bool = False
        self._updating_selection: bool = False
        self._pending_pick_call: tuple[list[int], bool] | None = None
        self._group_mode: str = "label"
        self._table_font_size = max(6, min(14, int(getattr(parent, "_trip_table_font_size", 8)))) if parent is not None else 8
        self._grouped_positions_df = pd.DataFrame(
            columns=["label", "start_points", "end_points", "total_points", "total_count", "avg_latitude", "avg_longitude"]
        )
        self._grouped_sources: dict[str, list[int]] = {}
        self._suspend_view_refresh: bool = False
        self._pick_debounce_timer: QTimer = QTimer(self)
        self._pick_debounce_timer.setSingleShot(True)
        self._pick_debounce_timer.setInterval(80)
        self._pick_debounce_timer.timeout.connect(self._flush_pending_pick)
        self.df_positions = pd.DataFrame(
            columns=['pos_type', 'pos_id', 'latitude', 'longitude', 'count', 'label']
        )

        central = QWidget()
        main_layout = QVBoxLayout(central)
        h_splitter = QSplitter(Qt.Orientation.Horizontal)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(2, 2, 2, 2)
        left_layout.setSpacing(0)
        self.map_canvas = FoliumMapView()
        left_layout.addWidget(self.map_canvas, stretch=1)

        table_panel = QWidget()
        table_panel_layout = QVBoxLayout(table_panel)
        table_panel_layout.setContentsMargins(2, 2, 2, 2)
        table_panel_layout.setSpacing(2)

        self.positions_table = QTableView()
        self.positions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.positions_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.positions_table.setFont(QFont("Monospace", self._table_font_size))
        self.positions_table.setStyleSheet(
            "QTableView::item:selected {"
            "  background-color: #c6f6c6;"
            "  color: #1f1f1f;"
            "}"
        )

        grouped_tab = QWidget()
        grouped_layout = QVBoxLayout(grouped_tab)
        grouped_layout.setContentsMargins(2, 2, 2, 2)
        grouped_toolbar = QWidget()
        grouped_toolbar_layout = QHBoxLayout(grouped_toolbar)
        grouped_toolbar_layout.setContentsMargins(0, 0, 0, 0)
        grouped_toolbar_layout.addWidget(QLabel("Group by:"))
        self.group_mode_combo = QComboBox()
        self.group_mode_combo.addItem("Label (ignore type)", "label")
        self.group_mode_combo.addItem("Start labels", "start")
        self.group_mode_combo.addItem("End labels", "end")
        self.group_mode_combo.currentIndexChanged.connect(self._on_group_mode_changed)
        grouped_toolbar_layout.addWidget(self.group_mode_combo)
        grouped_toolbar_layout.addStretch()
        self.grouped_positions_table = QTableView()
        self.grouped_positions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.grouped_positions_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.grouped_positions_table.setSortingEnabled(True)
        self.grouped_positions_table.verticalHeader().setVisible(False)
        self.grouped_positions_table.setFont(QFont("Monospace", self._table_font_size))
        self.grouped_positions_table.setStyleSheet(
            "QTableView::item:selected {"
            "  background-color: #c6f6c6;"
            "  color: #1f1f1f;"
            "}"
        )
        grouped_layout.addWidget(grouped_toolbar)
        grouped_layout.addWidget(self.grouped_positions_table)

        self.table_tabs = QTabWidget()
        self.table_tabs.addTab(self.positions_table, "Positions")
        self.table_tabs.addTab(grouped_tab, "Grouped labels")
        table_panel_layout.addWidget(self.table_tabs)

        editor = QFrame()
        editor_layout = QVBoxLayout(editor)
        editor_layout.setContentsMargins(4, 4, 4, 4)
        editor_layout.setSpacing(4)

        self.selected_info = QLabel("Loading positions...")
        self.selected_info.setWordWrap(True)
        self.selected_info.setMaximumHeight(42)
        editor_layout.addWidget(self.selected_info)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setHorizontalSpacing(6)
        form.setVerticalSpacing(3)
        self.pos_type_combo = QComboBox()
        self.pos_type_combo.addItems(["start", "end"])
        self.pos_type_combo.setFixedWidth(88)
        self.pos_id_spin = QSpinBox()
        self.pos_id_spin.setRange(1, 2_147_483_647)
        self.pos_id_spin.setFixedWidth(112)
        self.lat_spin = QDoubleSpinBox()
        self.lat_spin.setDecimals(7)
        self.lat_spin.setRange(-90.0, 90.0)
        self.lat_spin.setFixedWidth(128)
        self.lon_spin = QDoubleSpinBox()
        self.lon_spin.setDecimals(7)
        self.lon_spin.setRange(-180.0, 180.0)
        self.lon_spin.setFixedWidth(128)
        self.count_spin = QSpinBox()
        self.count_spin.setRange(0, 10_000_000)
        self.count_spin.setFixedWidth(112)
        self.min_count_filter_spin = QSpinBox()
        self.min_count_filter_spin.setRange(0, 10_000_000)
        self.min_count_filter_spin.setValue(0)
        self.min_count_filter_spin.setToolTip("Only show rows with count >= this value")
        self.min_count_filter_spin.setFixedWidth(112)
        self.label_edit = QLineEdit()
        self.label_edit.setPlaceholderText("Location label")
        self.label_edit.setMaximumWidth(220)
        self._label_completer = QCompleter([], self)
        self._label_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self._label_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.label_edit.setCompleter(self._label_completer)
        self.label_filter_chk = QCheckBox("Filter table by label")
        self.label_filter_edit = QLineEdit()
        self.label_filter_edit.setPlaceholderText("Text to match (empty = has any label)")
        self.label_filter_edit.setEnabled(False)
        self.label_filter_edit.setFixedWidth(180)
        self.hide_labeled_chk = QCheckBox("Hide labeled")
        self.show_labeled_points_chk = QCheckBox("Hide labeled points")
        self.show_labeled_points_chk.setChecked(False)
        self.visible_on_map_chk = QCheckBox("Table: only points visible on map")
        self.visible_on_map_chk.setChecked(False)
        form.addRow("Type", self.pos_type_combo)
        form.addRow("ID", self.pos_id_spin)
        form.addRow("Latitude", self.lat_spin)
        form.addRow("Longitude", self.lon_spin)
        form.addRow("Count", self.count_spin)
        form.addRow("Min count (table)", self.min_count_filter_spin)
        form.addRow("Label", self.label_edit)
        editor_layout.addLayout(form)

        button_row = QHBoxLayout()
        button_row.setSpacing(4)
        self.refresh_btn = QPushButton("Refresh")
        self.new_btn = QPushButton("New")
        self.save_btn = QPushButton("Save")
        self.apply_label_btn = QPushButton("Apply label")
        self.delete_btn = QPushButton("Delete")
        for btn in (self.refresh_btn, self.new_btn, self.save_btn, self.apply_label_btn, self.delete_btn):
            btn.setFixedHeight(24)
        self.toggle_labels_btn = QPushButton("Labels on")
        self.toggle_labels_btn.setCheckable(True)
        self.toggle_labels_btn.setChecked(True)
        self.toggle_labels_btn.setFixedHeight(24)
        self.zoom_in_btn = QPushButton("Z in")
        self.zoom_out_step_btn = QPushButton("Z out")
        self.zoom_out_btn = QPushButton("Full")
        for btn in (self.zoom_in_btn, self.zoom_out_step_btn, self.zoom_out_btn):
            btn.setFixedSize(64, 24)
        self.selected_only_map_btn = QPushButton("Map: selected only")
        self.selected_only_map_btn.setCheckable(True)
        self.selected_only_map_btn.setChecked(False)
        self.selected_only_map_btn.setFixedHeight(24)
        self.sort_similar_btn = QPushButton("Sort by similar lat/lon")
        self.sort_similar_btn.setFixedHeight(24)
        self.reload_map_btn = QPushButton("Reload map")
        self.reload_map_btn.setFixedSize(88, 24)
        map_btn_row = QHBoxLayout()
        map_btn_row.setSpacing(4)
        map_btn_row.addWidget(self.toggle_labels_btn)
        map_btn_row.addWidget(self.selected_only_map_btn)
        map_btn_row.addWidget(self.zoom_in_btn)
        map_btn_row.addWidget(self.zoom_out_step_btn)
        map_btn_row.addWidget(self.zoom_out_btn)
        map_btn_row.addWidget(self.reload_map_btn)
        map_btn_row.addStretch()
        editor_layout.addLayout(map_btn_row)
        self.deselect_all_btn = QPushButton("Deselect all")
        self.deselect_all_btn.setFixedHeight(24)
        button_row.addWidget(self.refresh_btn)
        button_row.addWidget(self.new_btn)
        button_row.addWidget(self.save_btn)
        button_row.addWidget(self.apply_label_btn)
        button_row.addWidget(self.delete_btn)
        button_row.addWidget(self.sort_similar_btn)
        button_row.addWidget(self.deselect_all_btn)
        button_row.addStretch()
        editor_layout.addLayout(button_row)

        filter_row = QHBoxLayout()
        filter_row.setSpacing(6)
        filter_row.addWidget(self.hide_labeled_chk)
        filter_row.addWidget(self.show_labeled_points_chk)
        filter_row.addWidget(self.visible_on_map_chk)
        filter_row.addWidget(self.label_filter_chk)
        filter_row.addWidget(self.label_filter_edit)
        filter_row.addStretch()
        editor_layout.addLayout(filter_row)

        right_stack = QSplitter(Qt.Orientation.Vertical)
        right_stack.addWidget(left_panel)
        right_stack.addWidget(editor)
        right_stack.setCollapsible(0, False)
        right_stack.setCollapsible(1, False)
        right_stack.setSizes([900, 230])

        h_splitter.addWidget(table_panel)
        h_splitter.addWidget(right_stack)
        h_splitter.setCollapsible(0, False)
        h_splitter.setCollapsible(1, False)
        h_splitter.setSizes([420, 1020])
        main_layout.addWidget(h_splitter)
        self.setCentralWidget(central)

        self.map_canvas.bridge.point_clicked.connect(self._on_map_point_clicked)
        self.map_canvas.bridge.map_view_changed.connect(self._on_map_view_changed)
        self.refresh_btn.clicked.connect(self.load_positions)
        self.new_btn.clicked.connect(self._start_new_entry)
        self.save_btn.clicked.connect(self.save_entry)
        self.apply_label_btn.clicked.connect(self.apply_label_to_selected)
        self.delete_btn.clicked.connect(self.delete_entry)
        self.min_count_filter_spin.valueChanged.connect(self._on_min_count_filter_changed)
        self.show_labeled_points_chk.toggled.connect(self._on_toggle_labeled_points)
        self.zoom_in_btn.clicked.connect(self._zoom_in)
        self.zoom_out_step_btn.clicked.connect(self._zoom_out)
        self.zoom_out_btn.clicked.connect(self._zoom_full)
        self.toggle_labels_btn.toggled.connect(self._on_toggle_labels)
        self.selected_only_map_btn.toggled.connect(self._on_toggle_selected_only_map)
        self.sort_similar_btn.clicked.connect(self._sort_table_by_similar_latlon)
        self.reload_map_btn.clicked.connect(self._force_reload_basemap)
        self.positions_table.horizontalHeader().sortIndicatorChanged.connect(self._on_sort_indicator_changed)
        self.hide_labeled_chk.toggled.connect(self._on_label_filter_changed)
        self.visible_on_map_chk.toggled.connect(self._on_visible_on_map_filter_changed)
        self.label_filter_chk.toggled.connect(self._on_label_filter_changed)
        self.label_filter_edit.textChanged.connect(self._on_label_filter_changed)
        self.label_edit.returnPressed.connect(self._on_label_return_pressed)
        self.deselect_all_btn.clicked.connect(self._deselect_all)

        self.load_positions()

    def set_table_font_size(self, value: int):
        self._table_font_size = max(6, min(14, int(value)))
        font = QFont("Monospace", self._table_font_size)
        self.positions_table.setFont(font)
        self.grouped_positions_table.setFont(font)

    @staticmethod
    def _table_info(pos_type: str) -> tuple[str, str, str, str]:
        if pos_type == "start":
            return ("startpos", "startid", "latstart", "lonstart")
        return ("endpos", "endid", "latend", "lonend")

    def _set_table_model(self):
        filtered_df = self._filtered_positions_df()
        self._table_model = PositionTableModel(filtered_df)
        self.positions_table.setModel(self._table_model)
        hdr = self.positions_table.horizontalHeader()
        hdr.setStretchLastSection(False)
        hdr.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.positions_table.resizeColumnsToContents()
        if self.positions_table.model() is not None:
            col_count = self.positions_table.model().columnCount()
            if col_count > 0:
                self.positions_table.setColumnWidth(0, 120)
            if col_count > 1:
                self.positions_table.setColumnWidth(1, 54)
            if col_count > 2:
                self.positions_table.setColumnWidth(2, 64)
            if col_count > 3:
                self.positions_table.setColumnWidth(3, 82)
            if col_count > 4:
                self.positions_table.setColumnWidth(4, 82)
            if col_count > 5:
                self.positions_table.setColumnWidth(5, 64)
        self.positions_table.setSortingEnabled(True)
        self.positions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.positions_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        if self._current_sort_column >= 0:
            self._applying_sort = True
            try:
                self.positions_table.sortByColumn(self._current_sort_column, self._current_sort_order)
            finally:
                self._applying_sort = False
        if self.positions_table.selectionModel() is not None:
            self.positions_table.selectionModel().selectionChanged.connect(self._on_table_selection_changed)
        self._refresh_grouped_positions_table()

    def _filtered_positions_df(self) -> pd.DataFrame:
        filtered_df = self.df_positions
        if self._min_count_filter > 0 and not filtered_df.empty:
            filtered_df = filtered_df[filtered_df["count"] >= self._min_count_filter]
        if self._hide_labeled_active and not filtered_df.empty:
            filtered_df = filtered_df[self._unlabeled_mask(filtered_df["label"])]
        if self._label_filter_active and not filtered_df.empty:
            if self._label_filter_text:
                mask = filtered_df["label"].astype(str).str.contains(
                    self._label_filter_text, case=False, na=False, regex=False
                )
                filtered_df = filtered_df[mask]
            else:
                filtered_df = filtered_df[filtered_df["label"].astype(str).str.strip() != ""]
        if self._visible_on_map_filter_active and self._visible_map_bounds is not None and not filtered_df.empty:
            south, west, north, east = self._visible_map_bounds
            lat = pd.to_numeric(filtered_df["latitude"], errors="coerce")
            lon = pd.to_numeric(filtered_df["longitude"], errors="coerce")
            filtered_df = filtered_df[
                lat.between(south, north, inclusive="both") &
                lon.between(west, east, inclusive="both")
            ]
        return filtered_df

    @staticmethod
    def _unlabeled_mask(series: pd.Series) -> pd.Series:
        stripped = series.fillna("").astype(str).str.strip()
        return series.isna() | stripped.eq("") | stripped.str.casefold().eq("none")

    def _on_group_mode_changed(self, index: int):
        mode = str(self.group_mode_combo.currentData() or "label")
        self._group_mode = mode
        self._refresh_grouped_positions_table()

    def _refresh_grouped_positions_table(self):
        base = self._filtered_positions_df().copy()
        self._grouped_sources = {}
        if base.empty:
            self._grouped_positions_df = pd.DataFrame(
                columns=["label", "start_points", "end_points", "total_points", "total_count", "avg_latitude", "avg_longitude"]
            )
            self.grouped_positions_table.setModel(PandasModel(self._grouped_positions_df))
            return

        base["label_group"] = base["label"].fillna("").astype(str).str.strip()
        base.loc[base["label_group"] == "", "label_group"] = "(no label)"
        if self._group_mode == "start":
            base = base[base["pos_type"] == "start"]
        elif self._group_mode == "end":
            base = base[base["pos_type"] == "end"]

        if base.empty:
            self._grouped_positions_df = pd.DataFrame(
                columns=["label", "start_points", "end_points", "total_points", "total_count", "avg_latitude", "avg_longitude"]
            )
            self.grouped_positions_table.setModel(PandasModel(self._grouped_positions_df))
            return

        rows: list[dict[str, Any]] = []
        for label, group in base.groupby("label_group", dropna=False):
            self._grouped_sources[str(label)] = [int(v) for v in group.index.tolist()]
            rows.append(
                {
                    "label": str(label),
                    "start_points": int((group["pos_type"] == "start").sum()),
                    "end_points": int((group["pos_type"] == "end").sum()),
                    "total_points": int(len(group)),
                    "total_count": round(float(pd.to_numeric(group["count"], errors="coerce").fillna(0).sum()), 2),
                    "avg_latitude": round(float(pd.to_numeric(group["latitude"], errors="coerce").mean()), 6),
                    "avg_longitude": round(float(pd.to_numeric(group["longitude"], errors="coerce").mean()), 6),
                }
            )

        self._grouped_positions_df = pd.DataFrame(
            rows,
            columns=["label", "start_points", "end_points", "total_points", "total_count", "avg_latitude", "avg_longitude"],
        )
        if not self._grouped_positions_df.empty:
            self._grouped_positions_df.sort_values(by="label", inplace=True)
            self._grouped_positions_df.reset_index(drop=True, inplace=True)

        model = PandasModel(self._grouped_positions_df)
        self.grouped_positions_table.setModel(model)
        ghdr = self.grouped_positions_table.horizontalHeader()
        ghdr.setStretchLastSection(False)
        ghdr.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.grouped_positions_table.resizeColumnsToContents()
        self.grouped_positions_table.setColumnWidth(0, 150)
        self.grouped_positions_table.setColumnWidth(1, 72)
        self.grouped_positions_table.setColumnWidth(2, 72)
        self.grouped_positions_table.setColumnWidth(3, 78)
        self.grouped_positions_table.setColumnWidth(4, 72)
        self.grouped_positions_table.setColumnWidth(5, 96)
        self.grouped_positions_table.setColumnWidth(6, 96)
        if self.grouped_positions_table.selectionModel() is not None:
            self.grouped_positions_table.selectionModel().selectionChanged.connect(self._on_grouped_selection_changed)

    def _on_grouped_selection_changed(self, selected, deselected):
        if self.grouped_positions_table.selectionModel() is None or self._grouped_positions_df.empty:
            return
        rows = self.grouped_positions_table.selectionModel().selectedRows()
        if not rows:
            return
        source_rows: list[int] = []
        for row in rows:
            view_idx = row.row()
            if view_idx < 0 or view_idx >= len(self._grouped_positions_df.index):
                continue
            label = str(self._grouped_positions_df.iloc[view_idx]["label"])
            source_rows.extend(self._grouped_sources.get(label, []))
        if not source_rows:
            return
        source_rows = list(dict.fromkeys(source_rows))
        self._select_rows_by_indices(source_rows, select_table=True, zoom_to_points=False)
        # Zoom map to fit all points in the selected group(s)
        group_df = self.df_positions.loc[self.df_positions.index.isin(source_rows)]
        lats = pd.to_numeric(group_df["latitude"], errors="coerce").dropna()
        lons = pd.to_numeric(group_df["longitude"], errors="coerce").dropna()
        if not lats.empty:
            if len(lats) == 1:
                self.map_canvas.set_view(float(lats.iloc[0]), float(lons.iloc[0]), 14)
            else:
                pad = 0.002
                self.map_canvas.zoom_full(
                    float(lats.min()) - pad, float(lons.min()) - pad,
                    float(lats.max()) + pad, float(lons.max()) + pad,
                )

    def _on_min_count_filter_changed(self, value: int):
        self._min_count_filter = int(value)
        self._set_table_model()
        if self._selected_row_indices:
            self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

    def _on_sort_indicator_changed(self, logical_index: int, order: Qt.SortOrder) -> None:
        if self._applying_sort:
            return
        self._current_sort_column = logical_index
        self._current_sort_order = order

    def _on_label_filter_changed(self) -> None:
        self._hide_labeled_active = self.hide_labeled_chk.isChecked()
        self._label_filter_active = self.label_filter_chk.isChecked()
        self._label_filter_text = self.label_filter_edit.text().strip()
        self.label_filter_edit.setEnabled(self._label_filter_active)
        self._set_table_model()
        if self._selected_row_indices:
            self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

    def _on_visible_on_map_filter_changed(self, checked: bool) -> None:
        self._visible_on_map_filter_active = bool(checked)
        if not self._visible_on_map_filter_active:
            self._visible_map_bounds = None
            self._set_table_model()
            if self._selected_row_indices:
                self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)
            return

        self.map_canvas.get_visible_bounds(self._on_visible_bounds_received)

    def _on_visible_bounds_received(self, bounds) -> None:
        self._visible_map_bounds = self._parse_bounds(bounds)
        self._set_table_model()
        if self._selected_row_indices:
            self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

    @staticmethod
    def _parse_bounds(bounds) -> tuple[float, float, float, float] | None:
        if not isinstance(bounds, dict):
            return None
        try:
            south_raw = bounds.get("south")
            west_raw = bounds.get("west")
            north_raw = bounds.get("north")
            east_raw = bounds.get("east")
            if south_raw is None or west_raw is None or north_raw is None or east_raw is None:
                return None
            south = float(south_raw)
            west = float(west_raw)
            north = float(north_raw)
            east = float(east_raw)
            return (south, west, north, east)
        except Exception as e:
            logger.debug(f"Unable to parse map bounds: {e} ({type(e)})")
            return None

    def _on_map_view_changed(self, data_str: str) -> None:
        if not self._visible_on_map_filter_active:
            return
        try:
            payload = json.loads(data_str)
        except Exception as e:
            logger.debug(f"Unable to parse map view changed payload: {e} ({type(e)})")
            return
        self._visible_map_bounds = self._parse_bounds(payload)
        self._set_table_model()
        if self._selected_row_indices:
            self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

    def _update_label_completer(self) -> None:
        labels = sorted(set(
            str(v) for v in self.df_positions["label"].dropna()
            if str(v).strip()
        ))
        self._label_completer.setModel(QStringListModel(labels, self._label_completer))

    def _on_label_return_pressed(self) -> None:
        if self._selected_row_indices:
            self.apply_label_to_selected()
        elif self._selected_row_index is not None:
            self.save_entry()

    def _deselect_all(self) -> None:
        self._selected_row_index = None
        self._selected_row_indices = []
        self._clear_selection_markers()
        if self._show_selected_only_on_map:
            self._plot_positions(preserve_view=True)
        if self.positions_table.selectionModel() is not None:
            self._updating_selection = True
            try:
                self.positions_table.selectionModel().clearSelection()
            finally:
                self._updating_selection = False
        self.selected_info.setText("Select a start/end point from the map or table")

    def _on_toggle_labeled_points(self, checked: bool):
        # Checkbox meaning: checked => hide labeled points.
        self._show_labeled_points = not bool(checked)
        self._plot_positions(preserve_view=True)
        if self._selected_row_indices:
            QTimer.singleShot(500, lambda: self._draw_selection_markers(self._selected_row_indices))

    def load_positions(self):
        self.selected_info.setText("Loading position data...")
        if self._thread_is_running(self._load_thread):
            self.selected_info.setText("Load already in progress")
            return

        thread = QThread()
        worker = PositionLoadWorker(self.engine.url.render_as_string(hide_password=False))
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(self._on_positions_loaded)
        worker.error.connect(self._on_positions_error)
        worker.finished.connect(thread.quit)
        worker.error.connect(thread.quit)
        thread.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._on_load_thread_finished)
        thread.finished.connect(lambda t=thread: self._active_threads.discard(t))
        thread.finished.connect(lambda t=thread: self._thread_registry.pop(id(t), None))
        self._load_thread = thread
        self._load_worker = worker
        self._active_threads.add(thread)
        self._register_thread(
            thread,
            owner_name="PositionManagerWindow",
            task_name="Positions load",
            launched_by="load_positions",
            worker=worker,
        )
        if self.args.debug:
            logger.debug(f"Started position load worker thread {thread}. active threads: {len(self._active_threads)}")
        thread.start()

    def _on_load_thread_finished(self):
        self._load_thread = None
        self._load_worker = None
        if self._pending_close and not self._any_worker_running():
            self._pending_close = False
            self.close()

    def _shutdown_thread(self, thread: QThread | None, name: str):
        if thread is None:
            return True
        try:
            if not thread.isRunning():
                return True
        except RuntimeError as e:
            logger.error(f"RuntimeError checking thread.isRunning() for '{name}': {e} ({type(e)})")
            return True
        if thread.currentThread() is thread:
            return False
        logger.debug(f"PositionManagerWindow requesting stop for thread '{name}'")
        thread.requestInterruption()
        thread.quit()
        if not thread.wait(3000):
            logger.warning(f"PositionManagerWindow thread '{name}' did not stop in time; terminating")
            thread.terminate()
            return bool(thread.wait(1000))
        return True

    def _detach_running_threads_for_close(self):
        threads: set[QThread] = set()
        if self._thread_is_running(self._load_thread):
            threads.add(cast(QThread, self._load_thread))
        for t in list(self._active_threads):
            if self._thread_is_running(t):
                threads.add(t)
        if self.args.debug:
            logger.debug(f"Detaching {len(threads)} threads")
        for t in threads:
            _ORPHAN_QTHREADS.add(t)
            try:
                t.finished.connect(lambda thr=t: _release_orphan_thread(thr))
            except RuntimeError as e:
                logger.error(f"RuntimeError connecting finished signal to orphan release: {e} ({type(e)})")
                _release_orphan_thread(t)
        self._active_threads.clear()
        self._load_thread = None
        self._load_worker = None

    def _any_worker_running(self) -> bool:
        if self._thread_is_running(self._load_thread):
            return True
        for t in list(self._active_threads):
            if self._thread_is_running(t):
                return True
        return False

    def _retry_pending_close(self):
        if not self._pending_close:
            return
        if self._any_worker_running():
            started = self._pending_close_started_at
            if started is not None and (time.monotonic() - started) >= 15.0:
                logger.warning("PositionManagerWindow forcing close; detaching still-running worker threads")
                self._detach_running_threads_for_close()
                self._pending_close = False
                self._pending_close_started_at = None
                self.close()
                return
            QTimer.singleShot(250, self._retry_pending_close)
            return
        self._pending_close = False
        self._pending_close_started_at = None
        self.close()

    def _on_positions_loaded(self, df: pd.DataFrame):
        self.df_positions = df.reset_index(drop=True)
        self._selected_row_index = None
        self._selected_row_indices = []
        self._clear_selection_markers()
        self._set_table_model()
        self._update_label_completer()
        self._plot_positions(preserve_view=True)

        restore_state = self._restore_after_reload
        self._restore_after_reload = None
        if restore_state:
            selected_keys = cast(list[dict[str, Any]] | None, restore_state.get("selected_keys"))

            if selected_keys and not self.df_positions.empty:
                matched_indices: list[int] = []
                for key in selected_keys:
                    k_type = str(key.get("pos_type", ""))
                    k_id = int(key.get("pos_id", 0))
                    if not k_type or k_id <= 0:
                        continue
                    matched = self.df_positions[
                        (self.df_positions["pos_type"] == k_type) &
                        (self.df_positions["pos_id"] == k_id)
                    ]
                    if not matched.empty:
                        matched_indices.append(int(matched.index[0]))
                if matched_indices:
                    self._select_rows_by_indices(matched_indices, select_table=True, zoom_to_points=False)
                    return

        self._start_new_entry()

    def _on_positions_error(self, error_message: str):
        logger.error(f'{self} {error_message}')
        QMessageBox.warning(self, "Load Failed", error_message)
        self.selected_info.setText("Failed to load positions")

    def _plot_positions(self, preserve_view: bool = False):
        if self.df_positions.empty:
            self._full_bounds_latlon = None
            self.map_canvas.show_empty("No start/end points available")
            if self.args.debug:
                logger.warning("Plotting positions: no start/end points available")
            return

        plot_df = self.df_positions
        if self._show_selected_only_on_map:
            if self._selected_row_indices:
                plot_df = plot_df.loc[plot_df.index.isin(self._selected_row_indices)]
            else:
                plot_df = plot_df.iloc[0:0]
        if plot_df.empty:
            self._full_bounds_latlon = None
            if self._show_selected_only_on_map:
                self.map_canvas.show_empty("No selected points")
            else:
                self.map_canvas.show_empty("No points for current filter")
            if self.args.debug:
                logger.warning("Plotting positions: no points to plot after filtering")
            return

        if not self._show_labeled_points:
            plot_df = plot_df[self._unlabeled_mask(plot_df["label"])]
            if self.args.debug and len(plot_df) == 0:
                logger.warning(f"Plotting positions: show_labeled_points is {self._show_labeled_points}, filtered out labeled points, remaining count: {len(plot_df)}")
            if self.args.debug and len(plot_df) > 0:
                logger.debug(f"Plotting positions: show_labeled_points is {self._show_labeled_points}, filtered out labeled points, remaining count: {len(plot_df)}")


        self._visible_row_indices = set(int(i) for i in plot_df.index.tolist())

        lat_min = float(plot_df["latitude"].min())
        lat_max = float(plot_df["latitude"].max())
        lon_min = float(plot_df["longitude"].min())
        lon_max = float(plot_df["longitude"].max())
        pad_lat = max(0.001, (lat_max - lat_min) * 0.06)
        pad_lon = max(0.001, (lon_max - lon_min) * 0.06)
        self._full_bounds_latlon = (lat_min - pad_lat, lon_min - pad_lon, lat_max + pad_lat, lon_max + pad_lon)

        clat = (lat_min + lat_max) / 2
        clon = (lon_min + lon_max) / 2
        try:
            m = folium.Map(location=[clat, clon], zoom_start=8)
        except ValueError as e:
            logger.error(f"Error creating folium Map: {e} ({type(e)})")
            m = folium.Map(location=[0, 0], zoom_start=2)
        m.fit_bounds([[lat_min - pad_lat, lon_min - pad_lon], [lat_max + pad_lat, lon_max + pad_lon]])

        # Build GeoJSON features
        selected_row_set = set(int(v) for v in self._selected_row_indices)
        features = []
        for idx, row in plot_df.iterrows():
            pos_type = str(row["pos_type"])
            pos_id = int(row["pos_id"])
            lat = float(row["latitude"])
            lon = float(row["longitude"])
            selected_label = False
            if isinstance(idx, int):
                selected_label = idx in selected_row_set
            elif isinstance(idx, str):
                idx_stripped = idx.strip()
                if idx_stripped.startswith("-"):
                    idx_digits = idx_stripped[1:]
                else:
                    idx_digits = idx_stripped
                if idx_digits.isdigit():
                    selected_label = int(idx_stripped) in selected_row_set
            count = max(1, int(row.get("count", 1)))
            label_raw = row.get("label", None)
            label_txt = "" if pd.isna(label_raw) else str(label_raw).strip()
            missing_label = (not label_txt) or label_txt.casefold() == "none"
            color = "blue" if pos_type == "start" else "red"
            radius = max(4.0, min(12.0, count * 0.5 + 4.0))
            tooltip_str = f"{pos_id}: {label_txt}" if not missing_label else f"{pos_id}: (no label)"
            if not self._show_point_labels:
                tooltip_str = str(pos_id)
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "row_index": int(idx),  # type: ignore
                    "pos_id": pos_id,
                    "pos_type": pos_type,
                    "color": color,
                    "radius": radius,
                    "tt": tooltip_str,
                    "always_label": bool(self._show_point_labels),
                    "missing_label": bool(missing_label),
                    "selected_label": bool(selected_label),
                },
            })

        on_each_feature = JsCode(
            "function(feature, layer) {"
            "  var p = feature.properties;"
            "  var cls = p.selected_label ? 'selected-label-tip' : (p.missing_label ? 'missing-label-tip' : '');"
            "  layer.bindTooltip(String(p.tt), {"
            "    permanent: !!p.always_label,"
            "    sticky: !p.always_label,"
            "    direction: 'top',"
            "    className: cls,"
            "    opacity: 0.92"
            "  });"
            "  layer.on('click', function(e) {"
            "    var d = JSON.stringify({row_index: p.row_index, pos_id: p.pos_id, pos_type: p.pos_type});"
            "    new QWebChannel(qt.webChannelTransport, function(ch) {"
            "      ch.objects.bridge.on_point_clicked(d);"
            "    });"
            "  });"
            "}"
        )
        geojson_layer = folium.GeoJson(
            {"type": "FeatureCollection", "features": features},
            marker=folium.CircleMarker(radius=6, fill=True),
            style_function=lambda f: {
                "fillColor": f["properties"]["color"],
                "color": f["properties"]["color"],
                "radius": f["properties"]["radius"],
                "weight": 1,
                "fill": True,
                "fillOpacity": 0.75,
            },
            name="positions",
            on_each_feature=on_each_feature,
        )
        geojson_layer.add_to(m)

        cast(Any, m.get_root()).header.add_child(folium.Element(
            "<style>"
            ".missing-label-tip {"
            "  background: #ffe770 !important;"
            "  border: 1px solid #c1a400 !important;"
            "  color: #1f1f1f !important;"
            "  font-weight: 600;"
            "}"
            ".selected-label-tip {"
            "  background: #c6f6c6 !important;"
            "  border: 1px solid #5aa45a !important;"
            "  color: #1f1f1f !important;"
            "  font-weight: 600;"
            "}"
            "</style>"
        ))

        self.map_canvas.display_map(m, preserve_view=preserve_view)

    def _on_map_point_clicked(self, data_str: str) -> None:
        try:
            data = json.loads(data_str)
        except Exception as e:
            logger.error(f"Error parsing point click data: {e} ({type(e)})")
            return
        row_index = int(data.get("row_index", -1))
        if row_index < 0 or row_index not in self.df_positions.index:
            return
        if row_index in self._selected_row_indices:
            pending_rows = [r for r in self._selected_row_indices if r != row_index] or [row_index]
        else:
            pending_rows = list(dict.fromkeys(self._selected_row_indices + [row_index]))
        self._select_rows_by_indices(pending_rows, select_table=True, zoom_to_points=(len(pending_rows) == 1))

    def _flush_pending_pick(self):
        if self._pending_pick_call is None:
            return
        rows, zoom = self._pending_pick_call
        self._pending_pick_call = None
        self._select_rows_by_indices(rows, select_table=True, zoom_to_points=zoom)

    def _on_table_selection_changed(self, selected, deselected):
        if self._updating_selection:
            return
        if self.positions_table.selectionModel() is None or self._table_model is None:
            return
        rows = self.positions_table.selectionModel().selectedRows()
        if not rows:
            self._selected_row_indices = []
            self._clear_selection_markers()
            self.selected_info.setText("Select a start/end point from the map or table")
            return

        source_rows: list[int] = []
        for row in rows:
            source_row = self._table_model.source_row_for_view_row(row.row())
            if source_row is not None:
                source_rows.append(source_row)
        if not source_rows:
            return
        self._select_rows_by_indices(source_rows, select_table=False, zoom_to_points=(len(source_rows) == 1))

    def _clear_selection_markers(self) -> None:
        self.map_canvas.clear_selection_markers()

    def _clear_point_labels(self) -> None:
        pass

    def _draw_point_labels(self, source_df=None) -> None:
        pass

    def _on_toggle_labels(self, checked: bool) -> None:
        self._show_point_labels = bool(checked)
        self.toggle_labels_btn.setText("Labels on" if checked else "Labels off")
        self._plot_positions(preserve_view=True)

    def _on_toggle_selected_only_map(self, checked: bool) -> None:
        self._show_selected_only_on_map = bool(checked)
        self._plot_positions(preserve_view=True)
        if self._selected_row_indices:
            self._draw_selection_markers(self._selected_row_indices)

    def _sort_table_by_similar_latlon(self):
        if self._table_model is None:
            return
        source_rows = self._table_model.source_rows()
        if not source_rows:
            return

        tmp = self.df_positions.loc[source_rows, ['latitude', 'longitude']].copy()
        tmp['lat_bucket'] = tmp['latitude'].round(3)
        tmp['lon_bucket'] = tmp['longitude'].round(3)
        tmp['_src'] = tmp.index
        tmp.sort_values(by=['lat_bucket', 'lon_bucket', 'latitude', 'longitude'], inplace=True, kind='mergesort')
        self._table_model.set_view_order([int(v) for v in tmp['_src'].tolist()])
        self._current_sort_column = -1
        self.positions_table.horizontalHeader().setSortIndicator(-1, Qt.SortOrder.AscendingOrder)
        if self._selected_row_indices:
            self._select_rows_by_indices(self._selected_row_indices, select_table=True, zoom_to_points=False)

    def _draw_selection_markers(self, row_indices: list[int]) -> None:
        self.map_canvas.clear_selection_markers()
        pairs: list[tuple[float, float]] = []
        for idx in row_indices:
            if idx not in self.df_positions.index:
                continue
            row = self.df_positions.loc[idx]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            pairs.append((float(row["latitude"]), float(row["longitude"])))
        if pairs:
            self.map_canvas.add_selection_markers(pairs)

    def _draw_selection_marker(self, x: float, y: float) -> None:
        # Backward compat wrapper
        self._draw_selection_markers(self._selected_row_indices or ([self._selected_row_index] if self._selected_row_index is not None else []))

    def _select_rows_by_indices(self, row_indices: list[int], select_table: bool, zoom_to_points: bool):
        clean_rows = [int(i) for i in row_indices if i in self.df_positions.index]
        if not clean_rows:
            return
        clean_rows = list(dict.fromkeys(clean_rows))
        self._selected_row_indices = clean_rows
        self._selected_row_index = clean_rows[0]

        if self._show_selected_only_on_map:
            self._plot_positions(preserve_view=True)

        if select_table and self.positions_table.selectionModel() is not None and self._table_model is not None:
            selection_model = self.positions_table.selectionModel()
            self._updating_selection = True
            try:
                selection_model.clearSelection()
                for idx, source_row in enumerate(clean_rows):
                    view_row = self._table_model.view_row_for_source_row(source_row)
                    if view_row is None:
                        if self.args.debug:
                            logger.warning(f"[{idx}/{len(clean_rows)}] Source row {source_row} not found in current table model for selection")
                        continue
                    model_index = self.positions_table.model().index(view_row, 0)
                    flags = QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows
                    selection_model.select(model_index, flags)
                    if idx == 0:
                        self.positions_table.scrollTo(model_index)
            finally:
                self._updating_selection = False

        rows_data = []
        for ridx in clean_rows:
            row = self.df_positions.loc[ridx]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            rows_data.append(cast(dict[str, Any], row.to_dict()))

        if len(rows_data) == 1:
            row_data = rows_data[0]
            self.pos_type_combo.setCurrentText(str(row_data.get('pos_type', 'start')))
            self.pos_id_spin.setValue(int(row_data.get('pos_id', 1)))
            self.lat_spin.setValue(float(row_data.get('latitude', 0.0)))
            self.lon_spin.setValue(float(row_data.get('longitude', 0.0)))
            self.count_spin.setValue(int(row_data.get('count', 0)))
            self.label_edit.setText(str(row_data.get('label', '')))
            self.selected_info.setText(
                f'Selected {str(row_data.get("pos_type", ""))} point #{int(row_data.get("pos_id", 0))}  |  '
                f'lat={float(row_data.get("latitude", 0.0)):.6f}, lon={float(row_data.get("longitude", 0.0)):.6f}, count={int(row_data.get("count", 0))}'
            )
            if zoom_to_points:
                self._zoom_to_point(float(row_data.get("latitude", 0.0)), float(row_data.get("longitude", 0.0)))
        else:
            labels = [str(row_entry.get("label", "")) for row_entry in rows_data]
            common_label = labels[0] if labels and all(label_value == labels[0] for label_value in labels) else ""
            self.label_edit.setText(common_label)
            self.selected_info.setText(f"Selected {len(rows_data)} points. Edit label and click 'Apply label to selected'.")

        self._draw_selection_markers(clean_rows)

    def _select_row_by_index(self, row_index: int, select_table: bool, zoom_to_point: bool):
        self._select_rows_by_indices([row_index], select_table=select_table, zoom_to_points=zoom_to_point)

    def _zoom_to_point(self, lat: float, lon: float) -> None:
        self.map_canvas.set_view(lat, lon, 14)

    def _zoom_in(self) -> None:
        self.map_canvas.zoom_in()

    def _zoom_out(self) -> None:
        self.map_canvas.zoom_out()

    def _zoom_full(self) -> None:
        if self._full_bounds_latlon is not None:
            self.map_canvas.zoom_full(*self._full_bounds_latlon)

    def _force_reload_basemap(self) -> None:
        self._plot_positions(preserve_view=True)

    def _start_new_entry(self):
        self._selected_row_index = None
        self._selected_row_indices = []
        self.pos_type_combo.setCurrentText("start")
        self.pos_id_spin.setValue(1)
        self.lat_spin.setValue(0.0)
        self.lon_spin.setValue(0.0)
        self.count_spin.setValue(0)
        self.label_edit.clear()
        self._clear_selection_markers()
        self.selected_info.setText("Create new start/end position entry")

    def apply_label_to_selected(self):
        selected_rows = self._selected_row_indices[:]
        if not selected_rows and self._selected_row_index is not None:
            selected_rows = [self._selected_row_index]
        if not selected_rows:
            QMessageBox.information(self, "No Selection", "Select one or more entries first.")
            return

        label = self.label_edit.text().strip()
        label_value = label if label else None
        rows_data: list[dict[str, Any]] = []
        for ridx in selected_rows:
            if ridx not in self.df_positions.index:
                continue
            row = self.df_positions.loc[ridx]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            rows_data.append(cast(dict[str, Any], row.to_dict()))

        if not rows_data:
            QMessageBox.information(self, "No Selection", "No valid rows selected.")
            return

        try:
            with self.engine.begin() as conn:
                for row_data in rows_data:
                    pos_type = str(row_data.get("pos_type", ""))
                    pos_id = int(row_data.get("pos_id", 0))
                    if pos_id <= 0 or pos_type not in ("start", "end"):
                        continue
                    table, id_col, _, _ = self._table_info(pos_type)
                    conn.execute(
                        text(f'UPDATE {table} SET label = :label WHERE {id_col} = :pos_id'),
                        {"label": label_value, "pos_id": pos_id},
                    )
        except Exception as e:
            logger.error(f"Failed to apply label to selected points: {e} ({type(e)})")
            QMessageBox.warning(self, "Update Failed", f"Could not apply label:\n{e}")
            return

        self._restore_after_reload = {
            "selected_keys": [
                {"pos_type": str(r.get("pos_type", "")), "pos_id": int(r.get("pos_id", 0))}
                for r in rows_data
            ],
        }
        self.load_positions()

    def save_entry(self):
        pos_type = self.pos_type_combo.currentText()
        pos_id = int(self.pos_id_spin.value())
        lat = float(self.lat_spin.value())
        lon = float(self.lon_spin.value())
        count = int(self.count_spin.value())
        label = self.label_edit.text().strip()
        label_value = label if label else None

        table, id_col, lat_col, lon_col = self._table_info(pos_type)
        old_type = None
        old_id = None
        if self._selected_row_index is not None and self._selected_row_index in self.df_positions.index:
            old_row = self.df_positions.loc[self._selected_row_index]
            if isinstance(old_row, pd.DataFrame):
                old_row = old_row.iloc[0]
            old_data = cast(dict[str, Any], old_row.to_dict())
            old_type = str(old_data.get("pos_type", ""))
            old_id = int(old_data.get("pos_id", 0))

        try:
            with self.engine.begin() as conn:
                if old_type and old_id and (old_type != pos_type or old_id != pos_id):
                    old_table, old_id_col, _, _ = self._table_info(old_type)
                    conn.execute(text(f'DELETE FROM {old_table} WHERE {old_id_col} = :pos_id'), {"pos_id": old_id})

                conn.execute(text(f'DELETE FROM {table} WHERE {id_col} = :pos_id'), {"pos_id": pos_id})
                conn.execute(
                    text(
                        f'INSERT INTO {table} ({id_col}, {lat_col}, {lon_col}, count, label) '
                        f'VALUES (:pos_id, :lat, :lon, :count, :label)'
                    ),
                    {"pos_id": pos_id, "lat": lat, "lon": lon, "count": count, "label": label_value},
                )
        except Exception as e:
            logger.error(f"Failed to save position entry: {e} ({type(e)})")
            QMessageBox.warning(self, "Save Failed", f"Could not save entry:\n{e}")
            return

        self._restore_after_reload = {
            "selected_keys": [{"pos_type": pos_type, "pos_id": pos_id}],
        }
        self.load_positions()

    def delete_entry(self):
        if self._selected_row_index is None or self._selected_row_index not in self.df_positions.index:
            QMessageBox.information(self, "No Selection", "Select an entry first.")
            return
        row = self.df_positions.loc[self._selected_row_index]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        row_data = cast(dict[str, Any], row.to_dict())
        pos_type = str(row_data.get("pos_type", ""))
        pos_id = int(row_data.get("pos_id", 0))
        table, id_col, _, _ = self._table_info(pos_type)

        confirm = QMessageBox.question(
            self,
            "Delete Entry",
            f"Delete {pos_type} entry #{pos_id}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if confirm != QMessageBox.StandardButton.Yes:
            return

        try:
            with self.engine.begin() as conn:
                conn.execute(text(f'DELETE FROM {table} WHERE {id_col} = :pos_id'), {"pos_id": pos_id})
        except Exception as e:
            logger.error(f"Failed to delete position entry: {e} ({type(e)})")
            QMessageBox.warning(self, "Delete Failed", f"Could not delete entry:\n{e}")
            return

        QMessageBox.information(self, "Deleted", "Entry deleted.")
        self.load_positions()

    @staticmethod
    def _thread_is_running(thread: QThread | None) -> bool:
        if thread is None:
            return False
        try:
            return thread.isRunning()
        except RuntimeError as e:
            logger.warning(f"RuntimeError calling thread.isRunning(): {e} ({type(e)})")
            return False

    def _register_thread(
        self,
        thread: QThread,
        owner_name: str,
        task_name: str,
        launched_by: str,
        worker: object | None = None,
    ) -> None:
        launcher = str(launched_by)
        if "." not in launcher:
            launcher = f"{self.__class__.__module__}.{self.__class__.__name__}.{launcher}"
        self._thread_registry[id(thread)] = {
            "thread": thread,
            "thread_id": id(thread),
            "owner_name": str(owner_name),
            "task_name": str(task_name),
            "launched_by": launcher,
            "worker_name": type(worker).__name__ if worker is not None else "",
            "started_at": time.time(),
        }

    def get_running_tasks(self) -> list[dict[str, Any]]:
        tasks: list[dict[str, Any]] = []
        for meta in self._thread_registry.values():
            thread = cast(QThread | None, meta.get("thread"))
            if not self._thread_is_running(thread):
                continue
            entry = dict(meta)
            entry["running"] = True
            tasks.append(entry)
        return sorted(tasks, key=lambda item: float(item.get("started_at", 0.0)))

    def stop_tracked_task(self, thread_id: int) -> bool:
        meta = self._thread_registry.get(int(thread_id))
        if not meta:
            return False
        thread = cast(QThread | None, meta.get("thread"))
        task_name = str(meta.get("task_name") or f"thread_{thread_id}")
        return bool(self._shutdown_thread(thread, task_name))

    def closeEvent(self, event: QCloseEvent):
        if self._any_worker_running():
            if not self._pending_close:
                logger.warning("PositionManagerWindow close deferred until workers stop")
                self._pending_close_started_at = time.monotonic()
            elif self._pending_close_started_at is not None and (time.monotonic() - self._pending_close_started_at) >= 2.5:
                logger.warning("PositionManagerWindow close forcing detach of stuck worker threads")
                self._detach_running_threads_for_close()
                self._pending_close = False
                self._pending_close_started_at = None
                super().closeEvent(event)
                return
            self._pending_close = True
            self._shutdown_thread(self._load_thread, "positions_load")
            for idx, t in enumerate(list(self._active_threads)):
                self._shutdown_thread(t, f"active_{idx}")
            QTimer.singleShot(200, self._retry_pending_close)
            event.ignore()
            return
        self._pending_close = False
        self._pending_close_started_at = None
        super().closeEvent(event)
