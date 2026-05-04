import io
import json
import time
from typing import Any, cast
import numpy as np
import pandas as pd
import folium
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import matplotlib.colors as mcolors
from loguru import logger
from sqlalchemy import text, inspect
from sqlalchemy.orm import sessionmaker
from PySide6.QtWidgets import (
	QMainWindow, QTableView, QVBoxLayout, QWidget, QSplitter,
	QHBoxLayout, QLabel, QComboBox, QScrollArea, QFileDialog, QMessageBox,
	QSlider, QLineEdit, QPushButton, QInputDialog, QAbstractItemView, QTabWidget, QSpinBox, QSizePolicy,
)
from PySide6.QtGui import QFont, QAction, QCloseEvent
from PySide6.QtCore import Qt, QTimer, QThread, QItemSelectionModel
from PySide6.QtWidgets import QHeaderView

from schemas import dataschema
from metric_analysis import categorize_metric, get_analysis_suggestion, group_metrics_by_category
from .map_canvas import FoliumMapView
from .time_series_canvas import TimeSeriesCanvas
from .trip_list_worker import TripListWorker
from .trip_plot_worker import TripPlotWorker
from .position_manager_window import PositionManagerWindow
from .start_end_window import StartEndWindow
from .pandas_model import PandasModel
from .tasks_window import TasksWindow
from ._helpers import _normalize_col_name, format_duration, _ORPHAN_QTHREADS, _release_orphan_thread


class MainWindow(QMainWindow):
	def __init__(self, args, engine):
		super().__init__()
		self.args = args
		self.engine = engine
		self.setWindowTitle("TorqFiles Viewer")
		self._torqlogs_norm_to_actual = self._build_torqlogs_column_map()
		self._resolved_torqlogs_columns = {
			name: actual
			for name in ['latitude', 'longitude', 'speedobdkmh']
			if (actual := self._resolve_actual_torqlogs_column(name)) is not None
		}
		self._trip_plot_cache: dict[tuple[int, str], dict[str, list]] = {}
		self._trip_geo_cache: dict[int, dict[str, list]] = {}
		self._selection_stats_cache: dict[tuple[int, ...], tuple[dict, dict[str, dict[str, float]]]] = {}
		self._map_cache_version = "v2"
		self._current_colormap = 'Set1'
		self._fill_map_panel = True
		self._dot_size_scale = 1.0
		self._plot_refresh_timer = QTimer(self)
		self._plot_refresh_timer.setSingleShot(True)
		self._plot_refresh_timer.timeout.connect(self.refresh_plot)
		self._show_all_start_end_points = False
		self._start_end_overlay_data: list[dict[str, Any]] = []
		self._mw_full_bounds_latlon: tuple[float, float, float, float] | None = None
		self._mw_current_fileids: list[int] = []
		self._mw_last_metric: str = 'speedobdkmh'
		self._metric_summary_cache: dict[tuple[int, ...], pd.DataFrame] = {}
		self._metric_source_df = pd.DataFrame(columns=["name", "min", "max", "avg"])
		self._label_groups_df = pd.DataFrame(columns=["label", "start_points", "end_points", "total_points", "total_count"])
		self._start_end_points_df = pd.DataFrame(columns=["pos_type", "pos_id", "lat", "lon", "count", "label_group"])
		self._label_group_mode: str = "label"
		self._suppress_trip_selection_handler: bool = False
		self._initial_trips_thread: QThread | None = None
		self._initial_trips_worker: TripListWorker | None = None
		self._plot_data_thread: QThread | None = None
		self._plot_data_worker: TripPlotWorker | None = None
		self._plot_data_request_id = 0
		self._plot_async_threshold = 60
		self._force_next_plot_async = False
		self._suppress_metric_selection_handler = False
		self._plot_request_context: dict[int, dict[str, Any]] = {}
		self._preview_render_min_interval_s = 0.25
		self._active_threads: set[QThread] = set()
		self._thread_registry: dict[int, dict[str, Any]] = {}
		self._closing = False
		self._position_manager_window: PositionManagerWindow | None = None
		self._start_end_window: StartEndWindow | None = None
		self._task_window = None
		self._position_manager_embedded_widget: QWidget | None = None
		self._start_end_embedded_widget: QWidget | None = None
		self._positions_tab_container: QWidget | None = None
		self._start_end_tab_container: QWidget | None = None
		self._positions_tab_layout: QVBoxLayout | None = None
		self._start_end_tab_layout: QVBoxLayout | None = None
		self._all_trips_df = pd.DataFrame(columns=["id", "fileid", "trip_distance", "tripdate", "time", "trip_distance_sort", "tripdate_raw", "time_raw"])
		self._trip_table_font_size = 8
		self._point_sample_percent = 10
		self._trip_row_count_cache: dict[int, int] = {}
		self._sampling_target_points_per_trip = 12000
		self._bounds_padding_ratio = 0.06
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()
		self._ensure_map_cache_schema()

		# Set up UI
		splitter = QSplitter(Qt.Orientation.Horizontal)
		self.main_splitter = splitter
		self.table = QTableView()
		self.label_groups_table = QTableView()
		self.map_canvas = FoliumMapView()
		self.map_canvas.bridge.point_clicked.connect(self._on_map_point_clicked)
		self.timeseries_canvas = TimeSeriesCanvas()
		logger.debug(f"Resolved torqlogs columns: {self._resolved_torqlogs_columns}")

		# Zoom control
		zoom_widget = QWidget()
		main_layout = QHBoxLayout(zoom_widget)
		zoom_label = QLabel("Zoom:")
		self.zoom_combo = QComboBox()
		zoom_levels = [str(z) for z in range(10, 19)]
		self.zoom_combo.addItems(zoom_levels)
		self.zoom_combo.setCurrentText('10')
		self.zoom_combo.setFixedWidth(60)
		self.zoom_combo.setMaximumHeight(25)
		self.zoom_combo.currentTextChanged.connect(self.on_zoom_changed)
		dot_size_label = QLabel("Dot size:")
		self.toggle_all_start_end_btn = QPushButton("All points off")
		self.toggle_all_start_end_btn.setCheckable(True)
		self.toggle_all_start_end_btn.setChecked(False)
		self.toggle_all_start_end_btn.setFixedHeight(24)
		self.toggle_all_start_end_btn.toggled.connect(self._on_toggle_all_start_end)
		self.dot_size_slider = QSlider(Qt.Orientation.Horizontal)
		self.dot_size_slider.setMinimum(25)
		self.dot_size_slider.setMaximum(300)
		self.dot_size_slider.setValue(100)
		self.dot_size_slider.setSingleStep(5)
		self.dot_size_slider.setPageStep(25)
		self.dot_size_slider.setFixedWidth(140)
		self.dot_size_slider.valueChanged.connect(self.on_dot_size_changed)
		self.dot_size_value_label = QLabel("1.00x")
		self.dot_size_value_label.setFixedWidth(44)
		main_layout.addWidget(zoom_label)
		main_layout.addWidget(self.zoom_combo)
		main_layout.addWidget(dot_size_label)
		main_layout.addWidget(self.dot_size_slider)
		main_layout.addWidget(self.dot_size_value_label)
		main_layout.addWidget(self.toggle_all_start_end_btn)
		self._mw_zoom_in_btn = QPushButton("Z in")
		self._mw_zoom_in_btn.setFixedHeight(24)
		self._mw_zoom_in_btn.setFixedWidth(68)
		self._mw_zoom_in_btn.clicked.connect(self._mw_zoom_in)
		self._mw_zoom_out_btn = QPushButton("Z out")
		self._mw_zoom_out_btn.setFixedHeight(24)
		self._mw_zoom_out_btn.setFixedWidth(72)
		self._mw_zoom_out_btn.clicked.connect(self._mw_zoom_out)
		self._mw_zoom_full_btn = QPushButton("Full")
		self._mw_zoom_full_btn.setFixedHeight(24)
		self._mw_zoom_full_btn.setFixedWidth(44)
		self._mw_zoom_full_btn.clicked.connect(self._mw_zoom_full)
		main_layout.addWidget(self._mw_zoom_in_btn)
		main_layout.addWidget(self._mw_zoom_out_btn)
		main_layout.addWidget(self._mw_zoom_full_btn)
		self._mw_reload_map_btn = QPushButton("Reload map")
		self._mw_reload_map_btn.setFixedHeight(24)
		self._mw_reload_map_btn.setFixedWidth(84)
		self._mw_reload_map_btn.clicked.connect(self._mw_force_reload_basemap)
		main_layout.addWidget(self._mw_reload_map_btn)
		sample_label = QLabel("Pts %:")
		self.sample_percent_spin = QComboBox()
		self.sample_percent_spin.addItems(["5", "10", "50", "100"])
		self.sample_percent_spin.setCurrentText(str(self._point_sample_percent))
		self.sample_percent_spin.setFixedWidth(72)
		self.sample_percent_spin.setToolTip("Approximate percentage of torqlogs points to render")
		self.sample_percent_spin.currentTextChanged.connect(self._on_sampling_changed)
		self.sample_refresh_btn = QPushButton("Refresh")
		self.sample_refresh_btn.setFixedHeight(24)
		self.sample_refresh_btn.clicked.connect(lambda: self._plot_refresh_timer.start(50))
		padding_label = QLabel("Bds %:")
		self.bounds_padding_spin = QSpinBox()
		self.bounds_padding_spin.setRange(1, 30)
		self.bounds_padding_spin.setValue(int(self._bounds_padding_ratio * 100))
		self.bounds_padding_spin.setFixedWidth(72)
		self.bounds_padding_spin.setToolTip("Padding around trip bounds")
		self.bounds_padding_spin.valueChanged.connect(self._on_bounds_padding_changed)
		font_label = QLabel("font:")
		self.trip_table_font_spin = QSpinBox()
		self.trip_table_font_spin.setRange(6, 14)
		self.trip_table_font_spin.setValue(self._trip_table_font_size)
		self.trip_table_font_spin.setFixedWidth(72)
		self.trip_table_font_spin.valueChanged.connect(self._on_trip_table_font_size_changed)
		main_layout.addWidget(sample_label)
		main_layout.addWidget(self.sample_percent_spin)
		main_layout.addWidget(self.sample_refresh_btn)
		main_layout.addWidget(padding_label)
		main_layout.addWidget(self.bounds_padding_spin)
		main_layout.addWidget(font_label)
		main_layout.addWidget(self.trip_table_font_spin)
		main_layout.addStretch()
		main_layout.setSpacing(6)
		main_layout.setContentsMargins(6, 2, 6, 2)
		zoom_widget.setMaximumHeight(31)

		# Metric list (replaces QComboBox)
		metric_panel = QWidget()
		metric_panel_layout = QVBoxLayout(metric_panel)
		metric_panel_layout.setContentsMargins(2, 2, 2, 2)
		metric_title = QLabel("Metrics")
		metric_title_font = QFont()
		metric_title_font.setPointSize(9)
		metric_title_font.setBold(True)
		metric_title.setFont(metric_title_font)
		metric_filter_row = QWidget()
		metric_filter_layout = QHBoxLayout(metric_filter_row)
		metric_filter_layout.setContentsMargins(1, 1, 1, 1)
		metric_filter_layout.setSpacing(4)
		metric_filter_layout.addWidget(metric_title)
		self.metric_filter_edit = QLineEdit()
		self.metric_filter_edit.setPlaceholderText("Filter metric names")
		self.metric_filter_edit.setClearButtonEnabled(True)
		self.metric_filter_edit.setFixedWidth(220)
		self.metric_filter_edit.textChanged.connect(self._on_metric_filter_changed)
		metric_filter_layout.addWidget(self.metric_filter_edit)
		metric_filter_layout.addStretch()
		self.metric_table = QTableView()
		self.metric_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.metric_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		self.metric_table.setSortingEnabled(True)
		self.metric_table.verticalHeader().setVisible(False)
		mono_font = QFont("Monospace", 8)
		self.metric_table.setFont(mono_font)
		self._metric_df = pd.DataFrame(columns=["name", "min", "max", "avg"])
		self._set_metric_table_model(self._metric_df)
		metric_panel_layout.addWidget(metric_filter_row)
		metric_panel_layout.addWidget(self.metric_table)

		# Stats panel (scrollable)
		from PySide6.QtWidgets import QFrame
		stats_panel = QFrame()
		stats_layout = QVBoxLayout(stats_panel)
		stats_layout.setContentsMargins(4, 4, 4, 4)
		stats_title = QLabel("Selected Trip Stats")
		stats_title_font = QFont()
		stats_title_font.setPointSize(10)
		stats_title_font.setBold(True)
		stats_title.setFont(stats_title_font)
		self.stats_label = QLabel("No trip selected")
		self.stats_label.setWordWrap(True)
		self.stats_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
		stats_scroll = QScrollArea()
		stats_scroll.setWidget(self.stats_label)
		stats_scroll.setWidgetResizable(True)
		stats_left_panel = QWidget()
		stats_left_layout = QVBoxLayout(stats_left_panel)
		stats_left_layout.setContentsMargins(0, 0, 0, 0)
		stats_left_layout.setSpacing(2)
		stats_left_layout.addWidget(stats_title)
		stats_left_layout.addWidget(stats_scroll)

		all_metrics_title = QLabel("All Metrics")
		all_metrics_title_font = QFont()
		all_metrics_title_font.setPointSize(9)
		all_metrics_title_font.setBold(True)
		all_metrics_title.setFont(all_metrics_title_font)
		self.all_metrics_table = QTableView()
		self.all_metrics_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.all_metrics_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
		self.all_metrics_table.setSortingEnabled(True)
		self.all_metrics_table.verticalHeader().setVisible(False)
		self.all_metrics_table.setFont(QFont("Monospace", 7))
		self._all_metrics_df = pd.DataFrame(columns=["metric", "min", "avg", "max"])
		self._set_all_metrics_table_model(self._all_metrics_df)
		stats_right_panel = QWidget()
		stats_right_layout = QVBoxLayout(stats_right_panel)
		stats_right_layout.setContentsMargins(0, 0, 0, 0)
		stats_right_layout.setSpacing(2)
		stats_right_layout.addWidget(all_metrics_title)
		stats_right_layout.addWidget(self.all_metrics_table)

		stats_content_splitter = QSplitter(Qt.Orientation.Horizontal)
		stats_content_splitter.addWidget(stats_left_panel)
		stats_content_splitter.addWidget(stats_right_panel)
		stats_content_splitter.setSizes([430, 310])
		stats_layout.addWidget(stats_content_splitter)

		# Right panel: plots on top, metric list + stats below, zoom at bottom
		right_panel = QWidget()
		self.right_panel = right_panel
		right_layout = QVBoxLayout(right_panel)
		right_layout.setContentsMargins(0, 0, 0, 0)
		right_layout.setSpacing(2)

		plots_splitter = QSplitter(Qt.Orientation.Horizontal)
		plots_splitter.addWidget(self.map_canvas)
		plots_splitter.addWidget(self.timeseries_canvas)
		plots_splitter.setSizes([550, 450])

		lower_splitter = QSplitter(Qt.Orientation.Horizontal)
		lower_splitter.addWidget(metric_panel)
		lower_splitter.addWidget(stats_panel)
		lower_splitter.setSizes([200, 600])

		right_layout.addWidget(plots_splitter, stretch=6)
		right_layout.addWidget(lower_splitter, stretch=4)
		right_layout.addWidget(zoom_widget, stretch=0)

		# Make font a little smaller
		font = QFont()
		font.setPointSize(self._trip_table_font_size)
		self.table.setFont(font)
		self.label_groups_table.setFont(font)
		self.label_groups_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.label_groups_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		self.label_groups_table.setSortingEnabled(True)
		self.label_groups_table.verticalHeader().setVisible(False)

		label_tab = QWidget()
		label_tab_layout = QVBoxLayout(label_tab)
		label_tab_layout.setContentsMargins(2, 2, 2, 2)
		label_tab_layout.setSpacing(2)
		label_toolbar = QWidget()
		label_toolbar_layout = QHBoxLayout(label_toolbar)
		label_toolbar_layout.setContentsMargins(1, 1, 1, 1)
		label_toolbar_layout.setSpacing(4)
		label_toolbar_layout.addWidget(QLabel("Group by:"))
		self.label_group_mode_combo = QComboBox()
		self.label_group_mode_combo.addItem("Label (ignore type)", "label")
		self.label_group_mode_combo.addItem("Start labels", "start")
		self.label_group_mode_combo.addItem("End labels", "end")
		self.label_group_mode_combo.setFixedWidth(170)
		self.label_group_mode_combo.currentIndexChanged.connect(self._on_label_group_mode_changed)
		self.select_trips_by_labels_btn = QPushButton("Select trips by labels")
		self.select_trips_by_labels_btn.setFixedHeight(24)
		self.select_trips_by_labels_btn.clicked.connect(self._select_torqtrips_for_selected_labels)
		self.cancel_plot_load_btn = QPushButton("Cancel load")
		self.cancel_plot_load_btn.setEnabled(False)
		self.cancel_plot_load_btn.setFixedHeight(24)
		self.cancel_plot_load_btn.clicked.connect(self._cancel_async_plot_load)
		label_toolbar_layout.addWidget(self.label_group_mode_combo)
		label_toolbar_layout.addWidget(self.select_trips_by_labels_btn)
		label_toolbar_layout.addWidget(self.cancel_plot_load_btn)
		label_toolbar_layout.addStretch()
		label_toolbar.setMaximumHeight(30)
		label_tab_layout.addWidget(self.label_groups_table)
		label_tab_layout.addWidget(label_toolbar)

		self.left_tabs = QTabWidget()
		self.left_tabs.setDocumentMode(True)
		self.left_tabs.setTabPosition(QTabWidget.TabPosition.North)
		self.left_tabs.setMinimumWidth(0)
		self.left_tabs.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)

		self.trip_distance_min_filter = QLineEdit()
		self.trip_distance_min_filter.setPlaceholderText("min")
		self.trip_distance_min_filter.setFixedWidth(52)
		self.trip_distance_max_filter = QLineEdit()
		self.trip_distance_max_filter.setPlaceholderText("max")
		self.trip_distance_max_filter.setFixedWidth(52)
		self.trip_date_filter = QLineEdit()
		self.trip_date_filter.setPlaceholderText("YYYY-MM-DD or text")
		self.trip_date_filter.setFixedWidth(118)
		self.trip_time_filter = QLineEdit()
		self.trip_time_filter.setPlaceholderText("seconds")
		self.trip_time_filter.setFixedWidth(64)
		self.trip_filters_apply_btn = QPushButton("Apply")
		self.trip_filters_clear_btn = QPushButton("Clear")
		self.trip_filters_apply_btn.clicked.connect(self._apply_trip_filters)
		self.trip_filters_clear_btn.clicked.connect(self._clear_trip_filters)
		self.trip_distance_min_filter.returnPressed.connect(self._apply_trip_filters)
		self.trip_distance_max_filter.returnPressed.connect(self._apply_trip_filters)
		self.trip_date_filter.returnPressed.connect(self._apply_trip_filters)
		self.trip_time_filter.returnPressed.connect(self._apply_trip_filters)

		trip_filter_row1 = QWidget()
		trip_filter_row1_layout = QHBoxLayout(trip_filter_row1)
		trip_filter_row1_layout.setContentsMargins(2, 1, 2, 1)
		trip_filter_row1_layout.setSpacing(4)
		trip_filter_row1_layout.addWidget(QLabel("Distance km:"))
		trip_filter_row1_layout.addWidget(self.trip_distance_min_filter)
		trip_filter_row1_layout.addWidget(self.trip_distance_max_filter)
		trip_filter_row1_layout.addWidget(QLabel("Trip date:"))
		trip_filter_row1_layout.addWidget(self.trip_date_filter)
		trip_filter_row1_layout.addStretch()
		trip_filter_row1.setMaximumHeight(28)

		trip_filter_row2 = QWidget()
		trip_filter_row2_layout = QHBoxLayout(trip_filter_row2)
		trip_filter_row2_layout.setContentsMargins(2, 1, 2, 1)
		trip_filter_row2_layout.setSpacing(4)
		trip_filter_row2_layout.addWidget(QLabel("Time min:"))
		trip_filter_row2_layout.addWidget(self.trip_time_filter)
		trip_filter_row2_layout.addWidget(self.trip_filters_apply_btn)
		trip_filter_row2_layout.addWidget(self.trip_filters_clear_btn)
		trip_filter_row2_layout.addStretch()
		trip_filter_row2.setMaximumHeight(28)

		trip_filter_bar = QWidget()
		trip_filter_bar_layout = QVBoxLayout(trip_filter_bar)
		trip_filter_bar_layout.setContentsMargins(0, 0, 0, 0)
		trip_filter_bar_layout.setSpacing(1)
		trip_filter_bar_layout.addWidget(trip_filter_row1)
		trip_filter_bar_layout.addWidget(trip_filter_row2)
		trip_filter_bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)

		trips_tab = QWidget()
		trips_tab_layout = QVBoxLayout(trips_tab)
		trips_tab_layout.setContentsMargins(2, 2, 2, 2)
		trips_tab_layout.setSpacing(2)
		trips_tab_layout.addWidget(self.table)
		trips_tab_layout.addWidget(trip_filter_bar)
		self.left_tabs.addTab(trips_tab, "Trips")

		self._positions_tab_container = QWidget()
		self._positions_tab_layout = QVBoxLayout(self._positions_tab_container)
		self._positions_tab_layout.setContentsMargins(0, 0, 0, 0)
		self._positions_tab_layout.setSpacing(0)
		self._positions_tab_container.setMinimumWidth(0)
		self._positions_tab_container.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Expanding)

		self._start_end_tab_container = QWidget()
		self._start_end_tab_layout = QVBoxLayout(self._start_end_tab_container)
		self._start_end_tab_layout.setContentsMargins(0, 0, 0, 0)
		self._start_end_tab_layout.setSpacing(0)
		self._start_end_tab_container.setMinimumWidth(0)
		self._start_end_tab_container.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Expanding)

		self.left_tabs.addTab(self._start_end_tab_container, "Start/End")
		self.left_tabs.addTab(self._positions_tab_container, "Positions")
		self.left_tabs.addTab(label_tab, "Label groups")

		self.left_tabs.currentChanged.connect(self._on_left_tab_changed)

		splitter.addWidget(self.left_tabs)
		splitter.addWidget(right_panel)
		splitter.setSizes([150, 600])

		container = QWidget()
		layout = QVBoxLayout(container)
		layout.setContentsMargins(2, 2, 2, 2)
		layout.setSpacing(2)
		layout.addWidget(splitter)
		self.setCentralWidget(container)

		# Render immediately, then hydrate data/metrics after first paint.
		empty_df = pd.DataFrame(columns=['fileid', 'trip_distance', 'tripdate', 'time'])
		empty_df.index.name = 'id'
		self._set_table_model(empty_df)
		self._set_label_groups_table_model(self._label_groups_df)
		QTimer.singleShot(0, self._start_async_initial_trips_load)
		QTimer.singleShot(0, self._populate_label_groups_table)
		QTimer.singleShot(0, self._populate_metric_columns)
		self._create_menu_bar()
		logger.debug("MainWindow initialized and UI set up")

	def __repr__(self):
		return f"<MainWindow with {len(self.df_trips)} trips loaded>"

	def _create_menu_bar(self):
		menu_bar = self.menuBar()

		# File menu
		file_menu = menu_bar.addMenu("&File")
		open_action = QAction("&Open Database...", self)
		open_action.setShortcut("Ctrl+O")
		open_action.triggered.connect(self._open_database)
		file_menu.addAction(open_action)
		file_menu.addSeparator()
		export_action = QAction("&Export Stats...", self)
		export_action.triggered.connect(self._export_stats)
		file_menu.addAction(export_action)
		file_menu.addSeparator()
		exit_action = QAction("E&xit", self)
		exit_action.setShortcut("Ctrl+Q")
		exit_action.triggered.connect(self.close)
		file_menu.addAction(exit_action)

		# View menu — colormap submenu
		view_menu = menu_bar.addMenu("&View")
		colormap_menu = view_menu.addMenu("&Colormap")
		qualitative_maps = ['Set1', 'tab10', 'tab20', 'Dark2', 'Pastel1', 'Pastel2', 'Set2', 'Set3', 'Accent']
		sequential_maps = ['viridis', 'plasma', 'inferno', 'magma', 'Blues', 'Greens', 'Reds', 'YlOrRd']
		self._colormap_actions: dict[str, QAction] = {}
		for cmap_name in qualitative_maps + sequential_maps:
			action = QAction(cmap_name, self)
			action.setCheckable(True)
			action.setChecked(cmap_name == self._current_colormap)
			action.triggered.connect(lambda checked, name=cmap_name: self._set_colormap(name))
			colormap_menu.addAction(action)
			self._colormap_actions[cmap_name] = action
		view_menu.addSeparator()
		self._fill_map_panel_action = QAction("Fill &Trip Map Panel", self)
		self._fill_map_panel_action.setCheckable(True)
		self._fill_map_panel_action.setChecked(self._fill_map_panel)
		self._fill_map_panel_action.toggled.connect(self._set_fill_map_panel)
		view_menu.addAction(self._fill_map_panel_action)

		# Cache menu
		cache_menu = menu_bar.addMenu("&Cache")
		clear_map_cache_action = QAction("Clear &Map Image Cache", self)
		clear_map_cache_action.triggered.connect(self._clear_map_image_cache)
		cache_menu.addAction(clear_map_cache_action)

		# Tools menu
		tools_menu = menu_bar.addMenu("&Tools")
		position_manager_action = QAction("&Position manager", self)
		position_manager_action.triggered.connect(self._open_position_manager)
		tools_menu.addAction(position_manager_action)
		start_end_action = QAction("Start/&End grouped trips", self)
		start_end_action.triggered.connect(self._open_start_end_window)
		tools_menu.addAction(start_end_action)
		tasks_action = QAction("&Tasks", self)
		tasks_action.triggered.connect(self._open_tasks_window)
		tools_menu.addAction(tasks_action)

	def _open_tasks_window(self) -> None:
		if self._task_window is None:
			self._task_window = TasksWindow(self._collect_running_tasks, self._stop_task_from_monitor, self)
			self._task_window.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)
		self._task_window.show()
		self._task_window.setWindowState(self._task_window.windowState() & ~Qt.WindowState.WindowMinimized)
		self._task_window.raise_()
		self._task_window.activateWindow()

	def _open_position_manager(self):
		if hasattr(self, "left_tabs") and self.left_tabs is not None:
			self._ensure_positions_tab_embedded()
			self.left_tabs.setCurrentIndex(2)
			return
		if self._position_manager_window is None:
			self._position_manager_window = PositionManagerWindow(self.args, self.engine, self)
		self._position_manager_window.show()
		self._position_manager_window.raise_()
		self._position_manager_window.activateWindow()

	def _open_start_end_window(self):
		if hasattr(self, "left_tabs") and self.left_tabs is not None:
			self._ensure_start_end_tab_embedded()
			self.left_tabs.setCurrentIndex(1)
			return
		if self._start_end_window is None:
			self._start_end_window = StartEndWindow(self.args, self.engine, self)
		self._start_end_window.show()
		self._start_end_window.raise_()
		self._start_end_window.activateWindow()

	def _ensure_positions_tab_embedded(self):
		if self._position_manager_embedded_widget is not None:
			return
		if self._positions_tab_layout is None or self._positions_tab_container is None:
			return
		if self._position_manager_window is None:
			self._position_manager_window = PositionManagerWindow(self.args, self.engine, self)
			if hasattr(self._position_manager_window, "set_table_font_size"):
				self._position_manager_window.set_table_font_size(self._trip_table_font_size)
		embedded = self._position_manager_window.takeCentralWidget()
		if embedded is None:
			return
		embedded.setParent(self._positions_tab_container)
		embedded.setMinimumSize(0, 0)
		embedded.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Expanding)
		self._positions_tab_layout.addWidget(embedded)
		embedded.show()
		self._position_manager_embedded_widget = embedded

	def _ensure_start_end_tab_embedded(self):
		if self._start_end_embedded_widget is not None:
			return
		if self._start_end_tab_layout is None or self._start_end_tab_container is None:
			return
		if self._start_end_window is None:
			self._start_end_window = StartEndWindow(self.args, self.engine, self)
			if hasattr(self._start_end_window, "set_table_font_size"):
				self._start_end_window.set_table_font_size(self._trip_table_font_size)
		left_panel = self._start_end_window.left_panel
		left_panel.setParent(self._start_end_tab_container)
		left_panel.setMinimumSize(0, 0)
		left_panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
		self._start_end_tab_layout.addWidget(left_panel)
		left_panel.show()
		self._start_end_embedded_widget = left_panel

	def _open_database(self):
		path, _ = QFileDialog.getOpenFileName(self, "Open Database", "", "SQLite Database (*.db);;All Files (*)")
		if path:
			QMessageBox.information(self, "Open Database",
				f"Selected: {path}\n\nRestart with --dbfile \"{path}\" to switch databases.")

	def _export_stats(self):
		path, _ = QFileDialog.getSaveFileName(self, "Export Stats", "trip_stats.txt", "Text Files (*.txt);;All Files (*)")
		if path:
			try:
				with open(path, 'w', encoding='utf-8') as f:
					f.write(self.stats_label.text())
				logger.info(f"Stats exported to {path}")
			except Exception as e:
				logger.error(f"Export failed: {e}")

	def _clear_map_image_cache(self):
		confirm = QMessageBox.question(
			self,
			"Clear Cache",
			"Clear all rows from mapimagecache?\n\nThis will force map images to be regenerated.",
			QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
			QMessageBox.StandardButton.No,
		)
		if confirm != QMessageBox.StandardButton.Yes:
			return

		try:
			with self.engine.begin() as conn:
				conn.execute(text("DELETE FROM mapimagecache"))
			self._trip_plot_cache.clear()
			self._trip_geo_cache.clear()
			self._selection_stats_cache.clear()
			self._plot_refresh_timer.start(50)
			logger.info("Cleared mapimagecache and invalidated in-memory plot/stat caches")
			QMessageBox.information(self, "Cache Cleared", "Map image cache was cleared successfully.")
		except Exception as e:
			logger.error(f"Failed to clear mapimagecache: {e} ({type(e)})")
			QMessageBox.warning(self, "Cache Clear Failed", f"Could not clear mapimagecache:\n{e}")

	def _set_colormap(self, colormap_name: str):
		self._invalidate_and_cancel_active_plot_load("Color change requested")
		self._current_colormap = colormap_name
		for name, action in self._colormap_actions.items():
			action.setChecked(name == colormap_name)
		self._plot_refresh_timer.start(200)

	def _set_fill_map_panel(self, enabled: bool):
		self._fill_map_panel = bool(enabled)
		self._plot_refresh_timer.start(50)

	def on_zoom_changed(self, zoom_level):
		self._invalidate_and_cancel_active_plot_load("Zoom changed")
		self._plot_refresh_timer.start(300)

	def on_dot_size_changed(self, value: int):
		self._invalidate_and_cancel_active_plot_load("Dot size changed")
		self._dot_size_scale = float(value) / 100.0
		self.dot_size_value_label.setText(f"{self._dot_size_scale:.2f}x")
		self._plot_refresh_timer.start(120)

	def _sample_step(self) -> int:
		pct = max(1, min(100, int(self._point_sample_percent)))
		return max(1, int(round(100.0 / float(pct))))

	def _load_trip_row_count(self, fileid: int) -> int:
		cached = self._trip_row_count_cache.get(int(fileid))
		if cached is not None:
			return int(cached)
		count = 0
		try:
			q = text(
				"""
				SELECT COALESCE(sent_rows, 0)
				FROM torqfiles
				WHERE fileid = :fileid
				LIMIT 1
				"""
			)
			with self.engine.connect() as conn:
				row = conn.execute(q, {"fileid": int(fileid)}).first()
			if row and row[0] is not None:
				count = max(0, int(row[0]))
		except Exception as e:
			logger.error(f"Could not load torqfiles.sent_rows for fileid={fileid}: {e} ({type(e)})")
		if count <= 0:
			try:
				cq = text(
					"""
					SELECT COUNT(*)
					FROM torqlogs
					WHERE fileid = :fileid
					"""
				)
				with self.engine.connect() as conn:
					value = conn.execute(cq, {"fileid": int(fileid)}).scalar()
				if value is not None:
					count = max(0, int(value))
			except Exception as e:
				logger.warning(f"Could not count torqlogs rows for fileid={fileid}: {e} ({type(e)})")
				count = 0
		self._trip_row_count_cache[int(fileid)] = int(count)
		return int(count)

	def _adaptive_sample_step(self, row_count: int, base_step: int | None = None) -> int:
		step = max(1, int(base_step if base_step is not None else self._sample_step()))
		rows = max(0, int(row_count))
		if rows <= 0:
			return step
		max_points = max(1000, int(self._sampling_target_points_per_trip))
		if rows // step > max_points:
			step = max(step, int(np.ceil(rows / float(max_points))))
		return max(1, int(step))

	def _sample_step_for_fileid(self, fileid: int) -> int:
		row_count = self._load_trip_row_count(int(fileid))
		return self._adaptive_sample_step(row_count)

	def _sampling_cache_token(self, fileids: list[int]) -> str:
		if not fileids:
			return f"base={self._sample_step()}"
		parts = [f"{int(fid)}:{self._sample_step_for_fileid(int(fid))}" for fid in sorted(fileids)]
		return "|".join(parts)

	def _on_sampling_changed(self, value: int | str):
		self._invalidate_and_cancel_active_plot_load("Sampling changed")
		self._point_sample_percent = max(1, min(100, int(value)))
		self._trip_plot_cache.clear()
		self._trip_geo_cache.clear()
		self._trip_row_count_cache.clear()
		self._plot_refresh_timer.start(100)

	def _on_bounds_padding_changed(self, value: int):
		self._bounds_padding_ratio = max(0.01, min(0.30, float(value) / 100.0))
		self._plot_refresh_timer.start(120)

	def _on_trip_table_font_size_changed(self, value: int):
		self._trip_table_font_size = max(6, min(14, int(value)))
		font = self.table.font()
		font.setPointSize(self._trip_table_font_size)
		self.table.setFont(font)
		self.label_groups_table.setFont(font)
		self.metric_table.setFont(QFont("Monospace", max(6, self._trip_table_font_size - 1)))
		if self._position_manager_window is not None and hasattr(self._position_manager_window, "set_table_font_size"):
			self._position_manager_window.set_table_font_size(self._trip_table_font_size)
		if self._start_end_window is not None and hasattr(self._start_end_window, "set_table_font_size"):
			self._start_end_window.set_table_font_size(self._trip_table_font_size)

	def _safe_float_from_line_edit(self, edit: QLineEdit) -> float | None:
		text_value = edit.text().strip()
		if not text_value:
			return None
		try:
			return float(text_value)
		except ValueError as e:
			logger.warning(f"Could not parse float from '{text_value}': {e}")
			return None

	def _apply_trip_filters(self, auto_select_latest: bool = False):
		if self._all_trips_df.empty:
			return
		df = self._all_trips_df.copy()
		dmin = self._safe_float_from_line_edit(self.trip_distance_min_filter)
		dmax = self._safe_float_from_line_edit(self.trip_distance_max_filter)
		date_filter = self.trip_date_filter.text().strip().lower()
		time_min = self._safe_float_from_line_edit(self.trip_time_filter)
		if dmin is not None:
			df = df[df["trip_distance_sort"] >= dmin * 1000.0]
		if dmax is not None:
			df = df[df["trip_distance_sort"] <= dmax * 1000.0]
		if date_filter:
			df = df[df["tripdate"].astype(str).str.lower().str.contains(date_filter, na=False, regex=False)]
		if time_min is not None:
			df = df[df["time_raw"] >= time_min]
		self._set_table_model(df)
		if auto_select_latest and not self.df_trips.empty and self.table.selectionModel() is not None:
			model_index = self.table.model().index(0, 0)
			self.table.selectionModel().select(
				model_index,
				QItemSelectionModel.SelectionFlag.ClearAndSelect | QItemSelectionModel.SelectionFlag.Rows,
			)
			self.table.scrollTo(model_index)
			self._plot_refresh_timer.start(50)

	def _clear_trip_filters(self):
		self.trip_distance_min_filter.clear()
		self.trip_distance_max_filter.clear()
		self.trip_date_filter.clear()
		self.trip_time_filter.clear()
		self._trip_row_count_cache.clear()
		self._apply_trip_filters(auto_select_latest=False)

	def on_metric_selection_changed(self):
		if self._suppress_metric_selection_handler:
			return
		self._invalidate_and_cancel_active_plot_load("Metric selection changed")
		if self._get_selected_metrics():
			if self.left_tabs.currentIndex() == 3 and self._get_selected_label_groups():
				self._trigger_label_groups_trip_plot()
			else:
				self._plot_refresh_timer.start(200)

	def _on_toggle_all_start_end(self, checked: bool):
		self._show_all_start_end_points = bool(checked)
		self.toggle_all_start_end_btn.setText("All points on" if checked else "All points off")
		self._plot_refresh_timer.start(120)

	def _load_selected_file_start_end_points(self, fileids: list[int]) -> list[dict[str, Any]]:
		if not fileids:
			return []
		placeholders = ", ".join(f":fid{idx}" for idx in range(len(fileids)))
		params = {f"fid{idx}": int(fid) for idx, fid in enumerate(fileids)}
		q = text(
			f"""
			SELECT tf.fileid AS fileid, 'start' AS pos_type, sp.startid AS pos_id, sp.latstart AS lat, sp.lonstart AS lon, sp.label AS label
			FROM torqfiles tf
			LEFT JOIN startpos sp ON tf.startid = sp.startid
			WHERE tf.fileid IN ({placeholders})
			UNION ALL
			SELECT tf.fileid AS fileid, 'end' AS pos_type, ep.endid AS pos_id, ep.latend AS lat, ep.lonend AS lon, ep.label AS label
			FROM torqfiles tf
			LEFT JOIN endpos ep ON tf.endid = ep.endid
			WHERE tf.fileid IN ({placeholders})
			"""
		)
		with self.engine.connect() as conn:
			rows = conn.execute(q, params).mappings().all()
		result = []
		for row in rows:
			if row.get("lat") is None or row.get("lon") is None or row.get("pos_id") is None:
				continue
			result.append(dict(row))
		return result

	def _load_visible_start_end_points_latlon(
		self, bounds_latlon: tuple[float, float, float, float]
	) -> list[dict]:
		lat_min, lon_min, lat_max, lon_max = bounds_latlon
		q = text(
			"""
			SELECT 'start' AS pos_type, startid AS pos_id, latstart AS lat, lonstart AS lon, label
			FROM startpos WHERE latstart BETWEEN :lat_lo AND :lat_hi AND lonstart BETWEEN :lon_lo AND :lon_hi
			UNION ALL
			SELECT 'end' AS pos_type, endid AS pos_id, latend AS lat, lonend AS lon, label
			FROM endpos WHERE latend BETWEEN :lat_lo AND :lat_hi AND lonend BETWEEN :lon_lo AND :lon_hi
			"""
		)
		with self.engine.connect() as conn:
			rows = conn.execute(q, {"lat_lo": lat_min, "lat_hi": lat_max, "lon_lo": lon_min, "lon_hi": lon_max}).mappings().all()
		return [dict(r) for r in rows if r.get("lat") is not None and r.get("lon") is not None]

	def _overlay_start_end_points(
		self, m: folium.Map, fileids: list[int], bounds_latlon: tuple[float, float, float, float] | None
	) -> None:
		selected_points = self._load_selected_file_start_end_points(fileids)
		all_points: list[dict] = []
		if self._show_all_start_end_points and bounds_latlon is not None:
			all_points = self._load_visible_start_end_points_latlon(bounds_latlon)

		seen: set[tuple[str, int]] = set()
		merged: list[tuple[dict, bool]] = []
		for p in selected_points:
			key = (str(p.get("pos_type", "")), int(p.get("pos_id", 0)))
			if key not in seen:
				seen.add(key)
				merged.append((p, True))
		for p in all_points:
			key = (str(p.get("pos_type", "")), int(p.get("pos_id", 0)))
			if key not in seen:
				seen.add(key)
				merged.append((p, False))

		self._start_end_overlay_data = []
		features: list[dict] = []
		for point, is_selected_file in merged:
			lat = float(point.get("lat", 0.0))
			lon = float(point.get("lon", 0.0))
			pos_type = str(point.get("pos_type", ""))
			pos_id = int(point.get("pos_id", 0))
			label_text = str(point.get("label", "")).strip()
			prefix = "S" if pos_type == "start" else "E"
			full_label = f"{prefix}{pos_id}: {label_text}" if label_text else f"{prefix}{pos_id}"
			color = "green" if pos_type == "start" else "orange"
			size = 9 if is_selected_file else 5
			opacity = 1.0 if is_selected_file else 0.55
			features.append({
				"type": "Feature",
				"geometry": {"type": "Point", "coordinates": [lon, lat]},
				"properties": {
					"pos_type": pos_type,
					"pos_id": pos_id,
					"lat": lat,
					"lon": lon,
					"label": label_text,
					"tooltip": full_label,
					"color": color,
					"radius": size,
					"fillOpacity": opacity,
				},
			})
			self._start_end_overlay_data.append(dict(point))

		if features:
			on_each_feature = (
				"function(feature, layer) {"
				"  layer.on('click', function(e) {"
				"    e.originalEvent.stopPropagation();"
				"    var p = feature.properties;"
				"    var d = JSON.stringify({pos_type: p.pos_type, pos_id: p.pos_id, lat: p.lat, lon: p.lon, label: p.label});"
				"    new QWebChannel(qt.webChannelTransport, function(ch) {"
				"      ch.objects.bridge.on_point_clicked(d);"
				"    });"
				"  });"
				"}"
			)
			folium.GeoJson(
				{"type": "FeatureCollection", "features": features},
				marker=folium.CircleMarker(radius=6, fill=True),
				style_function=lambda f: {
					"fillColor": f["properties"]["color"],
					"color": f["properties"]["color"],
					"radius": f["properties"]["radius"],
					"weight": 1,
					"fill": True,
					"fillOpacity": f["properties"]["fillOpacity"],
				},
				tooltip=folium.GeoJsonTooltip(fields=["tooltip"], aliases=[""]),
				name="start_end_points",
				on_each_feature=on_each_feature,
			).add_to(m)

	def _get_selected_metrics(self) -> list[str]:
		selection_model = self.metric_table.selectionModel()
		if selection_model is None or self._metric_df.empty:
			return []
		rows = sorted(set(index.row() for index in selection_model.selectedRows()))
		result: list[str] = []
		for row in rows:
			if 0 <= row < len(self._metric_df.index):
				result.append(str(self._metric_df.iloc[row]["name"]))
		return result

	def _get_selected_metric(self) -> str:
		metrics = self._get_selected_metrics()
		if metrics:
			return metrics[0]

		if not self._metric_df.empty:
			return str(self._metric_df.iloc[0]["name"])

		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows())) if self.table.selectionModel() is not None else []
		fileids = self._get_selected_fileids(rows)
		valid_metrics = self._get_metric_columns_with_valid_data(fileids if fileids else None)
		return valid_metrics[0] if valid_metrics else ""

	def _set_metric_table_model(self, df: pd.DataFrame):
		self._metric_source_df = df.reset_index(drop=True)
		filter_text = self.metric_filter_edit.text().strip().casefold() if hasattr(self, "metric_filter_edit") else ""
		if filter_text and not self._metric_source_df.empty:
			self._metric_df = self._metric_source_df[
				self._metric_source_df["name"].astype(str).str.casefold().str.contains(filter_text, na=False, regex=False)
			].reset_index(drop=True)
		else:
			self._metric_df = self._metric_source_df.copy()
		self.metric_table_model = PandasModel(self._metric_df)
		self.metric_table.setModel(self.metric_table_model)
		self.metric_table.horizontalHeader().setStretchLastSection(True)
		self.metric_table.resizeColumnsToContents()
		selection_model = self.metric_table.selectionModel()
		if selection_model is not None:
			selection_model.selectionChanged.connect(lambda *_: self.on_metric_selection_changed())

	def _on_metric_filter_changed(self, text: str) -> None:
		prev_selected = set(self._get_selected_metrics())
		self._set_metric_table_model(self._metric_source_df)

		selection_model = self.metric_table.selectionModel()
		if selection_model is None:
			return

		self._suppress_metric_selection_handler = True
		try:
			selection_model.clearSelection()
			for row in range(len(self._metric_df.index)):
				metric_name = str(self._metric_df.iloc[row]["name"])
				if metric_name in prev_selected:
					self.metric_table.selectRow(row)
		finally:
			self._suppress_metric_selection_handler = False

	def _set_all_metrics_table_model(self, df: pd.DataFrame):
		self._all_metrics_df = df.reset_index(drop=True)
		display_columns = ["metric", "min", "avg", "max"]
		self.all_metrics_table_model = PandasModel(self._all_metrics_df, display_columns=display_columns)
		self.all_metrics_table.setModel(self.all_metrics_table_model)
		hdr = self.all_metrics_table.horizontalHeader()
		hdr.setStretchLastSection(False)
		hdr.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
		self.all_metrics_table.resizeColumnsToContents()
		if self.all_metrics_table.model() is not None:
			self.all_metrics_table.setColumnWidth(0, 186)
			self.all_metrics_table.setColumnWidth(1, 68)
			self.all_metrics_table.setColumnWidth(2, 68)
			self.all_metrics_table.setColumnWidth(3, 68)

	def _set_label_groups_table_model(self, df: pd.DataFrame):
		self._label_groups_df = df.reset_index(drop=True)
		display_columns = ["label", "start_points", "end_points", "total_points", "total_count"]
		self.label_groups_table_model = PandasModel(self._label_groups_df, display_columns=display_columns)
		self.label_groups_table.setModel(self.label_groups_table_model)
		hdr = self.label_groups_table.horizontalHeader()
		hdr.setStretchLastSection(False)
		hdr.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
		self.label_groups_table.resizeColumnsToContents()
		if self.label_groups_table.model() is not None:
			self.label_groups_table.setColumnWidth(0, 140)
			self.label_groups_table.setColumnWidth(1, 68)
			self.label_groups_table.setColumnWidth(2, 68)
			self.label_groups_table.setColumnWidth(3, 74)
			self.label_groups_table.setColumnWidth(4, 74)
		selection_model = self.label_groups_table.selectionModel()
		if selection_model is not None:
			selection_model.selectionChanged.connect(self._on_label_group_selection_changed)

	def _build_torqlogs_column_map(self) -> dict[str, str]:
		inspector = inspect(self.engine)
		actual_columns = [str(col["name"]) for col in inspector.get_columns("torqlogs")]
		return {_normalize_col_name(col): col for col in actual_columns}

	def _set_table_model(self, df: pd.DataFrame):
		self.df_trips = df
		display_columns = ["fileid", "trip_distance", "tripdate", "time"]
		sort_overrides: dict[str, str] = {}
		if 'trip_distance_sort' in self.df_trips.columns:
			sort_overrides['trip_distance'] = 'trip_distance_sort'
		if 'time_raw' in self.df_trips.columns:
			sort_overrides['time'] = 'time_raw'
		self.table_model = PandasModel(self.df_trips, display_columns=display_columns, sort_overrides=sort_overrides)
		self.table.setModel(self.table_model)
		font = self.table.font()
		font.setPointSize(self._trip_table_font_size)
		self.table.setFont(font)
		self.table.setSortingEnabled(True)
		self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		self.table.selectionModel().selectionChanged.connect(self.on_row_selected)
		hdr = self.table.horizontalHeader()
		hdr.setStretchLastSection(False)
		hdr.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
		self.table.setColumnWidth(0, 64)
		self.table.setColumnWidth(1, 92)
		self.table.setColumnWidth(2, 138)
		self.table.setColumnWidth(3, 84)
		logger.debug(f"Table model set with {len(df)} rows and {len(df.columns)} columns")

	def _ensure_map_cache_schema(self):
		inspector = inspect(self.engine)
		existing_columns = {str(col["name"]).lower() for col in inspector.get_columns("mapimagecache")}
		alter_statements = {
			"ext_west": 'ALTER TABLE mapimagecache ADD COLUMN ext_west DOUBLE PRECISION',
			"ext_east": 'ALTER TABLE mapimagecache ADD COLUMN ext_east DOUBLE PRECISION',
			"ext_south": 'ALTER TABLE mapimagecache ADD COLUMN ext_south DOUBLE PRECISION',
			"ext_north": 'ALTER TABLE mapimagecache ADD COLUMN ext_north DOUBLE PRECISION',
		}
		with self.engine.begin() as conn:
			for col_name, sql_stmt in alter_statements.items():
				if col_name not in existing_columns:
					conn.execute(text(sql_stmt))

	def _start_async_initial_trips_load(self):
		thread = QThread()
		worker = TripListWorker(self.engine.url.render_as_string(hide_password=False))
		worker.moveToThread(thread)

		thread.started.connect(worker.run)
		worker.finished.connect(self._on_initial_trips_loaded)
		worker.error.connect(self._on_initial_trips_error)
		worker.finished.connect(thread.quit)
		worker.error.connect(thread.quit)
		thread.finished.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)
		thread.finished.connect(lambda t=thread: self._active_threads.discard(t))
		thread.finished.connect(lambda t=thread: self._thread_registry.pop(id(t), None))
		thread.finished.connect(lambda t=thread: self._on_initial_trips_thread_finished(t))

		self._initial_trips_worker = worker
		self._initial_trips_thread = thread
		self._active_threads.add(thread)
		self._register_thread(
			thread,
			owner_name="MainWindow",
			task_name="Initial trips load",
			launched_by="_start_async_initial_trips_load",
			worker=worker,
		)
		if self.args.debug:
			logger.debug(f'Starting initial trips load in thread {thread} active threads: {len(self._active_threads)})')
		thread.start()

	def _on_initial_trips_loaded(self, df_trips: pd.DataFrame):
		logger.debug(f"Loaded {len(df_trips)} trips from database")
		df = df_trips.copy()
		df['trip_distance_sort'] = pd.to_numeric(df['trip_distance'], errors='coerce')
		df['tripdate_raw'] = pd.to_datetime(df['tripdate'], errors='coerce')
		df['time_raw'] = pd.to_numeric(df['time'], errors='coerce')
		df.sort_values(by=['tripdate_raw', 'fileid'], ascending=[False, False], inplace=True)
		df['tripdate'] = df['tripdate_raw'].dt.strftime('%Y-%m-%d %H:%M')
		df['time'] = df['time_raw'].apply(format_duration)
		df['trip_distance'] = df['trip_distance_sort'].apply(lambda x: f"{x/1000:.1f} km" if pd.notna(x) else "")
		df.set_index('id', inplace=True)
		self._all_trips_df = df
		self._apply_trip_filters(auto_select_latest=True)

	def _populate_label_groups_table(self):
		query = text(
			"""
			SELECT 'start' AS pos_type, startid AS pos_id, latstart AS lat, lonstart AS lon, count, label
			FROM startpos
			UNION ALL
			SELECT 'end' AS pos_type, endid AS pos_id, latend AS lat, lonend AS lon, count, label
			FROM endpos
			"""
		)
		try:
			df_points = pd.read_sql(query, self.engine)
		except Exception as e:
			logger.error(f"Failed to load label groups from start/end tables: {e} ({type(e)})")
			df_points = pd.DataFrame(columns=["pos_type", "pos_id", "lat", "lon", "count", "label"])

		if df_points.empty:
			self._start_end_points_df = pd.DataFrame(columns=["pos_type", "pos_id", "lat", "lon", "count", "label_group"])
			self._set_label_groups_table_model(pd.DataFrame(columns=["label", "start_points", "end_points", "total_points", "total_count"]))
			return

		df_points["label_group"] = df_points["label"].fillna("").astype(str).str.strip()
		df_points.loc[df_points["label_group"] == "", "label_group"] = "(no label)"
		df_points["count"] = pd.to_numeric(df_points["count"], errors="coerce").fillna(0).astype(float)
		df_points["lat"] = pd.to_numeric(df_points["lat"], errors="coerce")
		df_points["lon"] = pd.to_numeric(df_points["lon"], errors="coerce")
		df_points = df_points[df_points["lat"].notna() & df_points["lon"].notna()].copy()
		self._start_end_points_df = df_points

		if self._start_end_points_df.empty:
			self._set_label_groups_table_model(pd.DataFrame(columns=["label", "start_points", "end_points", "total_points", "total_count"]))
			return

		points_for_mode = self._start_end_points_df
		if self._label_group_mode == "start":
			points_for_mode = points_for_mode[points_for_mode["pos_type"] == "start"]
		elif self._label_group_mode == "end":
			points_for_mode = points_for_mode[points_for_mode["pos_type"] == "end"]

		if points_for_mode.empty:
			self._set_label_groups_table_model(pd.DataFrame(columns=["label", "start_points", "end_points", "total_points", "total_count"]))
			return

		grouped = points_for_mode.groupby("label_group", dropna=False)
		rows: list[dict[str, Any]] = []
		for label, group in grouped:
			start_points = int((group["pos_type"] == "start").sum())
			end_points = int((group["pos_type"] == "end").sum())
			total_points = int(len(group))
			total_count = int(group["count"].sum())
			rows.append(
				{
					"label": str(label),
					"start_points": start_points,
					"end_points": end_points,
					"total_points": total_points,
					"total_count": total_count,
				}
			)

		label_groups_df = pd.DataFrame(rows, columns=["label", "start_points", "end_points", "total_points", "total_count"])
		if not label_groups_df.empty:
			label_groups_df.sort_values(by="label", inplace=True)
			label_groups_df.reset_index(drop=True, inplace=True)
		self._set_label_groups_table_model(label_groups_df)

	def _on_label_group_mode_changed(self, index: int):
		if not hasattr(self, "label_group_mode_combo"):
			return
		mode = str(self.label_group_mode_combo.currentData() or "label")
		self._label_group_mode = mode
		self._populate_label_groups_table()

	def _on_label_group_selection_changed(self, selected, deselected):
		labels = self._get_selected_label_groups()
		if labels:
			self._plot_label_groups_on_map(labels)
			if self._get_selected_metrics():
				self._trigger_label_groups_trip_plot()

	def _trigger_label_groups_trip_plot(self) -> None:
		"""Silently select and async-plot trips for the currently selected label groups."""
		labels = self._get_selected_label_groups()
		if not labels:
			return

		placeholders = ", ".join(f":lbl{idx}" for idx in range(len(labels)))
		params = {f"lbl{idx}": label for idx, label in enumerate(labels)}

		label_expr_start = "COALESCE(NULLIF(TRIM(sp.label), ''), '(no label)')"
		label_expr_end = "COALESCE(NULLIF(TRIM(ep.label), ''), '(no label)')"
		if self._label_group_mode == "start":
			where_clause = f"{label_expr_start} IN ({placeholders})"
		elif self._label_group_mode == "end":
			where_clause = f"{label_expr_end} IN ({placeholders})"
		else:
			where_clause = f"({label_expr_start} IN ({placeholders}) OR {label_expr_end} IN ({placeholders}))"

		query = text(
			f"""
			SELECT DISTINCT tf.fileid AS fileid
			FROM torqfiles tf
			LEFT JOIN startpos sp ON tf.startid = sp.startid
			LEFT JOIN endpos ep ON tf.endid = ep.endid
			WHERE {where_clause}
			"""
		)
		try:
			with self.engine.connect() as conn:
				rows = conn.execute(query, params).mappings().all()
		except Exception as e:
			logger.error(f"Failed to query trips for label groups: {e} ({type(e)})")
			return

		fileids = sorted({int(r["fileid"]) for r in rows if r.get("fileid") is not None})
		if fileids:
			self._select_trips_by_fileids(fileids, "No visible trips match the selected labels.", force_async_plot=True)

	def _get_selected_label_groups(self) -> list[str]:
		selection_model = self.label_groups_table.selectionModel()
		if selection_model is None or self._label_groups_df.empty:
			return []
		rows = sorted(set(index.row() for index in selection_model.selectedRows()))
		selected_labels: list[str] = []
		for row in rows:
			if 0 <= row < len(self._label_groups_df.index):
				selected_labels.append(str(self._label_groups_df.iloc[row]["label"]))
		return selected_labels

	def _plot_label_groups_on_map(self, labels: list[str]) -> None:
		if not labels or self._start_end_points_df.empty:
			return

		points = self._start_end_points_df[self._start_end_points_df["label_group"].isin(labels)].copy()
		if self._label_group_mode == "start":
			points = points[points["pos_type"] == "start"]
		elif self._label_group_mode == "end":
			points = points[points["pos_type"] == "end"]
		if points.empty:
			return

		all_lat = pd.to_numeric(points["lat"], errors="coerce").dropna().tolist()
		all_lon = pd.to_numeric(points["lon"], errors="coerce").dropna().tolist()
		if not all_lat:
			return

		bounds = self._compute_plot_bounds_latlon(all_lat, all_lon)
		lat_min, lon_min, lat_max, lon_max = bounds or (min(all_lat), min(all_lon), max(all_lat), max(all_lon))
		clat = (lat_min + lat_max) / 2
		clon = (lon_min + lon_max) / 2
		m = folium.Map(location=[clat, clon], zoom_start=int(self.zoom_combo.currentText()))
		m.fit_bounds([[lat_min, lon_min], [lat_max, lon_max]])

		start_pts = points[points["pos_type"] == "start"]
		end_pts = points[points["pos_type"] == "end"]
		for _, r in start_pts.iterrows():
			folium.CircleMarker([float(r["lat"]), float(r["lon"])], radius=6, color="green",
								fill=True, fill_color="green", fill_opacity=0.75).add_to(m)
		for _, r in end_pts.iterrows():
			folium.CircleMarker([float(r["lat"]), float(r["lon"])], radius=6, color="orange",
								fill=True, fill_color="orange", fill_opacity=0.75).add_to(m)

		if bounds:
			self._mw_full_bounds_latlon = bounds
			self._mw_current_fileids = []
			self._mw_last_metric = "labels"

		self.map_canvas.display_map(m)
		self.timeseries_canvas.ax.clear()
		self.timeseries_canvas.draw_idle()
		self.stats_label.setText(f"Selected labels: {len(labels)}\nPoints shown: {len(points)}")

	def _select_torqtrips_for_selected_labels(self):
		labels = self._get_selected_label_groups()
		if not labels:
			QMessageBox.information(self, "No labels selected", "Select one or more labels in the Label Groups tab.")
			return

		if self.table.selectionModel() is None or self.df_trips.empty:
			return

		placeholders = ", ".join(f":lbl{idx}" for idx in range(len(labels)))
		params = {f"lbl{idx}": label for idx, label in enumerate(labels)}

		label_expr_start = "COALESCE(NULLIF(TRIM(sp.label), ''), '(no label)')"
		label_expr_end = "COALESCE(NULLIF(TRIM(ep.label), ''), '(no label)')"
		if self._label_group_mode == "start":
			where_clause = f"{label_expr_start} IN ({placeholders})"
		elif self._label_group_mode == "end":
			where_clause = f"{label_expr_end} IN ({placeholders})"
		else:
			where_clause = f"({label_expr_start} IN ({placeholders}) OR {label_expr_end} IN ({placeholders}))"

		query = text(
			f"""
			SELECT DISTINCT tf.fileid AS fileid
			FROM torqfiles tf
			LEFT JOIN startpos sp ON tf.startid = sp.startid
			LEFT JOIN endpos ep ON tf.endid = ep.endid
			WHERE {where_clause}
			"""
		)

		try:
			with self.engine.connect() as conn:
				rows = conn.execute(query, params).mappings().all()
		except Exception as e:
			logger.error(f"Failed to query torqtrips by labels: {e} ({type(e)})")
			QMessageBox.warning(self, "Query failed", f"Could not select trips by labels:\n{e}")
			return

		fileids = {int(r["fileid"]) for r in rows if r.get("fileid") is not None}
		if not fileids:
			QMessageBox.information(self, "No matches", "No trips match the selected labels.")
			return

		self._select_trips_by_fileids(sorted(fileids), "No visible trips match the selected labels.")

	def _select_trips_by_fileids(self, fileids: list[int], no_visible_message: str, force_async_plot: bool = False) -> bool:
		if not fileids:
			return False

		selection_model = self.table.selectionModel()
		if selection_model is None:
			return False

		table_df = self.df_trips.reset_index(drop=True)
		matching_rows = [
			int(idx)
			for idx, fid in enumerate(pd.to_numeric(table_df.get("fileid"), errors="coerce").fillna(-1).astype(int).tolist())
			if fid in fileids
		]
		if not matching_rows:
			QMessageBox.information(self, "No matches", no_visible_message)
			return False

		self._suppress_trip_selection_handler = True
		selection_model.blockSignals(True)
		self.table.setUpdatesEnabled(False)
		try:
			selection_model.clearSelection()
			flags = QItemSelectionModel.SelectionFlag.Select | QItemSelectionModel.SelectionFlag.Rows
			for row_idx in matching_rows:
				model_index = self.table.model().index(row_idx, 0)
				selection_model.select(model_index, flags)
		finally:
			self.table.setUpdatesEnabled(True)
			selection_model.blockSignals(False)
			self._suppress_trip_selection_handler = False

		self.left_tabs.setCurrentIndex(0)
		selected_fileids = self._get_selected_fileids(matching_rows)
		self._populate_metric_columns(selected_fileids if selected_fileids else None)
		if selected_fileids and force_async_plot:
			self._force_next_plot_async = True
			self._start_async_plot_for_fileids(selected_fileids)
		else:
			self._plot_refresh_timer.start(120)
		return True

	def _on_left_tab_changed(self, index: int):
		if hasattr(self, "right_panel") and self.right_panel is not None:
			self.right_panel.setVisible(index != 2)
		if hasattr(self, "main_splitter") and self.main_splitter is not None:
			sizes = self.main_splitter.sizes()
			if len(sizes) >= 2:
				total = max(1, sizes[0] + sizes[1])
				if index == 0:
					self.main_splitter.setSizes([320, max(900, total - 320)])
		if index == 0:
			rows = sorted(set(idx.row() for idx in self.table.selectionModel().selectedRows())) if self.table.selectionModel() is not None else []
			self._populate_metric_columns(self._get_selected_fileids(rows) if rows else None)
		elif index == 1:
			self._ensure_start_end_tab_embedded()
		elif index == 2:
			self._ensure_positions_tab_embedded()

	def _on_initial_trips_error(self, error_message: str):
		logger.error(error_message)

	def _populate_metric_columns(self, fileids: list[int] | None = None):
		prev_selected = set(self._get_selected_metrics())
		summary_df = self._get_metric_summary_for_selection(fileids)
		self._set_metric_table_model(summary_df)

		if self._metric_df.empty:
			logger.warning("No metrics with valid non-zero data were found for current selection")
			return

		selection_model = self.metric_table.selectionModel()
		if selection_model is None:
			return

		self._suppress_metric_selection_handler = True
		try:
			selection_model.clearSelection()
			restored_any = False
			for row in range(len(self._metric_df.index)):
				metric_name = str(self._metric_df.iloc[row]["name"])
				if metric_name in prev_selected:
					self.metric_table.selectRow(row)
					restored_any = True

			if not restored_any:
				self.metric_table.selectRow(0)
		finally:
			self._suppress_metric_selection_handler = False

		logger.debug(
			f"Populated metric table with {len(self._metric_df)} metrics "
			f"for {len(fileids) if fileids else 'all'} selected trips"
		)

	def _resolve_actual_torqlogs_column(self, requested_column: str) -> str | None:
		return self._torqlogs_norm_to_actual.get(_normalize_col_name(requested_column))

	def _get_metric_summary_for_selection(self, fileids: list[int] | None = None) -> pd.DataFrame:
		cache_key = tuple(sorted(int(fid) for fid in fileids)) if fileids else tuple()
		cached = self._metric_summary_cache.get(cache_key)
		if cached is not None:
			return cached.copy()

		requested = sorted(dataschema.keys())
		numeric_cols = self._get_torqlogs_numeric_columns()
		column_pairs: list[tuple[str, str]] = []
		for req in requested:
			actual = self._resolve_actual_torqlogs_column(req)
			if actual and actual in numeric_cols:
				column_pairs.append((req, actual))

		if not column_pairs:
			empty_df = pd.DataFrame(columns=["name", "min", "max", "avg"])
			self._metric_summary_cache[cache_key] = empty_df
			return empty_df.copy()

		select_parts: list[str] = []
		for idx, (_, actual_col) in enumerate(column_pairs):
			select_parts.append(
				f'SUM(CASE WHEN "{actual_col}" IS NOT NULL AND CAST("{actual_col}" AS FLOAT) <> 0 THEN 1 ELSE 0 END) AS "_c_{idx}"'
			)
			select_parts.append(
				f'MIN(CASE WHEN "{actual_col}" IS NOT NULL AND CAST("{actual_col}" AS FLOAT) <> 0 THEN CAST("{actual_col}" AS FLOAT) END) AS "_min_{idx}"'
			)
			select_parts.append(
				f'MAX(CASE WHEN "{actual_col}" IS NOT NULL AND CAST("{actual_col}" AS FLOAT) <> 0 THEN CAST("{actual_col}" AS FLOAT) END) AS "_max_{idx}"'
			)
			select_parts.append(
				f'AVG(CASE WHEN "{actual_col}" IS NOT NULL AND CAST("{actual_col}" AS FLOAT) <> 0 THEN CAST("{actual_col}" AS FLOAT) END) AS "_avg_{idx}"'
			)

		where_clause = ""
		if fileids:
			fileids_str = ",".join(str(int(fid)) for fid in sorted(fileids))
			where_clause = f" WHERE fileid IN ({fileids_str})"

		query = f"SELECT {', '.join(select_parts)} FROM torqlogs{where_clause}"
		rows: list[dict[str, float | str]] = []
		try:
			df = pd.read_sql(query, self.engine)
			if not df.empty:
				row = df.iloc[0]
				for idx, (requested_col, _) in enumerate(column_pairs):
					count_val = row.get(f"_c_{idx}")
					if count_val is None or pd.isna(count_val) or int(count_val) <= 0:
						continue
					min_val = row.get(f"_min_{idx}")
					max_val = row.get(f"_max_{idx}")
					avg_val = row.get(f"_avg_{idx}")
					if min_val is None or max_val is None or avg_val is None:
						continue
					if pd.isna(min_val) or pd.isna(max_val) or pd.isna(avg_val):
						continue
					rows.append({
						"name": requested_col,
						"min": float(min_val),
						"max": float(max_val),
						"avg": float(avg_val),
					})
		except Exception as e:
			logger.warning(f"Failed to evaluate metric summary for selection: {e} ({type(e)})")

		summary_df = pd.DataFrame(rows, columns=["name", "min", "max", "avg"])
		if not summary_df.empty:
			summary_df.sort_values(by="name", inplace=True)
			summary_df.reset_index(drop=True, inplace=True)
			summary_df[["min", "max", "avg"]] = summary_df[["min", "max", "avg"]].round(3)

		self._metric_summary_cache[cache_key] = summary_df
		return summary_df.copy()

	def _get_metric_columns_with_valid_data(self, fileids: list[int] | None = None) -> list[str]:
		summary_df = self._get_metric_summary_for_selection(fileids)
		if summary_df.empty:
			return []
		return [str(name) for name in summary_df["name"].tolist()]

	def refresh_plot(self):
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			logger.debug(f"refresh_plot triggered with {len(rows)} selected row(s): {rows[:5]}{'...' if len(rows) > 5 else ''} _force_next_plot_async: {self._force_next_plot_async}")
			if self._force_next_plot_async or len(rows) > 1:
				self._force_next_plot_async = False
				self._start_async_plot_for_rows(rows)
				return
			self._plot_for_rows(rows)

	def _start_async_plot_for_rows(self, rows: list[int]):
		fileids = self._get_selected_fileids(rows)
		self._start_async_plot_for_fileids(fileids)

	def _start_async_plot_for_fileids(self, fileids: list[int]):
		if not fileids:
			self.stats_label.setText("No trip selected")
			self._set_all_metrics_table_model(pd.DataFrame(columns=["metric", "min", "avg", "max"]))
			return
		self._force_next_plot_async = False

		self._invalidate_and_cancel_active_plot_load(None)

		selected_metrics = self._get_selected_metrics()
		if not selected_metrics:
			fallback_metric = self._get_selected_metric()
			selected_metrics = [fallback_metric] if fallback_metric else []
		if not selected_metrics:
			self.stats_label.setText("No valid metrics available for plotting")
			return

		selected_metric = selected_metrics[0]
		lat_col = self._resolved_torqlogs_columns.get('latitude')
		lon_col = self._resolved_torqlogs_columns.get('longitude')
		time_col = (self._resolve_actual_torqlogs_column('gpstime')
					or self._resolve_actual_torqlogs_column('devicetime'))
		metric_col = self._resolve_actual_torqlogs_column(selected_metric)
		if not (lat_col and lon_col and metric_col):
			self.stats_label.setText("Missing required torqlogs columns for plotting")
			return

		worker_metric_names = [self._resolve_actual_torqlogs_column(name) or name for name in selected_metrics]
		speed_metric_col = self._preferred_speed_metric_column()
		if speed_metric_col and speed_metric_col not in worker_metric_names:
			worker_metric_names.append(speed_metric_col)

		self._plot_data_request_id += 1
		request_id = self._plot_data_request_id
		self._plot_request_context[request_id] = {
			"fileids": list(fileids),
			"selected_metrics": list(selected_metrics),
			"selected_metric": selected_metric,
			"colormap_name": self._current_colormap,
			"preview_trips": {},
			"preview_bounds": None,
			"last_preview_render_ts": 0.0,
			"last_preview_render_done": 0,
			"preview_render_mode": "first_and_final" if len(fileids) >= 10 else "throttled",
			"preview_render_min_interval_s": float(self._preview_render_min_interval_s),
		}

		sample_step = self._sample_step()
		self.stats_label.setText(
			f"Loading {len(fileids)} trips in background (paths first, sample 1/{sample_step})..."
		)
		self.cancel_plot_load_btn.setEnabled(True)

		thread = QThread()
		worker = TripPlotWorker(
			self.engine.url.render_as_string(hide_password=False),
			request_id,
			fileids,
			metric_col,
			lat_col,
			lon_col,
			time_col,
			sample_step,
			metric_names=worker_metric_names,
		)
		worker.moveToThread(thread)

		thread.started.connect(worker.run)
		worker.finished.connect(self._on_async_plot_data_loaded)
		worker.error.connect(self._on_async_plot_data_error)
		worker.progress.connect(self._on_async_plot_data_progress)
		worker.preview.connect(self._on_async_plot_preview)
		worker.cancelled.connect(self._on_async_plot_data_cancelled)
		worker.finished.connect(thread.quit)
		worker.error.connect(thread.quit)
		worker.cancelled.connect(thread.quit)
		thread.finished.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)
		thread.finished.connect(lambda t=thread: self._active_threads.discard(t))
		thread.finished.connect(lambda t=thread: self._thread_registry.pop(id(t), None))
		thread.finished.connect(lambda t=thread: self._on_plot_data_thread_finished(t))

		self._plot_data_worker = worker
		self._plot_data_thread = thread
		self._active_threads.add(thread)
		self._register_thread(
			thread,
			owner_name="MainWindow",
			task_name="Trip plot background load",
			launched_by="_load_plot_data_async",
			worker=worker,
		)
		thread.start()

	def _on_async_plot_preview(self, request_id: int, payload: object) -> None:
		if request_id != self._plot_data_request_id:
			return
		ctx = self._plot_request_context.get(request_id)
		if not ctx:
			return

		from typing import cast as _cast
		preview_payload = _cast(dict, payload)
		trip_payload = _cast(dict | None, preview_payload.get("trip"))
		done = int(preview_payload.get("done", 0) or 0)
		total = int(preview_payload.get("total", 0) or 0)

		preview_trips = _cast(dict, ctx.get("preview_trips", {}))
		if trip_payload:
			fid = int(trip_payload.get("fileid", -1))
			if fid >= 0:
				preview_trips[fid] = {
					"fileid": fid,
					"lat": list(trip_payload.get("lat", [])),
					"lon": list(trip_payload.get("lon", [])),
					"speed": [],
				}

		if not preview_trips:
			self.stats_label.setText(f"Loading trip paths... ({done}/{max(1,total)})")
			return

		# Avoid flickering: for large selections use first_and_final mode — render once when the
		# first trip arrives (so something appears immediately) then skip every intermediate
		# preview and only render again on completion.  For small selections use a throttled
		# interval so progress is visible without excessive reloads.
		is_final_preview = done >= max(1, total)
		preview_render_mode = str(ctx.get("preview_render_mode", "throttled") or "throttled")
		last_preview_render_ts = float(ctx.get("last_preview_render_ts", 0.0) or 0.0)
		last_preview_render_done = int(ctx.get("last_preview_render_done", 0) or 0)
		preview_render_min_interval_s = float(ctx.get("preview_render_min_interval_s", 0.25) or 0.25)
		now = time.monotonic()
		if preview_render_mode == "first_and_final":
			# Only two renders: first arrival + final result
			should_render_preview = is_final_preview or last_preview_render_ts <= 0.0
		else:
			should_render_preview = is_final_preview
			if not should_render_preview:
				if last_preview_render_ts <= 0.0:
					should_render_preview = True
				elif done > last_preview_render_done and (now - last_preview_render_ts) >= preview_render_min_interval_s:
					should_render_preview = True
		if not should_render_preview:
			self.stats_label.setText(f"Loading trip paths... ({done}/{max(1,total)})")
			return

		fileids = _cast(list, ctx.get("fileids", []))
		colormap_name = _cast(str, ctx.get("colormap_name", self._current_colormap))
		preview_bounds = _cast(tuple[float, float, float, float] | None, ctx.get("preview_bounds"))
		trip_list = list(preview_trips.values())
		m, bounds = self._build_trip_folium_map(
			trip_list,
			fileids,
			colormap_name,
			render_phase="preview",
			bounds_override=preview_bounds,
		)
		if m is not None:
			if bounds:
				if preview_bounds is None:
					ctx["preview_bounds"] = bounds
				self._mw_full_bounds_latlon = bounds
				self._mw_current_fileids = fileids
			self._overlay_start_end_points(m, fileids, bounds)
			self.map_canvas.display_map(m)
		ctx["last_preview_render_ts"] = now
		ctx["last_preview_render_done"] = done
		self.stats_label.setText(f"Loading trip paths... ({done}/{max(1,total)})")

	def _cancel_async_plot_load(self):
		thread = self._plot_data_thread
		if thread is None:
			self.cancel_plot_load_btn.setEnabled(False)
			return
		if thread.isRunning():
			self._request_thread_stop(thread, "plot_data")
			self.stats_label.setText("Cancelling background load...")
		self.cancel_plot_load_btn.setEnabled(False)

	def _invalidate_and_cancel_active_plot_load(self, reason: str | None):
		thread = self._plot_data_thread
		if thread is None or not thread.isRunning():
			return
		self._plot_data_request_id += 1
		self._plot_request_context.clear()
		self._request_thread_stop(thread, "plot_data")
		self.cancel_plot_load_btn.setEnabled(False)
		if reason:
			self.stats_label.setText(f"{reason}; cancelling previous background load...")

	def _on_initial_trips_thread_finished(self, finished_thread: QThread) -> None:
		if self._initial_trips_thread is finished_thread:
			self._initial_trips_thread = None
			self._initial_trips_worker = None

	def _on_plot_data_thread_finished(self, finished_thread: QThread) -> None:
		if self._plot_data_thread is finished_thread:
			self._plot_data_thread = None
			self._plot_data_worker = None

	def _request_thread_stop(self, thread: QThread | None, name: str) -> None:
		if thread is None:
			return
		try:
			if not thread.isRunning():
				return
		except RuntimeError:
			return
		thread.requestInterruption()
		thread.quit()
		QTimer.singleShot(2000, lambda t=thread, n=name: self._terminate_thread_if_running(t, n))

	def _terminate_thread_if_running(self, thread: QThread | None, name: str) -> None:
		if thread is None:
			return
		try:
			if not thread.isRunning():
				return
		except RuntimeError:
			return
		if self.args.debug:
			logger.warning(f"Force-terminating still-running thread '{name}'")
		thread.terminate()
		thread.wait(1000)

	def _on_async_plot_data_loaded(self, request_id: int, payload: object) -> None:
		if request_id != self._plot_data_request_id:
			return
		self.cancel_plot_load_btn.setEnabled(False)
		ctx = self._plot_request_context.pop(request_id, None)
		if not ctx:
			return

		from typing import cast as _cast
		data = _cast(dict, payload)
		trips = _cast(list, data.get("trips", []))
		all_lat = _cast(list, data.get("all_lat", []))
		all_lon = _cast(list, data.get("all_lon", []))
		all_metric_values = _cast(list, data.get("all_metric_values", []))

		fileids = _cast(list, ctx["fileids"])
		selected_metrics = _cast(list, ctx["selected_metrics"])
		selected_metric = _cast(str, ctx["selected_metric"])
		colormap_name = _cast(str, ctx["colormap_name"])

		for item in trips:
			fid = int(item.get("fileid", -1))
			if fid >= 0:
				lat_vals = list(item.get("lat", []))
				lon_vals = list(item.get("lon", []))
				time_vals = list(item.get("time", []))
				metrics_payload = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
				if metrics_payload:
					for metric_name, metric_values in metrics_payload.items():
						metric_list = list(metric_values) if isinstance(metric_values, list) else []
						point_count = min(len(lat_vals), len(lon_vals), len(metric_list))
						if time_vals:
							point_count = min(point_count, len(time_vals))
						self._trip_plot_cache[(fid, str(metric_name))] = {
							"lat": lat_vals[:point_count],
							"lon": lon_vals[:point_count],
							"speed": metric_list[:point_count],
							"time": time_vals[:point_count] if time_vals else [],
						}
				else:
					self._trip_plot_cache[(fid, selected_metric)] = {
						"lat": lat_vals,
						"lon": lon_vals,
						"speed": list(item.get("speed", [])),
						"time": time_vals,
					}

		fileid_color_map = self._build_fileid_color_map(fileids, colormap_name)
		m, bounds = self._build_trip_folium_map(trips, fileids, colormap_name, render_phase="final")

		if m is not None:
			self._overlay_start_end_points(m, fileids, bounds)
			if bounds:
				self._mw_full_bounds_latlon = bounds
				self._mw_current_fileids = list(fileids)
				self._mw_last_metric = selected_metric
			self.map_canvas.display_map(m)
		else:
			self.map_canvas.show_empty("No GPS data")

		self._update_timeseries_plot(fileids, selected_metrics, colormap_name, fileid_color_map)
		self._update_stats_panel(fileids, all_metric_values, all_lat, all_lon, selected_metric)

	def _on_async_plot_data_progress(self, request_id: int, done: int, total: int):
		if request_id != self._plot_data_request_id:
			return
		done_safe = max(0, int(done))
		total_safe = max(1, int(total))
		self.stats_label.setText(f"Processing metrics in background... ({done_safe}/{total_safe})")

	def _on_async_plot_data_error(self, request_id: int, error_message: str):
		if request_id != self._plot_data_request_id:
			return
		self.cancel_plot_load_btn.setEnabled(False)
		self._plot_request_context.pop(request_id, None)
		logger.error(error_message)

	def _on_async_plot_data_cancelled(self, request_id: int):
		if request_id != self._plot_data_request_id:
			return
		self.cancel_plot_load_btn.setEnabled(False)
		self._plot_request_context.pop(request_id, None)
		self.stats_label.setText("Background load cancelled")

	def _get_selected_fileids(self, rows):
		if not rows:
			return []
		if 'fileid' in self.df_trips.columns:
			return [int(k) for k in self.df_trips.iloc[rows]['fileid'].tolist() if pd.notna(k)]
		return []

	def _load_trip_geo_data(self, fileid: int) -> dict[str, list] | None:
		if fileid in self._trip_geo_cache:
			return self._trip_geo_cache[fileid]

		lat_col = self._resolved_torqlogs_columns.get('latitude')
		lon_col = self._resolved_torqlogs_columns.get('longitude')
		time_col = (self._resolve_actual_torqlogs_column('gpstime')
					or self._resolve_actual_torqlogs_column('devicetime'))
		if not (lat_col and lon_col):
			return None

		time_select = f', "{time_col}" AS metric_time' if time_col else ''
		order_expr = f'"{time_col}", id' if time_col else 'id'
		sample_step = self._sample_step_for_fileid(fileid)
		q = text(
			f'''
			WITH ordered AS (
				SELECT "{lon_col}" AS longitude, "{lat_col}" AS latitude{time_select},
					ROW_NUMBER() OVER (ORDER BY {order_expr}) AS rn
				FROM torqlogs
				WHERE fileid = :fileid
					AND "{lon_col}" IS NOT NULL
					AND "{lat_col}" IS NOT NULL
			)
			SELECT longitude, latitude{', metric_time' if time_col else ''}
			FROM ordered
			WHERE (:sample_step <= 1) OR ((rn - 1) % :sample_step = 0)
			ORDER BY rn
			'''
		)
		params: dict[str, Any] = {"fileid": int(fileid), "sample_step": int(sample_step)}
		try:
			df_geo = pd.read_sql(q, self.engine, params=params)
		except Exception as e:
			logger.error(f"Failed to load geo data for trip fileid={fileid}: {e} ({type(e)})")
			df_geo = pd.DataFrame()
		if df_geo.empty:
			payload = {"lat": [], "lon": [], "time": []}
			self._trip_geo_cache[fileid] = payload
			return payload

		lat_vals = pd.to_numeric(df_geo["latitude"], errors="coerce").tolist()
		lon_vals = pd.to_numeric(df_geo["longitude"], errors="coerce").tolist()
		time_values: list = []
		if 'metric_time' in df_geo.columns:
			time_values = pd.to_datetime(df_geo['metric_time'], errors='coerce').tolist()

		payload = {"lat": lat_vals, "lon": lon_vals, "time": time_values}
		self._trip_geo_cache[fileid] = payload
		return payload

	def _load_trip_plot_data(self, fileid: int, metric_name: str) -> dict[str, list] | None:
		cache_key = (fileid, metric_name)
		if cache_key in self._trip_plot_cache:
			return self._trip_plot_cache[cache_key]

		geo_payload = self._load_trip_geo_data(fileid)
		if geo_payload is None:
			if self.args.debug:
				logger.warning(f"No geo data available for trip fileid={fileid}, cannot load plot data for metric '{metric_name}'")
			return None

		lat_col = self._resolved_torqlogs_columns.get('latitude')
		lon_col = self._resolved_torqlogs_columns.get('longitude')
		if not (lat_col and lon_col):
			return None

		speed_col_name = self._resolve_actual_torqlogs_column(metric_name)
		time_col = (self._resolve_actual_torqlogs_column('gpstime')
					or self._resolve_actual_torqlogs_column('devicetime'))
		if not speed_col_name:
			return None

		order_expr = f'"{time_col}", id' if time_col else 'id'
		sample_step = self._sample_step_for_fileid(fileid)
		q = text(
			f'''
			WITH ordered AS (
				SELECT "{speed_col_name}" AS selectedmetric,
					ROW_NUMBER() OVER (ORDER BY {order_expr}) AS rn
				FROM torqlogs
				WHERE fileid = :fileid
					AND "{lon_col}" IS NOT NULL
					AND "{lat_col}" IS NOT NULL
			)
			SELECT selectedmetric
			FROM ordered
			WHERE (:sample_step <= 1) OR ((rn - 1) % :sample_step = 0)
			ORDER BY rn
			'''
		)
		params: dict[str, Any] = {"fileid": int(fileid), "sample_step": int(sample_step)}
		df_part = pd.read_sql(q, self.engine, params=params)
		if df_part.empty:
			self._trip_plot_cache[cache_key] = {"lat": [], "lon": [], "speed": [], "time": []}
			return self._trip_plot_cache[cache_key]
		speed_series = pd.to_numeric(df_part['selectedmetric'], errors='coerce').fillna(0)

		lat_vals = geo_payload["lat"]
		lon_vals = geo_payload["lon"]
		time_values = geo_payload["time"]
		points = min(len(lat_vals), len(lon_vals), len(speed_series))
		if time_values:
			points = min(points, len(time_values))

		payload: dict[str, list] = {
			"lat": lat_vals[:points],
			"lon": lon_vals[:points],
			"speed": speed_series.tolist()[:points],
			"time": time_values[:points] if time_values else [],
		}
		self._trip_plot_cache[cache_key] = payload
		logger.debug(f"Loaded trip plot data for fileid={fileid}, metric_name={metric_name}, points={len(payload['lat'])}")
		return payload

	def _selection_key(self, fileids: list[int], metric_name: str) -> str:
		return (
			f"{self._map_cache_version}|metric={metric_name}|sample={self._sampling_cache_token(fileids)}|"
			+ ",".join(str(fid) for fid in sorted(fileids))
		)

	def _timeseries_selection_key(self, fileids: list[int], metric_names: list[str]) -> str:
		metrics_part = ",".join(metric_names)
		files_part = ",".join(str(fid) for fid in sorted(fileids))
		return f"{self._map_cache_version}|timeseries|sample={self._sampling_cache_token(fileids)}|metrics={metrics_part}|{files_part}"

	def _cache_fileid(self, fileids: list[int]) -> int | None:
		return int(fileids[0]) if len(fileids) == 1 else None

	def _load_cached_timeseries_image(self, fileids: list[int], metric_names: list[str], colormap: str) -> bytes | None:
		selection_key = self._timeseries_selection_key(fileids, metric_names)
		q = text(
			"""
			SELECT image_png
			FROM mapimagecache
			WHERE selection_key = :selection_key AND zoom = :zoom AND colormap = :colormap
			LIMIT 1
			"""
		)
		with self.engine.connect() as conn:
			row = conn.execute(q, {
				"selection_key": selection_key,
				"zoom": -1,
				"colormap": colormap,
			}).first()
		return bytes(row[0]) if row and row[0] is not None else None

	def _save_cached_timeseries_image(self, fileids: list[int], metric_names: list[str], colormap: str):
		selection_key = self._timeseries_selection_key(fileids, metric_names)
		fileid = self._cache_fileid(fileids)
		buf = io.BytesIO()
		self.timeseries_canvas.figure.savefig(buf, format='png', dpi=100)
		image_bytes = buf.getvalue()
		upsert_sql = text(
			"""
			INSERT INTO mapimagecache (fileid, selection_key, zoom, colormap, image_png, ext_west, ext_east, ext_south, ext_north, created_at, updated_at)
			VALUES (:fileid, :selection_key, :zoom, :colormap, :image_png, :ext_west, :ext_east, :ext_south, :ext_north, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(selection_key, zoom, colormap)
			DO UPDATE SET
				fileid = excluded.fileid,
				image_png = excluded.image_png,
				updated_at = CURRENT_TIMESTAMP
			"""
		)
		with self.engine.begin() as conn:
			conn.execute(upsert_sql, {
				"fileid": fileid,
				"selection_key": selection_key,
				"zoom": -1,
				"colormap": colormap,
				"image_png": image_bytes,
				"ext_west": None,
				"ext_east": None,
				"ext_south": None,
				"ext_north": None,
			})

	def _compute_plot_bounds_latlon(
		self, all_lat: list[float], all_lon: list[float]
	) -> tuple[float, float, float, float] | None:
		if not all_lat or not all_lon:
			return None
		lat_min, lat_max = min(all_lat), max(all_lat)
		lon_min, lon_max = min(all_lon), max(all_lon)
		dlat = max(0.001, lat_max - lat_min)
		dlon = max(0.001, lon_max - lon_min)
		pad_lat = dlat * self._bounds_padding_ratio
		pad_lon = dlon * self._bounds_padding_ratio
		return (lat_min - pad_lat, lon_min - pad_lon, lat_max + pad_lat, lon_max + pad_lon)

	@staticmethod
	def _colormap_cycle_length(colormap_name: str) -> int:
		if colormap_name in ['tab10']:
			return 10
		if colormap_name in ['tab20', 'tab20b', 'tab20c']:
			return 20
		if colormap_name in ['Set1']:
			return 9
		if colormap_name in ['Set2', 'Dark2', 'Pastel2']:
			return 8
		if colormap_name in ['Set3', 'Pastel1']:
			return 12
		return 10

	def _build_fileid_color_map(self, fileids: list[int], colormap_name: str) -> dict[int, tuple[float, float, float, float]]:
		cmap = plt.colormaps[colormap_name]
		cycle_length = self._colormap_cycle_length(colormap_name)
		return {int(fileid): cmap(idx % cycle_length) for idx, fileid in enumerate(fileids)}

	def _preferred_speed_metric_column(self) -> str | None:
		for req_name in ("speedgpskmh", "gpsspeedkmh", "speedobdkmh"):
			actual_col = self._resolve_actual_torqlogs_column(req_name)
			if actual_col:
				return actual_col
		if self.args.debug:
			logger.warning("No preferred speed metric column found in torqlogs, speed-based coloring will be unavailable")
		return None

	def _speed_values_for_map_item(self, item: dict[str, Any]) -> list[float]:
		metrics_payload = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
		if metrics_payload:
			for speed_col in (
				self._resolve_actual_torqlogs_column("speedgpskmh"),
				self._resolve_actual_torqlogs_column("gpsspeedkmh"),
				self._resolve_actual_torqlogs_column("speedobdkmh"),
			):
				if not speed_col:
					if self.args.debug:
						logger.warning(f"No valid speed metric column found for map item with fileid={item.get('fileid', 'unknown')}, cannot extract speed values for coloring")
					continue
				raw_vals = metrics_payload.get(speed_col)
				if isinstance(raw_vals, list) and raw_vals:
					return pd.to_numeric(pd.Series(raw_vals), errors="coerce").fillna(0.0).tolist()

		raw_speed = item.get("speed")
		if isinstance(raw_speed, list) and raw_speed:
			return pd.to_numeric(pd.Series(raw_speed), errors="coerce").fillna(0.0).tolist()
		return []

	def _build_trip_folium_map(
		self,
		trip_data_list: list[dict],
		fileids: list[int],
		colormap_name: str,
		render_phase: str = "final",
		bounds_override: tuple[float, float, float, float] | None = None,
	) -> tuple[folium.Map | None, tuple[float, float, float, float] | None]:
		all_lat: list[float] = []
		all_lon: list[float] = []
		has_speed_data = False
		for item in trip_data_list:
			all_lat.extend(item.get("lat", []))
			all_lon.extend(item.get("lon", []))
			has_speed_data = has_speed_data or bool(self._speed_values_for_map_item(item))
		if not all_lat:
			if self.args.debug:
				logger.warning(f"No latitude data available in trip data list, cannot build folium map. trip_data_list: {len(trip_data_list)} fileids: {len(fileids)} {fileids[0:3]}")
			return None, None

		bounds = bounds_override or self._compute_plot_bounds_latlon(all_lat, all_lon)
		if bounds is None:
			if self.args.debug:
				logger.warning(f"Failed to compute plot bounds, cannot build folium map. trip_data_list: {len(trip_data_list)} fileids: {len(fileids)} {fileids[0:3]}")
			return None, None
		lat_min, lon_min, lat_max, lon_max = bounds
		clat = (lat_min + lat_max) / 2
		clon = (lon_min + lon_max) / 2

		m = folium.Map(location=[clat, clon], zoom_start=int(self.zoom_combo.currentText()))
		m.fit_bounds([[lat_min, lon_min], [lat_max, lon_max]])

		cmap = plt.colormaps[colormap_name]
		fileid_color_map = self._build_fileid_color_map(fileids, colormap_name)

		for idx, item in enumerate(trip_data_list):
			lat_vals = item.get("lat", [])
			lon_vals = item.get("lon", [])
			speed_vals = self._speed_values_for_map_item(item)
			fileid = int(item.get("fileid", -1))
			if not lat_vals:
				continue
			base_rgba = fileid_color_map.get(fileid, cmap(idx % self._colormap_cycle_length(colormap_name)))
			base_hex = mcolors.to_hex(base_rgba)
			features = []
			for i in range(len(lat_vals)):
				spd = float(speed_vals[i]) if i < len(speed_vals) else 0.0
				radius = max(2.0, min(8.0, spd / 10.0 * self._dot_size_scale + 1.0))
				features.append({
					"type": "Feature",
					"geometry": {"type": "Point", "coordinates": [lon_vals[i], lat_vals[i]]},
					"properties": {"color": base_hex, "radius": radius},
				})
			layer = folium.GeoJson(
				{"type": "FeatureCollection", "features": features},
				marker=folium.CircleMarker(radius=4, fill=True),
				style_function=lambda f: {
					"fillColor": f["properties"]["color"],
					"color": f["properties"]["color"],
					"radius": f["properties"]["radius"],
					"weight": 0,
					"fill": True,
					"fillOpacity": 0.65,
				},
				name=f"Trip {fileid}",
			)
			layer.add_to(m)
		if self.args.debug:
			if render_phase == "preview":
				logger.debug(f"Preview map update: loaded={len(trip_data_list)}/{len(fileids)} trips,  bounds={bounds}, speed_data_ready={has_speed_data}")
			else:
				logger.debug(f"Final map render: trips={len(trip_data_list)}, bounds={bounds}, speed_data_ready={has_speed_data}")
		return m, bounds

	def _plot_for_rows(self, rows):
		fileids = self._get_selected_fileids(rows)
		self._plot_for_fileids(fileids)

	def _plot_for_fileids(self, fileids: list[int]):
		if not fileids:
			self.stats_label.setText("No trip selected")
			self._set_all_metrics_table_model(pd.DataFrame(columns=["metric", "min", "avg", "max"]))
			return

		colormap_name = self._current_colormap
		selected_metrics = self._get_selected_metrics()
		if not selected_metrics:
			fallback_metric = self._get_selected_metric()
			selected_metrics = [fallback_metric] if fallback_metric else []
		if not selected_metrics:
			self.stats_label.setText("No valid metrics available for plotting")
			return
		selected_metric = selected_metrics[0]

		fileid_color_map = self._build_fileid_color_map(fileids, colormap_name)
		trip_data_list: list[dict] = []
		all_metric_values: list[float] = []

		for fileid in fileids:
			if self.args.debug:
				logger.debug(f"Processing fileid={fileid} for metric='{selected_metric}'")
			plot_data = self._load_trip_plot_data(fileid, selected_metric)
			if not plot_data:
				if self.args.debug:
					logger.warning(f"No plot data for fileid={fileid}, metric={selected_metric}")
				continue
			trip_data_list.append({
				"fileid": fileid,
				"lat": plot_data["lat"],
				"lon": plot_data["lon"],
				"speed": plot_data["speed"],
				"time": plot_data.get("time", []),
			})
			all_metric_values.extend(plot_data["speed"])

		if not trip_data_list:
			self.map_canvas.show_empty("No GPS data for selected trips")
			return

		m, bounds = self._build_trip_folium_map(trip_data_list, fileids, colormap_name)
		if m is None:
			self.map_canvas.show_empty("No GPS data")
			return

		self._overlay_start_end_points(m, fileids, bounds)
		self._mw_full_bounds_latlon = bounds
		self._mw_current_fileids = list(fileids)
		self._mw_last_metric = selected_metric
		self.map_canvas.display_map(m)

		all_lat = [v for item in trip_data_list for v in item["lat"]]
		all_lon = [v for item in trip_data_list for v in item["lon"]]
		self._update_timeseries_plot(fileids, selected_metrics, colormap_name, fileid_color_map)
		self._update_stats_panel(fileids, all_metric_values, all_lat, all_lon, selected_metric)

	def _plot_for_start_end_fileids(self, fileids: list[int]) -> None:
		if not fileids:
			return
		self._populate_metric_columns(fileids)
		if len(fileids) >= self._plot_async_threshold:
			self._start_async_plot_for_fileids(fileids)
		else:
			self._plot_for_fileids(fileids)

	def _load_trip_metadata(self, fileids: list[int]) -> dict:
		if not fileids:
			return {}

		try:
			fileids_str = ",".join(str(fid) for fid in fileids[:10])
			query = f"SELECT fileid, trip_start, trip_end, trip_duration, trip_distance FROM torqfiles WHERE fileid IN ({fileids_str})"
			df = pd.read_sql(query, self.engine)
			if df.empty:
				return {}
			return df.set_index('fileid').to_dict('index')
		except Exception as e:
			logger.warning(f"Could not load trip metadata: {e}")
			return {}

	def _format_trip_stats(self, fileids: list[int], trip_count: int, point_count: int,
		metric_name: str, metric_min: float, metric_avg: float, metric_max: float,
		all_lat: list[float], all_lon: list[float], trip_info: dict,
		all_metric_stats: dict | None = None) -> str:
		W = 52
		lines: list[str] = []

		lines.append("═" * W)
		lines.append(f"TRIP SUMMARY  — {trip_count} trip(s), {point_count} pts")
		lines.append("═" * W)

		if trip_info:
			total_distance = sum(row.get('trip_distance', 0) or 0 for row in trip_info.values())
			total_duration = sum(row.get('trip_duration', 0) or 0 for row in trip_info.values())
			start_dates = [str(row.get('trip_start', '')) for row in trip_info.values() if row.get('trip_start')]
			end_dates   = [str(row.get('trip_end',   '')) for row in trip_info.values() if row.get('trip_end')]
			if start_dates:
				lines.append(f"Date:     {start_dates[0]}")
			if end_dates and end_dates != start_dates:
				lines.append(f"End:      {end_dates[0]}")
			if total_distance:
				lines.append(f"Distance: {total_distance/1000:.2f} km")
			if total_duration:
				lines.append(f"Duration: {format_duration(total_duration)}")
			if total_distance and total_duration:
				avg_spd = (total_distance / 1000) / (total_duration / 3600)
				lines.append(f"Avg spd:  {avg_spd:.1f} km/h")

		category, display_name, unit = categorize_metric(metric_name)
		unit_str = f" {unit}" if unit else ""
		lines.append(f"\n{'─' * W}")
		lines.append(f"Selected: {display_name}{unit_str}")
		lines.append(f"  min {metric_min:.2f}  avg {metric_avg:.2f}  max {metric_max:.2f}")
		suggestion = get_analysis_suggestion(category)
		lines.append(f"  [{suggestion['analysis_type']}]  {suggestion['visualization']}")

		lines.append("\n" + "═" * W)
		return "\n".join(lines)

	def _build_all_metrics_table_df(self, all_metric_stats: dict | None) -> pd.DataFrame:
		if not all_metric_stats:
			return pd.DataFrame(columns=["metric", "min", "avg", "max"])

		rows: list[dict[str, Any]] = []
		grouped = group_metrics_by_category(list(all_metric_stats.keys()))
		for _, cat_metrics in grouped.items():
			for display_name, unit, original_metric in cat_metrics:
				stats = all_metric_stats.get(original_metric)
				if not stats:
					continue
				metric_label = f"{display_name} ({unit})" if unit else display_name
				rows.append(
					{
						"metric": metric_label,
						"min": round(float(stats.get("min", 0.0)), 2),
						"avg": round(float(stats.get("avg", 0.0)), 2),
						"max": round(float(stats.get("max", 0.0)), 2),
					}
				)

		table_df = pd.DataFrame(rows, columns=["metric", "min", "avg", "max"])
		if not table_df.empty:
			table_df.sort_values(by="metric", inplace=True)
			table_df.reset_index(drop=True, inplace=True)
		return table_df

	def _update_stats_panel(self, fileids: list[int], metric_values: list[float], all_lat: list[float], all_lon: list[float], metric_name: str):
		trip_count = len(fileids)
		point_count = len(all_lat)
		metric_series = pd.Series(metric_values, dtype="float64") if metric_values else pd.Series(dtype="float64")
		metric_min = float(metric_series.min()) if not metric_series.empty else 0.0
		metric_avg = float(metric_series.mean()) if not metric_series.empty else 0.0
		metric_max = float(metric_series.max()) if not metric_series.empty else 0.0

		selection_key = tuple(sorted(fileids))
		if selection_key in self._selection_stats_cache:
			trip_info, all_metric_stats = self._selection_stats_cache[selection_key]
		else:
			trip_info = self._load_trip_metadata(fileids)
			all_metric_stats = self._load_all_metric_stats(fileids)
			self._selection_stats_cache[selection_key] = (trip_info, all_metric_stats)

		stats_text = self._format_trip_stats(
			fileids, trip_count, point_count, metric_name,
			metric_min, metric_avg, metric_max, all_lat, all_lon, trip_info, all_metric_stats
		)

		self.stats_label.setText(stats_text)
		self._set_all_metrics_table_model(self._build_all_metrics_table_df(all_metric_stats))

	def _update_timeseries_plot(
		self,
		fileids: list[int],
		metric_names: list[str],
		colormap_name: str,
		fileid_color_map: dict[int, tuple[float, float, float, float]] | None = None,
	):
		ax = self.timeseries_canvas.ax
		ax.clear()
		cached_img = self._load_cached_timeseries_image(fileids, metric_names, colormap_name)
		if cached_img is not None:
			img = mpimg.imread(io.BytesIO(cached_img), format='png')
			ax.imshow(img, extent=(0, 1, 0, 1), transform=ax.transAxes, aspect='auto', zorder=0)
			ax.set_axis_off()
			self.timeseries_canvas.draw_idle()
			logger.debug(f"Loaded cached timeseries image for metrics={metric_names}, trips={fileids}")
			return
		ax.set_axis_on()
		cmap = plt.colormaps[colormap_name]
		cycle_length = self._colormap_cycle_length(colormap_name)
		linestyles = ['-', '--', ':', '-.']
		multi_metric = len(metric_names) > 1
		multi_trip = len(fileids) > 1
		if fileid_color_map is None:
			fileid_color_map = self._build_fileid_color_map(fileids, colormap_name)
		use_progress_axis = multi_trip
		has_datetime_x = False

		has_data = False
		for m_idx, metric_name in enumerate(metric_names):
			for t_idx, fileid in enumerate(fileids):
				if self.args.debug:
					logger.debug(f"Processing fileid={fileid} ({t_idx + 1}/{len(fileids)}) for metric='{metric_name}'")
				plot_data = self._load_trip_plot_data(fileid, metric_name)
				if not plot_data or not plot_data.get('speed'):
					if self.args.debug:
						logger.warning(f"No data for timeseries plot: fileid={fileid}, metric={metric_name}")
					continue
				time_vals = plot_data.get('time') or []
				metric_vals = list(plot_data['speed'])
				use_time = bool(time_vals) and any(t is not None and not pd.isna(t) for t in time_vals[:10])
				if use_progress_axis:
					if use_time:
						pairs = [(t, v) for t, v in zip(time_vals, metric_vals)
								 if t is not None and not pd.isna(t)]
						metric_clean = [v for _, v in pairs]
					else:
						metric_clean = metric_vals

					if len(metric_clean) <= 1:
						x_vals = [0.0] * len(metric_clean)
					else:
						x_vals = np.linspace(0.0, 100.0, num=len(metric_clean)).tolist()
					metric_vals = metric_clean
				else:
					if use_time:
						pairs = [(t, v) for t, v in zip(time_vals, metric_vals)
								 if t is not None and not pd.isna(t)]
						if pairs:
							x_vals, metric_vals = zip(*pairs)
							has_datetime_x = True
						else:
							x_vals, metric_vals = [], []
					else:
						x_vals = list(range(len(metric_vals)))
				if not x_vals:
					continue
				if multi_trip:
					color = fileid_color_map.get(int(fileid), cmap(t_idx % cycle_length))
					lstyle = linestyles[m_idx % len(linestyles)] if multi_metric else '-'
				else:
					color = cmap(m_idx % cycle_length)
					lstyle = '-'
				_, display_name, unit = categorize_metric(metric_name)
				if multi_metric and multi_trip:
					label = f"{display_name} / trip {fileid}"
				elif multi_metric:
					label = display_name
				elif multi_trip:
					label = f"trip {fileid}"
				else:
					label = None
				ax.plot(x_vals, metric_vals, color=color, linestyle=lstyle,
						linewidth=0.8, alpha=0.85, label=label)
				has_data = True

		if len(metric_names) == 1:
			_, display_name, unit = categorize_metric(metric_names[0])
			unit_str = f" ({unit})" if unit else ""
			ax.set_title(f"{display_name}{unit_str} over time", fontsize=9, pad=3)
			ax.set_ylabel(display_name, fontsize=8)
		else:
			ax.set_title("Metrics over time", fontsize=9, pad=3)
			ax.set_ylabel("Value", fontsize=8)
		ax.set_xlabel("Trip progress (%)" if use_progress_axis else "Time", fontsize=8)
		ax.tick_params(labelsize=7)
		if (multi_metric or multi_trip) and has_data:
			ax.legend(fontsize=7)
		if has_data:
			if has_datetime_x:
				try:
					ax.figure.autofmt_xdate(rotation=30)
				except Exception as e:
					logger.warning(f"Could not format x-axis dates: {e} ({type(e)})")
			try:
				self._save_cached_timeseries_image(fileids, metric_names, colormap_name)
			except Exception as e:
				logger.warning(f"Could not save timeseries cache: {e} ({type(e)})")
		self.timeseries_canvas.draw_idle()

	def _get_torqlogs_numeric_columns(self) -> set[str]:
		inspector = inspect(self.engine)
		numeric_type_prefixes = (
			'int', 'float', 'real', 'double', 'numeric', 'decimal',
			'smallint', 'bigint', 'money', 'number',
		)
		numeric_cols: set[str] = set()
		for col in inspector.get_columns("torqlogs"):
			type_str = str(col["type"]).lower()
			if any(type_str.startswith(p) for p in numeric_type_prefixes):
				numeric_cols.add(str(col["name"]))
		return numeric_cols

	def _load_all_metric_stats(self, fileids: list[int]) -> dict[str, dict[str, float]]:
		if not fileids:
			return {}
		metric_columns = self._get_metric_columns_with_valid_data()
		if not metric_columns:
			return {}

		numeric_cols = self._get_torqlogs_numeric_columns()

		agg_parts: list[str] = []
		col_map: list[tuple[str, str]] = []
		for metric in metric_columns:
			actual = self._resolve_actual_torqlogs_column(metric)
			if actual and actual in numeric_cols:
				agg_parts.append(
					f'MIN(CAST("{actual}" AS FLOAT)) AS "_s_{metric}_min", '
					f'AVG(CAST("{actual}" AS FLOAT)) AS "_s_{metric}_avg", '
					f'MAX(CAST("{actual}" AS FLOAT)) AS "_s_{metric}_max"'
				)
				col_map.append((metric, actual))

		if not agg_parts:
			return {}

		fileids_str = ",".join(str(fid) for fid in fileids)
		query = f"SELECT {', '.join(agg_parts)} FROM torqlogs WHERE fileid IN ({fileids_str})"
		try:
			df = pd.read_sql(query, self.engine)
			if df.empty:
				return {}
			row = df.iloc[0]
			result: dict[str, dict[str, float]] = {}
			for metric, _ in col_map:
				try:
					min_val = row.get(f"_s_{metric}_min")
					avg_val = row.get(f"_s_{metric}_avg")
					max_val = row.get(f"_s_{metric}_max")
					if min_val is not None and not pd.isna(min_val):
						result[metric] = {
							'min': float(min_val),
							'avg': float(avg_val) if avg_val is not None and not pd.isna(avg_val) else 0.0,
							'max': float(max_val) if max_val is not None and not pd.isna(max_val) else 0.0,
						}
				except Exception as e:
					logger.warning(f"Error processing stats for metric '{metric}': {e} ({type(e)})")
			return result
		except Exception as e:
			logger.warning(f"Could not load all metric stats: {e} ({type(e)})")
			return {}

	def _mw_force_reload_basemap(self):
		self._plot_refresh_timer.start(50)

	def _mw_zoom_in(self) -> None:
		self.map_canvas.zoom_in()

	def _mw_zoom_out(self) -> None:
		self.map_canvas.zoom_out()

	def _mw_zoom_full(self) -> None:
		if self._mw_full_bounds_latlon:
			self.map_canvas.zoom_full(*self._mw_full_bounds_latlon)

	def _on_map_point_clicked(self, data_str: str) -> None:
		try:
			data = json.loads(data_str)
		except Exception as e:
			logger.warning(f"Could not parse map point data: {e} ({type(e)})")
			return
		pos_type = str(data.get("pos_type", ""))
		pos_id = int(data.get("pos_id", 0))
		if not pos_type or pos_id <= 0:
			return
		current_label = str(data.get("label", "") or "")
		prefix = "S" if pos_type == "start" else "E"
		new_label, ok = QInputDialog.getText(
			self,
			f"Edit label — {prefix}{pos_id}",
			f"Label for {pos_type} point #{pos_id}:",
			QLineEdit.EchoMode.Normal,
			current_label,
		)
		if not ok:
			return
		if self._save_start_end_label(pos_type, pos_id, new_label):
			rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
			if rows:
				self._plot_refresh_timer.start(50)

	def _save_start_end_label(self, pos_type: str, pos_id: int, label: str) -> bool:
		table = "startpos" if pos_type == "start" else "endpos"
		id_col = "startid" if pos_type == "start" else "endid"
		label_value: str | None = label.strip() or None
		try:
			with self.engine.begin() as conn:
				conn.execute(
					text(f"UPDATE {table} SET label = :label WHERE {id_col} = :pos_id"),
					{"label": label_value, "pos_id": pos_id},
				)
			logger.debug(f"Saved label for {pos_type} #{pos_id}: '{label_value}'")
			self._populate_label_groups_table()
			return True
		except Exception as e:
			logger.error(f"Failed to save label for {pos_type} #{pos_id}: {e} ({type(e)})")
			return False

	@staticmethod
	def _thread_is_running(thread: QThread | None) -> bool:
		if thread is None:
			return False
		try:
			return bool(thread.isRunning())
		except RuntimeError as e:
			logger.warning(f"RuntimeError checking thread.isRunning(): {e} ({type(e)})")
			return False
		except Exception as e:
			logger.warning(f"Error checking thread.isRunning(): {e} ({type(e)})")
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

	def _collect_running_tasks(self) -> list[dict[str, Any]]:
		tasks = self.get_running_tasks()
		if self._position_manager_window is not None and hasattr(self._position_manager_window, "get_running_tasks"):
			tasks.extend(self._position_manager_window.get_running_tasks())
		if self._start_end_window is not None and hasattr(self._start_end_window, "get_running_tasks"):
			tasks.extend(self._start_end_window.get_running_tasks())
		return sorted(tasks, key=lambda item: float(item.get("started_at", 0.0)))

	def stop_tracked_task(self, thread_id: int) -> bool:
		meta = self._thread_registry.get(int(thread_id))
		if not meta:
			return False
		thread = cast(QThread | None, meta.get("thread"))
		try:
			task_name = str(meta.get("task_name") or f"thread_{thread_id}")
		except Exception as e:
			logger.warning(f"Error getting task name for thread_id={thread_id}: {e} ({type(e)})")
			task_name = f"thread_{thread_id}"
		return self._shutdown_thread(thread, task_name)

	def _stop_task_from_monitor(self, thread_id: int) -> bool:
		if self.stop_tracked_task(thread_id):
			return True
		if self._position_manager_window is not None and hasattr(self._position_manager_window, "stop_tracked_task"):
			if self._position_manager_window.stop_tracked_task(thread_id):
				return True
		if self._start_end_window is not None and hasattr(self._start_end_window, "stop_tracked_task"):
			if self._start_end_window.stop_tracked_task(thread_id):
				return True
		return False

	def _shutdown_thread(self, thread: QThread | None, name: str) -> bool:
		if self.args.debug:
			logger.debug(f"Stopping thread {name} {thread} from {self} active threads: {len(self._active_threads)})")
		if thread is None:
			return True
		try:
			if not thread.isRunning():
				return True
		except RuntimeError as e:
			if self.args.debug:
				logger.error(f"RuntimeError checking thread.isRunning() for '{name}': {e} ({type(e)})")
			return True
		if thread.currentThread() is thread:
			return False
		thread.requestInterruption()
		thread.quit()
		if not thread.wait(3000):
			if self.args.debug:
				logger.warning(f"Thread '{name}' did not stop in time; terminating")
			thread.terminate()
			stopped = thread.wait(1000)
			return bool(stopped)
		return True

	def _detach_running_threads_for_close(self) -> None:
		threads: set[QThread] = set()
		if self._thread_is_running(self._initial_trips_thread):
			threads.add(cast(QThread, self._initial_trips_thread))
		if self._thread_is_running(self._plot_data_thread):
			threads.add(cast(QThread, self._plot_data_thread))
		for t in list(self._active_threads):
			if self._thread_is_running(t):
				threads.add(t)
		for t in threads:
			_ORPHAN_QTHREADS.add(t)
			try:
				t.finished.connect(lambda thr=t: _release_orphan_thread(thr))
			except RuntimeError:
				_release_orphan_thread(t)
		self._active_threads.clear()
		self._initial_trips_thread = None
		self._initial_trips_worker = None
		self._plot_data_thread = None
		self._plot_data_worker = None

	def closeEvent(self, event: QCloseEvent):
		self._closing = True
		close_started = time.monotonic()
		self._shutdown_thread(self._initial_trips_thread, "initial_trips")
		self._shutdown_thread(self._plot_data_thread, "plot_data")
		for idx, t in enumerate(list(self._active_threads)):
			self._shutdown_thread(t, f"active_{idx}")
		if any(self._thread_is_running(t) for t in list(self._active_threads)) or self._thread_is_running(self._plot_data_thread):
			if self.args.debug:
				logger.warning(f"Closing with running workers after {time.monotonic() - close_started:.2f}s; detaching threads")
			self._detach_running_threads_for_close()
		super().closeEvent(event)

	def on_row_selected(self, selected, deselected):
		if self._suppress_trip_selection_handler:
			return
		self._invalidate_and_cancel_active_plot_load("Trip selection changed")
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			logger.debug(f"on_row_selected with {len(rows)} selected row(s): {rows[:5]}{'...' if len(rows) > 5 else ''}")
			self._populate_metric_columns(self._get_selected_fileids(rows))
			self._plot_refresh_timer.start(250)
		else:
			self._populate_metric_columns(None)
			self.stats_label.setText("No trip selected")
			self._set_all_metrics_table_model(pd.DataFrame(columns=["metric", "min", "avg", "max"]))
