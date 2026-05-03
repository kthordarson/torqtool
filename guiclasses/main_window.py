import io
from typing import Any, cast
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
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
from .map_canvas import MapCanvas
from .time_series_canvas import TimeSeriesCanvas
from .basemap_worker import BasemapWorker
from .trip_list_worker import TripListWorker
from .trip_plot_worker import TripPlotWorker
from .position_manager_window import PositionManagerWindow
from .start_end_window import StartEndWindow
from .pandas_model import PandasModel
from ._helpers import _normalize_col_name, format_duration


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
		self._dot_size_scale = 1.0
		self._plot_refresh_timer = QTimer(self)
		self._plot_refresh_timer.setSingleShot(True)
		self._plot_refresh_timer.timeout.connect(self.refresh_plot)
		self._basemap_request_id = 0
		self._basemap_request_context: dict[int, dict[str, object]] = {}
		self._basemap_thread: QThread | None = None
		self._basemap_worker: BasemapWorker | None = None
		self._show_all_start_end_points = False
		self._start_end_overlay_artists: list[Any] = []
		self._start_end_overlay_labels: list[Any] = []
		self._start_end_overlay_data: list[dict[str, Any]] = []
		self._mw_full_bounds: tuple[float, float, float, float] | None = None
		self._mw_current_fileids: list[int] = []
		self._mw_last_metric: str = 'speedobdkmh'
		self._metric_summary_cache: dict[tuple[int, ...], pd.DataFrame] = {}
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
		self._plot_request_context: dict[int, dict[str, Any]] = {}
		self._active_threads: set[QThread] = set()
		self._position_manager_window: PositionManagerWindow | None = None
		self._start_end_window: StartEndWindow | None = None
		self._position_manager_embedded_widget: QWidget | None = None
		self._start_end_embedded_widget: QWidget | None = None
		self._positions_tab_container: QWidget | None = None
		self._start_end_tab_container: QWidget | None = None
		self._positions_tab_layout: QVBoxLayout | None = None
		self._start_end_tab_layout: QVBoxLayout | None = None
		self._all_trips_df = pd.DataFrame(columns=["id", "fileid", "trip_distance", "tripdate", "time", "trip_distance_sort", "tripdate_raw", "time_raw"])
		self._trip_table_font_size = 8
		self._point_sample_percent = 10
		self._bounds_padding_ratio = 0.06
		self.Session = sessionmaker(bind=self.engine)
		self.session = self.Session()
		self._ensure_map_cache_schema()

		# Set up UI
		splitter = QSplitter(Qt.Orientation.Horizontal)
		self.main_splitter = splitter
		self.table = QTableView()
		self.label_groups_table = QTableView()
		self.map_canvas = MapCanvas()
		self.map_canvas.mpl_connect("pick_event", self._on_map_pick)
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
		self.sample_percent_spin = QSpinBox()
		self.sample_percent_spin.setRange(1, 100)
		self.sample_percent_spin.setValue(self._point_sample_percent)
		self.sample_percent_spin.setFixedWidth(72)
		self.sample_percent_spin.setToolTip("Approximate percentage of torqlogs points to render")
		self.sample_percent_spin.valueChanged.connect(self._on_sampling_changed)
		self.sample_refresh_btn = QPushButton("Refresh")
		self.sample_refresh_btn.setFixedHeight(24)
		self.sample_refresh_btn.clicked.connect(lambda: self._plot_refresh_timer.start(50))
		padding_label = QLabel("Bds %:")
		self.bounds_padding_spin = QSpinBox()
		self.bounds_padding_spin.setRange(1, 30)
		self.bounds_padding_spin.setValue(int(self._bounds_padding_ratio * 100))
		self.bounds_padding_spin.setFixedWidth(72)
		self.bounds_padding_spin.setToolTip("Padding around trip bounds before fetching basemap")
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
		self.metric_table = QTableView()
		self.metric_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		self.metric_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
		self.metric_table.setSortingEnabled(True)
		self.metric_table.verticalHeader().setVisible(False)
		mono_font = QFont("Monospace", 8)
		self.metric_table.setFont(mono_font)
		self._metric_df = pd.DataFrame(columns=["name", "min", "max", "avg"])
		self._set_metric_table_model(self._metric_df)
		metric_panel_layout.addWidget(metric_title)
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
		stats_layout.addWidget(stats_title)
		stats_layout.addWidget(stats_scroll)

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
		splitter.setSizes([150, 600])  # Give more space to the map panel

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
		embedded = self._start_end_window.takeCentralWidget()
		if embedded is None:
			return
		embedded.setParent(self._start_end_tab_container)
		embedded.setMinimumSize(0, 0)
		embedded.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Expanding)
		self._start_end_tab_layout.addWidget(embedded)
		embedded.show()
		self._start_end_embedded_widget = embedded

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
		self._current_colormap = colormap_name
		for name, action in self._colormap_actions.items():
			action.setChecked(name == colormap_name)
		self._plot_refresh_timer.start(200)

	def on_zoom_changed(self, zoom_level):
		"""Called when user changes map zoom"""
		# Debounce zoom updates to avoid blocking UI with repeated basemap fetches.
		self._plot_refresh_timer.start(300)

	def on_dot_size_changed(self, value: int):
		"""Called when user adjusts map dot size multiplier."""
		self._dot_size_scale = float(value) / 100.0
		self.dot_size_value_label.setText(f"{self._dot_size_scale:.2f}x")
		self._plot_refresh_timer.start(120)

	def _sample_step(self) -> int:
		pct = max(1, min(100, int(self._point_sample_percent)))
		return max(1, int(round(100.0 / float(pct))))

	def _on_sampling_changed(self, value: int):
		self._point_sample_percent = max(1, min(100, int(value)))
		self._trip_plot_cache.clear()
		self._trip_geo_cache.clear()
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
		except ValueError:
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
		self._apply_trip_filters(auto_select_latest=False)

	def on_metric_selection_changed(self):
		"""Called when user changes the metric selection in the list."""
		if self._get_selected_metrics():
			self._plot_refresh_timer.start(200)

	def _on_toggle_all_start_end(self, checked: bool):
		self._show_all_start_end_points = bool(checked)
		self.toggle_all_start_end_btn.setText("All points on" if checked else "All points off")
		self._plot_refresh_timer.start(120)

	@staticmethod
	def _lonlat_to_web_mercator(lon: float, lat: float) -> tuple[float, float]:
		lat_clamped = max(-85.05112878, min(85.05112878, float(lat)))
		x = float(lon) * 20037508.34 / 180.0
		y = np.log(np.tan(np.pi / 4.0 + np.deg2rad(lat_clamped) / 2.0)) * 6378137.0
		return float(x), float(y)

	@staticmethod
	def _web_mercator_to_lonlat(x: float, y: float) -> tuple[float, float]:
		lon = (float(x) / 20037508.34) * 180.0
		lat = np.rad2deg(2.0 * np.arctan(np.exp(float(y) / 6378137.0)) - np.pi / 2.0)
		return float(lon), float(lat)

	def _clear_start_end_overlays(self):
		for artist in self._start_end_overlay_artists:
			if artist is None or getattr(artist, "axes", None) is None:
				continue
			try:
				artist.remove()
			except NotImplementedError as e:
				logger.debug(f"Start/end overlay artist already detached during redraw: {e} ({type(e)})")
			except Exception as e:
				logger.error(f"Failed to remove start/end overlay artist: {e} ({type(e)})")
		for lbl in self._start_end_overlay_labels:
			if lbl is None or getattr(lbl, "axes", None) is None:
				continue
			try:
				lbl.remove()
			except NotImplementedError as e:
				logger.debug(f"Start/end overlay label already detached during redraw: {e} ({type(e)})")
			except Exception as e:
				logger.error(f"Failed to remove start/end overlay label: {e} ({type(e)})")
		self._start_end_overlay_artists = []
		self._start_end_overlay_labels = []
		self._start_end_overlay_data = []

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

	def _load_visible_start_end_points(self, bounds_mercator: tuple[float, float, float, float]) -> list[dict[str, Any]]:
		xmin, xmax, ymin, ymax = bounds_mercator
		lon_min, lat_min = self._web_mercator_to_lonlat(xmin, ymin)
		lon_max, lat_max = self._web_mercator_to_lonlat(xmax, ymax)
		lat_lo, lat_hi = (min(lat_min, lat_max), max(lat_min, lat_max))
		lon_lo, lon_hi = (min(lon_min, lon_max), max(lon_min, lon_max))
		q = text(
			"""
			SELECT 'start' AS pos_type, startid AS pos_id, latstart AS lat, lonstart AS lon, label
			FROM startpos
			WHERE latstart BETWEEN :lat_lo AND :lat_hi
			  AND lonstart BETWEEN :lon_lo AND :lon_hi
			UNION ALL
			SELECT 'end' AS pos_type, endid AS pos_id, latend AS lat, lonend AS lon, label
			FROM endpos
			WHERE latend BETWEEN :lat_lo AND :lat_hi
			  AND lonend BETWEEN :lon_lo AND :lon_hi
			"""
		)
		with self.engine.connect() as conn:
			rows = conn.execute(q, {
				"lat_lo": lat_lo,
				"lat_hi": lat_hi,
				"lon_lo": lon_lo,
				"lon_hi": lon_hi,
			}).mappings().all()
		return [dict(r) for r in rows if r.get("lat") is not None and r.get("lon") is not None and r.get("pos_id") is not None]

	def _overlay_start_end_points(self, fileids: list[int], bounds_mercator: tuple[float, float, float, float] | None):
		self._clear_start_end_overlays()
		selected_points = self._load_selected_file_start_end_points(fileids)
		all_points: list[dict[str, Any]] = []
		if self._show_all_start_end_points and bounds_mercator is not None:
			all_points = self._load_visible_start_end_points(bounds_mercator)

		seen: set[tuple[str, int]] = set()
		merged: list[tuple[dict[str, Any], bool]] = []
		for p in selected_points:
			key = (str(p.get("pos_type", "")), int(p.get("pos_id", 0)))
			if key in seen:
				continue
			seen.add(key)
			merged.append((p, True))
		for p in all_points:
			key = (str(p.get("pos_type", "")), int(p.get("pos_id", 0)))
			if key in seen:
				continue
			seen.add(key)
			merged.append((p, False))

		for point, is_selected_file in merged:
			lat = float(point.get("lat", 0.0))
			lon = float(point.get("lon", 0.0))
			x, y = self._lonlat_to_web_mercator(lon, lat)
			pos_type = str(point.get("pos_type", ""))
			pos_id = int(point.get("pos_id", 0))
			label_text = str(point.get("label", "")).strip()
			prefix = "S" if pos_type == "start" else "E"
			full_label = f"{prefix}{pos_id}: {label_text}" if label_text else f"{prefix}{pos_id}"
			color = "limegreen" if pos_type == "start" else "darkorange"
			alpha = 1.0 if is_selected_file else 0.55
			size = 90 if is_selected_file else 48
			artist = self.map_canvas.ax.scatter([x], [y], s=size, c=color, marker="D", edgecolors="black", linewidths=0.5, alpha=alpha, zorder=4, picker=8)
			self._start_end_overlay_artists.append(artist)
			self._start_end_overlay_data.append(dict(point))
			label_artist = self.map_canvas.ax.annotate(
				full_label,
				(x, y),
				xytext=(3, -10),
				textcoords="offset points",
				ha="left",
				va="top",
				fontsize=7,
				color="black",
				bbox={"boxstyle": "round,pad=0.12", "facecolor": "white", "alpha": 0.6, "edgecolor": "none"},
				zorder=5,
			)
			self._start_end_overlay_labels.append(label_artist)

	def _get_selected_metrics(self) -> list[str]:
		"""Return all currently selected metric names from the metric table."""
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
		"""Return the first selected metric (used for map coloring)."""
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
		self._metric_df = df.reset_index(drop=True)
		self.metric_table_model = PandasModel(self._metric_df)
		self.metric_table.setModel(self.metric_table_model)
		self.metric_table.horizontalHeader().setStretchLastSection(True)
		self.metric_table.resizeColumnsToContents()
		selection_model = self.metric_table.selectionModel()
		if selection_model is not None:
			selection_model.selectionChanged.connect(lambda *_: self.on_metric_selection_changed())

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
		sort_overrides = {'trip_distance': 'trip_distance_sort'} if 'trip_distance_sort' in self.df_trips.columns else None
		self.table_model = PandasModel(self.df_trips, display_columns=display_columns, sort_overrides=sort_overrides)
		self.table.setModel(self.table_model)
		font = self.table.font()
		font.setPointSize(self._trip_table_font_size)
		self.table.setFont(font)
		self.table.setSortingEnabled(True)
		self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
		# Allow Ctrl/Shift multi-select so multiple trips can be plotted together.
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
		thread.finished.connect(lambda: setattr(self, '_initial_trips_thread', None))

		self._initial_trips_worker = worker
		self._initial_trips_thread = thread
		self._active_threads.add(thread)
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

	def _plot_label_groups_on_map(self, labels: list[str]):
		if not labels or self._start_end_points_df.empty:
			return

		points = self._start_end_points_df[self._start_end_points_df["label_group"].isin(labels)].copy()
		if self._label_group_mode == "start":
			points = points[points["pos_type"] == "start"]
		elif self._label_group_mode == "end":
			points = points[points["pos_type"] == "end"]
		if points.empty:
			return

		points["x"] = points.apply(lambda r: self._lonlat_to_web_mercator(float(r["lon"]), float(r["lat"]))[0], axis=1)
		points["y"] = points.apply(lambda r: self._lonlat_to_web_mercator(float(r["lon"]), float(r["lat"]))[1], axis=1)

		self._clear_start_end_overlays()
		self.map_canvas.ax.clear()
		self.timeseries_canvas.ax.clear()
		self.timeseries_canvas.draw_idle()

		start_points = points[points["pos_type"] == "start"]
		end_points = points[points["pos_type"] == "end"]
		if not start_points.empty:
			self.map_canvas.ax.scatter(start_points["x"], start_points["y"], s=42, c="limegreen", marker="o", alpha=0.75, label="start", zorder=2)
		if not end_points.empty:
			self.map_canvas.ax.scatter(end_points["x"], end_points["y"], s=42, c="darkorange", marker="^", alpha=0.75, label="end", zorder=2)

		all_x = points["x"].tolist()
		all_y = points["y"].tolist()
		bounds = self._compute_plot_bounds(all_x, all_y)
		if bounds:
			xmin, xmax, ymin, ymax = bounds
			self.map_canvas.ax.set_xlim(xmin, xmax)
			self.map_canvas.ax.set_ylim(ymin, ymax)
			self._mw_full_bounds = bounds
			self._mw_current_fileids = []
			self._mw_last_metric = "labels"
			label_zoom = max(12, int(self.zoom_combo.currentText()))
			self._start_async_basemap(bounds, label_zoom, [], self._current_colormap, "labels")

		self.map_canvas.ax.legend(loc="best", fontsize=8)
		self.map_canvas.ax.set_title(f"Start/End labels: {', '.join(labels[:3])}{'...' if len(labels) > 3 else ''}")
		self.map_canvas.ax.set_xlabel("Longitude")
		self.map_canvas.ax.set_ylabel("Latitude")
		self.map_canvas.draw_idle()
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

		selection_model = self.table.selectionModel()
		if selection_model is None:
			return

		table_df = self.df_trips.reset_index(drop=True)
		matching_rows = [
			int(idx)
			for idx, fid in enumerate(pd.to_numeric(table_df.get("fileid"), errors="coerce").fillna(-1).astype(int).tolist())
			if fid in fileids
		]
		if not matching_rows:
			QMessageBox.information(self, "No matches", "No visible trips match the selected labels.")
			return

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
		self._plot_refresh_timer.start(120)

	def _on_left_tab_changed(self, index: int):
		# Hide right panel (trip map + metric plot) when on Positions tab.
		if hasattr(self, "right_panel") and self.right_panel is not None:
			self.right_panel.setVisible(index != 2)
		if hasattr(self, "main_splitter") and self.main_splitter is not None:
			sizes = self.main_splitter.sizes()
			if len(sizes) >= 2:
				total = max(1, sizes[0] + sizes[1])
				if index == 0:
					self.main_splitter.setSizes([320, max(900, total - 320)])
		# Keep metric panel in sync when returning to Trips tab.
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

		selection_model.clearSelection()
		restored_any = False
		for row in range(len(self._metric_df.index)):
			metric_name = str(self._metric_df.iloc[row]["name"])
			if metric_name in prev_selected:
				self.metric_table.selectRow(row)
				restored_any = True

		if not restored_any:
			self.metric_table.selectRow(0)

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
		"""Refresh the current plot with selected rows"""
		# Get currently selected rows and replot
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			logger.debug(f"refresh_plot triggered with {len(rows)} selected row(s): {rows[:5]}{'...' if len(rows) > 5 else ''}")
			if len(rows) >= self._plot_async_threshold:
				self._start_async_plot_for_rows(rows)
				return
			self._plot_for_rows(rows)

	def _start_async_plot_for_rows(self, rows: list[int]):
		fileids = self._get_selected_fileids(rows)
		if not fileids:
			self.stats_label.setText("No trip selected")
			return

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

		self._plot_data_request_id += 1
		request_id = self._plot_data_request_id
		self._plot_request_context[request_id] = {
			"fileids": list(fileids),
			"selected_metrics": list(selected_metrics),
			"selected_metric": selected_metric,
			"colormap_name": self._current_colormap,
			"preview_trips": {},
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
		thread.finished.connect(lambda: setattr(self, '_plot_data_thread', None))

		self._plot_data_worker = worker
		self._plot_data_thread = thread
		self._active_threads.add(thread)
		thread.start()

	def _on_async_plot_preview(self, request_id: int, payload: object):
		if request_id != self._plot_data_request_id:
			return
		ctx = self._plot_request_context.get(request_id)
		if not ctx:
			return

		preview_payload = cast(dict[str, Any], payload)
		trip_payload = cast(dict[str, Any] | None, preview_payload.get("trip"))
		done = int(preview_payload.get("done", 0) or 0)
		total = int(preview_payload.get("total", 0) or 0)

		preview_trips = cast(dict[int, dict[str, Any]], ctx.get("preview_trips", {}))
		if trip_payload:
			fid = int(trip_payload.get("fileid", -1))
			if fid >= 0:
				preview_trips[fid] = {
					"fileid": fid,
					"x": list(trip_payload.get("x", [])),
					"y": list(trip_payload.get("y", [])),
				}

		if not preview_trips:
			self.stats_label.setText(f"Loading trip paths... ({done}/{max(1, total)})")
			return

		self.map_canvas.ax.clear()
		cmap_name = cast(str, ctx.get("colormap_name", self._current_colormap))
		cmap = plt.colormaps[cmap_name]
		fileids = cast(list[int], ctx.get("fileids", []))
		fileid_color_map = self._build_fileid_color_map(fileids, cmap_name)

		all_x: list[float] = []
		all_y: list[float] = []
		for idx, trip in enumerate(preview_trips.values()):
			x_vals = cast(list[float], trip.get("x", []))
			y_vals = cast(list[float], trip.get("y", []))
			if not x_vals or not y_vals:
				continue
			fileid = int(trip.get("fileid", -1))
			all_x.extend(x_vals)
			all_y.extend(y_vals)
			self.map_canvas.ax.scatter(
				x_vals,
				y_vals,
				s=max(1.0, 5.0 * self._dot_size_scale),
				c=[fileid_color_map.get(fileid, cmap(idx % self._colormap_cycle_length(cmap_name)))],
				alpha=0.65,
				zorder=2,
			)

		bounds = self._compute_plot_bounds(all_x, all_y)
		if bounds:
			xmin, xmax, ymin, ymax = bounds
			self.map_canvas.ax.set_xlim(xmin, xmax)
			self.map_canvas.ax.set_ylim(ymin, ymax)
			self._mw_full_bounds = bounds
			self._mw_current_fileids = fileids
			self._mw_last_metric = "preview"
			self._overlay_start_end_points(fileids, bounds)

		self.map_canvas.ax.set_title("Trip Map - loading paths preview")
		self.map_canvas.ax.set_xlabel("Longitude")
		self.map_canvas.ax.set_ylabel("Latitude")
		self.map_canvas.draw_idle()
		self.stats_label.setText(f"Loading trip paths... ({done}/{max(1, total)})")

	def _cancel_async_plot_load(self):
		thread = self._plot_data_thread
		if thread is None:
			self.cancel_plot_load_btn.setEnabled(False)
			return
		if thread.isRunning():
			thread.requestInterruption()
			self.stats_label.setText("Cancelling background load...")
		self.cancel_plot_load_btn.setEnabled(False)

	def _on_async_plot_data_loaded(self, request_id: int, payload: object):
		if request_id != self._plot_data_request_id:
			return
		self.cancel_plot_load_btn.setEnabled(False)
		ctx = self._plot_request_context.pop(request_id, None)
		if not ctx:
			return

		data = cast(dict[str, Any], payload)
		trips = cast(list[dict[str, Any]], data.get("trips", []))
		all_x = cast(list[float], data.get("all_x", []))
		all_y = cast(list[float], data.get("all_y", []))
		all_metric_values = cast(list[float], data.get("all_metric_values", []))

		fileids = cast(list[int], ctx["fileids"])
		selected_metrics = cast(list[str], ctx["selected_metrics"])
		selected_metric = cast(str, ctx["selected_metric"])
		colormap_name = cast(str, ctx["colormap_name"])

		for item in trips:
			fid = int(item.get("fileid", -1))
			if fid >= 0:
				self._trip_plot_cache[(fid, selected_metric)] = {
					"x": list(item.get("x", [])),
					"y": list(item.get("y", [])),
					"speed": list(item.get("speed", [])),
					"time": list(item.get("time", [])),
				}

		self._clear_start_end_overlays()
		self.map_canvas.ax.clear()

		cmap = plt.colormaps[colormap_name]
		fileid_color_map = self._build_fileid_color_map(fileids, colormap_name)

		plots = []
		for idx, trip in enumerate(trips):
			x_vals = cast(list[float], trip.get("x", []))
			y_vals = cast(list[float], trip.get("y", []))
			speed_vals = pd.to_numeric(pd.Series(trip.get("speed", [])), errors='coerce').fillna(0)
			if not x_vals or not y_vals:
				continue
			sizes = (speed_vals.clip(lower=1, upper=50) * self._dot_size_scale).clip(lower=1, upper=200)
			fileid = int(trip.get("fileid", -1))
			base_color = fileid_color_map.get(fileid, cmap(idx % self._colormap_cycle_length(colormap_name)))
			sc = self.map_canvas.ax.scatter(
				x_vals,
				y_vals,
				s=sizes,
				c=[base_color],
				label=f"fileid {int(trip.get('fileid', -1))}",
				zorder=2,
			)
			plots.append(sc)

		bounds = self._compute_plot_bounds(all_x, all_y)
		effective_zoom = self._base_zoom_for_selection(fileids, int(self.zoom_combo.currentText()))
		cached_payload: tuple[bytes, tuple[float, float, float, float]] | None = None
		if bounds:
			effective_zoom = self._effective_basemap_zoom(bounds, effective_zoom)
			cached_payload = self._load_cached_map_image(fileids, effective_zoom, colormap_name, selected_metric)
			xmin, xmax, ymin, ymax = bounds
			self.map_canvas.ax.set_xlim(xmin, xmax)
			self.map_canvas.ax.set_ylim(ymin, ymax)
			self._mw_full_bounds = bounds
			self._mw_current_fileids = list(fileids)
			self._mw_last_metric = selected_metric

		self._overlay_start_end_points(fileids, bounds)

		if plots and cached_payload:
			cached_img, cached_ext = cached_payload
			img = mpimg.imread(io.BytesIO(cached_img), format='png')
			self.map_canvas.ax.imshow(img, extent=cached_ext, interpolation='bilinear', zorder=0)
		elif plots and bounds:
			self._start_async_basemap(bounds, effective_zoom, fileids, colormap_name, selected_metric)

		self.map_canvas.ax.set_title(f"Trip Map - {selected_metric}")
		self.map_canvas.ax.set_xlabel("Longitude")
		self.map_canvas.ax.set_ylabel("Latitude")
		self.map_canvas.draw_idle()
		self._update_timeseries_plot(fileids, selected_metrics, colormap_name, fileid_color_map)
		self._update_stats_panel(fileids, all_metric_values, all_x, all_y, selected_metric)

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
		order_col = f'"{time_col}"' if time_col else 'id'
		sample_where = " AND MOD(id, :sample_step) = 0" if self._sample_step() > 1 else ""
		q = text(
			f'SELECT "{lon_col}" AS longitude, "{lat_col}" AS latitude{time_select} '
			f'FROM torqlogs WHERE fileid = :fileid{sample_where} ORDER BY {order_col}'
		)
		params: dict[str, Any] = {"fileid": int(fileid)}
		if self._sample_step() > 1:
			params["sample_step"] = int(self._sample_step())
		try:
			df_geo = pd.read_sql(q, self.engine, params=params)
		except Exception as e:
			logger.error(f"Failed to load geo data for trip fileid={fileid}: {e} ({type(e)})")
			df_geo = pd.DataFrame()
		if df_geo.empty:
			payload = {"x": [], "y": [], "time": []}
			self._trip_geo_cache[fileid] = payload
			return payload

		try:
			gdf = gpd.GeoDataFrame(
				df_geo,
				geometry=[Point(xy) for xy in zip(df_geo['longitude'], df_geo['latitude'])],
				crs="EPSG:4326",
			).to_crs(epsg=3857)
		except KeyError as e:
			logger.error(f"Missing expected geo columns in trip data: {e} fileid={fileid}")
			return None

		time_values: list = []
		if 'metric_time' in df_geo.columns:
			time_values = pd.to_datetime(df_geo['metric_time'], errors='coerce').tolist()

		payload = {
			"x": gdf.geometry.x.tolist(),
			"y": gdf.geometry.y.tolist(),
			"time": time_values,
		}
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

		speed_col_name = self._resolve_actual_torqlogs_column(metric_name)
		time_col = (self._resolve_actual_torqlogs_column('gpstime')
					or self._resolve_actual_torqlogs_column('devicetime'))
		if not speed_col_name:
			return None

		time_order = f' ORDER BY "{time_col}"' if time_col else ' ORDER BY id'
		sample_where = " AND MOD(id, :sample_step) = 0" if self._sample_step() > 1 else ""
		q = text(
			f'SELECT "{speed_col_name}" AS selectedmetric '
			f'FROM torqlogs WHERE fileid = :fileid{sample_where}{time_order}'
		)
		params: dict[str, Any] = {"fileid": int(fileid)}
		if self._sample_step() > 1:
			params["sample_step"] = int(self._sample_step())
		df_part = pd.read_sql(q, self.engine, params=params)
		if df_part.empty:
			self._trip_plot_cache[cache_key] = {"x": [], "y": [], "speed": [], "time": []}
			return self._trip_plot_cache[cache_key]
		speed_series = pd.to_numeric(df_part['selectedmetric'], errors='coerce').fillna(0)

		x_vals = geo_payload["x"]
		y_vals = geo_payload["y"]
		time_values = geo_payload["time"]
		points = min(len(x_vals), len(y_vals), len(speed_series))
		if time_values:
			points = min(points, len(time_values))

		payload: dict[str, list] = {
			"x": x_vals[:points],
			"y": y_vals[:points],
			"speed": speed_series.tolist()[:points],
			"time": time_values[:points] if time_values else [],
		}
		self._trip_plot_cache[cache_key] = payload
		logger.debug(f"Loaded trip plot data for fileid={fileid}, metric_name={metric_name}, points={len(payload['x'])}")
		return payload

	def _selection_key(self, fileids: list[int], metric_name: str) -> str:
		return (
			f"{self._map_cache_version}|metric={metric_name}|sample={self._sample_step()}|"
			+ ",".join(str(fid) for fid in sorted(fileids))
		)

	def _basemap_selection_key(self, fileids: list[int]) -> str:
		# Basemap tiles are independent of metric and colormap for a fixed trip selection/zoom.
		return f"{self._map_cache_version}|basemap|sample={self._sample_step()}|" + ",".join(str(fid) for fid in sorted(fileids))

	def _timeseries_selection_key(self, fileids: list[int], metric_names: list[str]) -> str:
		metrics_part = ",".join(metric_names)
		files_part = ",".join(str(fid) for fid in sorted(fileids))
		return f"{self._map_cache_version}|timeseries|sample={self._sample_step()}|metrics={metrics_part}|{files_part}"

	def _cache_fileid(self, fileids: list[int]) -> int | None:
		return int(fileids[0]) if len(fileids) == 1 else None

	def _load_cached_map_image(self, fileids: list[int], zoom: int, colormap: str, metric_name: str) -> tuple[bytes, tuple[float, float, float, float]] | None:
		selection_key = self._selection_key(fileids, metric_name)
		basemap_key = self._basemap_selection_key(fileids)
		q = text(
			"""
			SELECT image_png, ext_west, ext_east, ext_south, ext_north
			FROM mapimagecache
			WHERE (
				(selection_key = :selection_key AND zoom = :zoom AND colormap = :colormap)
				OR
				(selection_key = :basemap_key AND zoom = :zoom)
			)
			ORDER BY CASE WHEN selection_key = :selection_key AND colormap = :colormap THEN 0 ELSE 1 END
			LIMIT 1
			"""
		)
		with self.engine.connect() as conn:
			row = conn.execute(q, {
				"selection_key": selection_key,
				"basemap_key": basemap_key,
				"zoom": zoom,
				"colormap": colormap,
			}).first()
		if row and all(v is not None for v in row[1:5]):
			return row[0], (float(row[1]), float(row[2]), float(row[3]), float(row[4]))
		return None

	def _save_cached_map_image(self, fileids: list[int], zoom: int, colormap: str, metric_name: str, ext: tuple[float, float, float, float]):
		selection_key = self._selection_key(fileids, metric_name)
		basemap_key = self._basemap_selection_key(fileids)
		fileid = self._cache_fileid(fileids)
		ext_west, ext_east, ext_south, ext_north = ext
		buf = io.BytesIO()
		# Cache only the basemap raster; scatter/labels are redrawn dynamically.
		self.map_canvas.ax.figure.canvas.draw_idle()
		img_artists = self.map_canvas.ax.images
		if not img_artists:
			return
		arr = img_artists[0].get_array()
		if arr is None:
			return
		mpimg.imsave(buf, arr, format='png')
		image_bytes = buf.getvalue()
		upsert_sql = text(
			"""
			INSERT INTO mapimagecache (fileid, selection_key, zoom, colormap, image_png, ext_west, ext_east, ext_south, ext_north, created_at, updated_at)
			VALUES (:fileid, :selection_key, :zoom, :colormap, :image_png, :ext_west, :ext_east, :ext_south, :ext_north, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(selection_key, zoom, colormap)
			DO UPDATE SET
				fileid = excluded.fileid,
				image_png = excluded.image_png,
				ext_west = excluded.ext_west,
				ext_east = excluded.ext_east,
				ext_south = excluded.ext_south,
				ext_north = excluded.ext_north,
				updated_at = CURRENT_TIMESTAMP
			"""
		)
		with self.engine.begin() as conn:
			conn.execute(upsert_sql, {
				"fileid": fileid,
				"selection_key": selection_key,
				"zoom": zoom,
				"colormap": colormap,
				"image_png": image_bytes,
				"ext_west": ext_west,
				"ext_east": ext_east,
				"ext_south": ext_south,
				"ext_north": ext_north,
			})
			# Also store a metric/colormap-agnostic basemap entry for cross-metric reuse.
			conn.execute(upsert_sql, {
				"fileid": fileid,
				"selection_key": basemap_key,
				"zoom": zoom,
				"colormap": "basemap",
				"image_png": image_bytes,
				"ext_west": ext_west,
				"ext_east": ext_east,
				"ext_south": ext_south,
				"ext_north": ext_north,
			})
		logger.debug(f"Saved cached map image for selection_key={selection_key}, zoom={zoom}, colormap={colormap}")

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

	def _compute_plot_bounds(self, all_x: list[float], all_y: list[float]) -> tuple[float, float, float, float] | None:
		if not all_x or not all_y:
			return None
		xmin = min(all_x)
		xmax = max(all_x)
		ymin = min(all_y)
		ymax = max(all_y)

		dx = max(1.0, xmax - xmin)
		dy = max(1.0, ymax - ymin)
		pad_x = dx * self._bounds_padding_ratio
		pad_y = dy * self._bounds_padding_ratio
		return (xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y)

	def _effective_basemap_zoom(self, bounds: tuple[float, float, float, float], base_zoom: int) -> int:
		"""Increase basemap zoom automatically for short trip extents to reduce blur."""
		xmin, xmax, ymin, ymax = bounds
		span = max(1.0, xmax - xmin, ymax - ymin)
		boost = 0
		if span <= 1_500:
			boost = 4
		elif span <= 3_000:
			boost = 3
		elif span <= 7_000:
			boost = 2
		elif span <= 15_000:
			boost = 1
		# Respect current user-selected zoom as baseline, but improve detail for tight bounds.
		return max(1, min(18, base_zoom + boost))

	def _base_zoom_for_selection(self, fileids: list[int], fallback_zoom: int) -> int:
		if not fileids or self._all_trips_df.empty:
			return fallback_zoom
		try:
			subset = self._all_trips_df[self._all_trips_df["fileid"].isin(fileids)]
			if subset.empty:
				return fallback_zoom
			distances = pd.to_numeric(subset["trip_distance_sort"], errors="coerce").dropna()
			if not distances.empty and bool((distances <= 10_000.0).all()):
				return max(fallback_zoom, 12)
		except Exception as e:
			logger.debug(f"Could not evaluate short-trip zoom policy: {e} ({type(e)})")
		return fallback_zoom

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

	def _plot_for_rows(self, rows):
		fileids = self._get_selected_fileids(rows)
		if not fileids:
			self.stats_label.setText("No trip selected")
			return

		base_zoom = self._base_zoom_for_selection(fileids, int(self.zoom_combo.currentText()))
		colormap_name = self._current_colormap
		selected_metrics = self._get_selected_metrics()
		if not selected_metrics:
			fallback_metric = self._get_selected_metric()
			selected_metrics = [fallback_metric] if fallback_metric else []
		if not selected_metrics:
			self.stats_label.setText("No valid metrics available for plotting")
			return
		selected_metric = selected_metrics[0]
		cached_payload: tuple[bytes, tuple[float, float, float, float]] | None = None

		# Remove overlay artists before clearing axes so remove() has valid artist owners.
		self._clear_start_end_overlays()
		self.map_canvas.ax.clear()

		# Get selected colormap
		cmap = plt.colormaps[colormap_name]
		fileid_color_map = self._build_fileid_color_map(fileids, colormap_name)
		logger.debug(f"Using colormap: {colormap_name}")

		plots = []
		all_x: list[float] = []
		all_y: list[float] = []
		all_metric_values: list[float] = []
		for idx, fileid in enumerate(fileids):
			if self.args.debug:
				logger.debug(f"Processing fileid={fileid} ({idx + 1}/{len(fileids)}) for metric='{selected_metric}'")
			plot_data = self._load_trip_plot_data(fileid, selected_metric)
			if not plot_data:
				if self.args.debug:
					logger.warning(f"No plot data for fileid={fileid}, metric={selected_metric}")
				continue

			x_vals = plot_data["x"]
			y_vals = plot_data["y"]
			all_x.extend(x_vals)
			all_y.extend(y_vals)
			speed_vals = pd.to_numeric(pd.Series(plot_data["speed"]), errors='coerce').fillna(0)
			all_metric_values.extend(speed_vals.tolist())
			if len(x_vals) == 0:
				continue

			sizes = (speed_vals.clip(lower=1, upper=50) * self._dot_size_scale).clip(lower=1, upper=200)
			base_color = fileid_color_map.get(int(fileid), cmap(idx % self._colormap_cycle_length(colormap_name)))
			sc = self.map_canvas.ax.scatter(x_vals, y_vals, s=sizes, c=[base_color], label=f"fileid {fileid}", zorder=2)
			plots.append(sc)

		logger.debug(f"Plotted {len(plots)} trips on map for fileids: {len(fileids)}")
		bounds = self._compute_plot_bounds(all_x, all_y)
		effective_zoom = base_zoom
		if bounds:
			effective_zoom = self._effective_basemap_zoom(bounds, base_zoom)
			cached_payload = self._load_cached_map_image(fileids, effective_zoom, colormap_name, selected_metric)
			if effective_zoom != base_zoom:
				logger.debug(
					f"Adaptive basemap zoom: base={base_zoom}, effective={effective_zoom}, "
					f"metric={selected_metric}, fileids={fileids}"
				)
		if bounds:
			xmin, xmax, ymin, ymax = bounds
			self.map_canvas.ax.set_xlim(xmin, xmax)
			self.map_canvas.ax.set_ylim(ymin, ymax)
			self._mw_full_bounds = bounds
			self._mw_current_fileids = list(fileids)
			self._mw_last_metric = selected_metric

		self._overlay_start_end_points(fileids, bounds)

		if plots and cached_payload:
			cached_img, cached_ext = cached_payload
			img = mpimg.imread(io.BytesIO(cached_img), format='png')
			self.map_canvas.ax.imshow(img, extent=cached_ext, interpolation='bilinear', zorder=0)
		elif plots:
			if bounds:
				self._start_async_basemap(bounds, effective_zoom, fileids, colormap_name, selected_metric)
		self.map_canvas.ax.set_title(f"Trip Map - {selected_metric}")
		self.map_canvas.ax.set_xlabel("Longitude")
		self.map_canvas.ax.set_ylabel("Latitude")
		self.map_canvas.draw_idle()
		if self.args.debug:
			logger.debug(f"Map plot updated for fileids={fileids}, metric='{selected_metric}' with {len(all_x)} points")
		self._update_timeseries_plot(fileids, selected_metrics, colormap_name, fileid_color_map)
		if self.args.debug:
			logger.debug(f"Timeseries plot updated for fileids={fileids}, metrics={selected_metrics}")
		self._update_stats_panel(fileids, all_metric_values, all_x, all_y, selected_metric)
		if self.args.debug:
			logger.debug(f"Stats panel updated for fileids={fileids}, metric='{selected_metric}'")

	def _load_trip_metadata(self, fileids: list[int]) -> dict:
		"""Load trip metadata from torqfiles table."""
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
		all_x: list[float], all_y: list[float], trip_info: dict,
		all_metric_stats: dict | None = None) -> str:
		"""Format comprehensive trip stats with all metric min/max/avg."""
		W = 52
		lines: list[str] = []

		lines.append("═" * W)
		lines.append(f"TRIP SUMMARY  — {trip_count} trip(s), {point_count} pts")
		lines.append("═" * W)

		# Trip metadata & date
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

		# Selected metric summary
		category, display_name, unit = categorize_metric(metric_name)
		unit_str = f" {unit}" if unit else ""
		lines.append(f"\n{'─' * W}")
		lines.append(f"Selected: {display_name}{unit_str}")
		lines.append(f"  min {metric_min:.2f}  avg {metric_avg:.2f}  max {metric_max:.2f}")
		suggestion = get_analysis_suggestion(category)
		lines.append(f"  [{suggestion['analysis_type']}]  {suggestion['visualization']}")

		# All metrics grouped by category
		if all_metric_stats:
			grouped = group_metrics_by_category(list(all_metric_stats.keys()))
			lines.append(f"\n{'─' * W}")
			lines.append("ALL METRICS  (min / avg / max):")
			for cat, cat_metrics in grouped.items():
				cat_lines: list[str] = []
				for disp, u, orig in cat_metrics:
					stats = all_metric_stats.get(orig)
					if stats:
						u_s = f" {u}" if u else ""
						cat_lines.append(
							f"  {disp:<28s}  "
							f"{stats['min']:.1f}/{stats['avg']:.1f}/{stats['max']:.1f}{u_s}"
						)
				if cat_lines:
					lines.append(f"\n{cat.value}:")
					lines.extend(cat_lines)

		lines.append("\n" + "═" * W)
		return "\n".join(lines)

	def _update_stats_panel(self, fileids: list[int], metric_values: list[float], all_x: list[float], all_y: list[float], metric_name: str):
		trip_count = len(fileids)
		point_count = len(all_x)
		metric_series = pd.Series(metric_values, dtype="float64") if metric_values else pd.Series(dtype="float64")
		metric_min = float(metric_series.min()) if not metric_series.empty else 0.0
		metric_avg = float(metric_series.mean()) if not metric_series.empty else 0.0
		metric_max = float(metric_series.max()) if not metric_series.empty else 0.0

		# Selection-level metadata and aggregate stats are expensive; cache by selected fileids.
		selection_key = tuple(sorted(fileids))
		if selection_key in self._selection_stats_cache:
			trip_info, all_metric_stats = self._selection_stats_cache[selection_key]
		else:
			trip_info = self._load_trip_metadata(fileids)
			all_metric_stats = self._load_all_metric_stats(fileids)
			self._selection_stats_cache[selection_key] = (trip_info, all_metric_stats)

		# Build stats text with trip info, metrics, and suggestions
		stats_text = self._format_trip_stats(
			fileids, trip_count, point_count, metric_name,
			metric_min, metric_avg, metric_max, all_x, all_y, trip_info, all_metric_stats
		)

		self.stats_label.setText(stats_text)

	def _update_timeseries_plot(
		self,
		fileids: list[int],
		metric_names: list[str],
		colormap_name: str,
		fileid_color_map: dict[int, tuple[float, float, float, float]] | None = None,
	):
		"""Draw one or more metrics over time for selected trips."""
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
		# For multi-trip plots, keep one stable color per fileid across map and timeseries.
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
				# Fall back to sequential index when timestamps are unavailable or all-NaT.
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
						# Drop rows where the timestamp is NaT to avoid matplotlib ConversionError.
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
		"""Return the set of actual torqlogs column names that have a numeric DB type."""
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
		"""Load min/avg/max for all available metrics for the selected fileids."""
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
			logger.warning(f"Could not load all metric stats: {e}")
			return {}

	def _start_async_basemap(self, bounds: tuple[float, float, float, float], zoom: int, fileids: list[int], colormap_name: str, metric_name: str):
		xmin, xmax, ymin, ymax = bounds
		if xmax <= xmin or ymax <= ymin:
			return

		self._basemap_request_id += 1
		request_id = self._basemap_request_id
		self._basemap_request_context[request_id] = {
			"fileids": fileids,
			"zoom": zoom,
			"colormap": colormap_name,
			"metric": metric_name,
		}

		thread = QThread()
		worker = BasemapWorker((xmin, xmax, ymin, ymax), zoom, request_id)
		worker.moveToThread(thread)

		thread.started.connect(worker.run)
		worker.finished.connect(self._on_basemap_loaded)
		worker.error.connect(self._on_basemap_error)
		worker.finished.connect(thread.quit)
		worker.error.connect(thread.quit)
		thread.finished.connect(worker.deleteLater)
		thread.finished.connect(thread.deleteLater)
		thread.finished.connect(lambda t=thread: self._active_threads.discard(t))

		self._basemap_worker = worker
		self._basemap_thread = thread
		self._active_threads.add(thread)
		if self.args.debug:
			logger.debug(f'{self} starting basemapworker {thread} active threads: {len(self._active_threads)}) request_id={request_id} with bounds=({xmin}, {ymin}, {xmax}, {ymax}) and zoom={zoom}')

		# logger.debug(f"{self} Starting basemap worker thread for request_id={request_id} with bounds=({xmin}, {ymin}, {xmax}, {ymax}) and zoom={zoom}")
		thread.start()

	def _on_basemap_loaded(self, img, ext, request_id: int):
		try:
			if request_id != self._basemap_request_id:
				return
		except Exception as e:
			logger.error(f"Error in basemap loaded handler: {e} ({type(e)})")
			return
		self.map_canvas.ax.imshow(img, extent=ext, interpolation='bilinear', zorder=0)
		self.map_canvas.draw_idle()
		ctx = self._basemap_request_context.get(request_id)
		if ctx:
			a, b, c, d = ext
			ext_typed: tuple[float, float, float, float] = (float(a), float(b), float(c), float(d))
			self._save_cached_map_image(
				cast(list[int], ctx["fileids"]),
				cast(int, ctx["zoom"]),
				cast(str, ctx["colormap"]),
				cast(str, ctx["metric"]),
				ext_typed,
			)
			self._basemap_request_context.pop(request_id, None)

	def _on_basemap_error(self, err: str, request_id: int):
		try:
			if request_id != self._basemap_request_id:
				return
		except Exception as e:
			logger.error(f"Error in basemap error handler: {e} ({type(e)}) error: {err} request_id: {request_id}")
			return
		self._basemap_request_context.pop(request_id, None)
		# Tile/network failures should not break UI interaction.
		print(f"{self} Basemap load failed: {err} (request_id={request_id}) current_id={self._basemap_request_id}")

	def _shutdown_thread(self, thread: QThread | None, name: str):
		if self.args.debug:
			logger.debug(f"Stopping thread {name} {thread} from {self} active threads: {len(self._active_threads)})")
		if thread is None:
			return
		try:
			if not thread.isRunning():
				return
		except RuntimeError as e:
			if self.args.debug:
				logger.error(f"RuntimeError checking thread.isRunning() for '{name}': {e} ({type(e)})")
			return
		if thread.currentThread() is thread:
			return
		thread.requestInterruption()
		thread.quit()
		if not thread.wait(3000):
			if self.args.debug:
				logger.warning(f"Thread '{name}' did not stop in time; terminating")
			thread.terminate()
			thread.wait(1000)

	def _mw_force_reload_basemap(self):
		"""Evict DB cache for the current trip selection and re-fetch basemap from network."""
		if not self._mw_current_fileids:
			return
		files_part = ",".join(str(fid) for fid in sorted(self._mw_current_fileids))
		pattern = f"{self._map_cache_version}|%{files_part}"
		try:
			with self.engine.begin() as conn:
				conn.execute(
					text("DELETE FROM mapimagecache WHERE selection_key LIKE :pattern"),
					{"pattern": pattern},
				)
			logger.debug(f"MW: cleared map cache for fileids={self._mw_current_fileids}")
		except Exception as e:
			logger.warning(f"MW: could not clear map cache from DB: {e} ({type(e)})")
		self.refresh_plot()

	def _mw_zoom_in(self):
		ax = self.map_canvas.ax
		x0, x1 = ax.get_xlim()
		y0, y1 = ax.get_ylim()
		cx = (x0 + x1) / 2.0
		cy = (y0 + y1) / 2.0
		span_x = (x1 - x0) * 0.65
		span_y = (y1 - y0) * 0.65
		new_half_x = max(span_x, 1.0) / 2.0
		new_half_y = max(span_y, 1.0) / 2.0
		ax.set_xlim(cx - new_half_x, cx + new_half_x)
		ax.set_ylim(cy - new_half_y, cy + new_half_y)
		self.map_canvas.draw_idle()

	def _mw_zoom_out(self):
		ax = self.map_canvas.ax
		x0, x1 = ax.get_xlim()
		y0, y1 = ax.get_ylim()
		cx = (x0 + x1) / 2.0
		cy = (y0 + y1) / 2.0
		new_half_x = (x1 - x0) * 0.725
		new_half_y = (y1 - y0) * 0.725
		if self._mw_full_bounds:
			bx0, bx1, by0, by1 = self._mw_full_bounds
			max_half_x = (bx1 - bx0) * 1.0
			max_half_y = (by1 - by0) * 1.0
			new_half_x = min(new_half_x, max_half_x)
			new_half_y = min(new_half_y, max_half_y)
		ax.set_xlim(cx - new_half_x, cx + new_half_x)
		ax.set_ylim(cy - new_half_y, cy + new_half_y)
		self.map_canvas.draw_idle()

	def _mw_zoom_full(self):
		if not self._mw_full_bounds:
			return
		xmin, xmax, ymin, ymax = self._mw_full_bounds
		ax = self.map_canvas.ax
		ax.set_xlim(xmin, xmax)
		ax.set_ylim(ymin, ymax)
		self.map_canvas.draw_idle()

	def _on_map_pick(self, event):
		if event.artist not in self._start_end_overlay_artists:
			return
		try:
			idx = self._start_end_overlay_artists.index(event.artist)
		except ValueError:
			return
		if idx >= len(self._start_end_overlay_data):
			return
		point = self._start_end_overlay_data[idx]
		pos_type = str(point.get("pos_type", ""))
		pos_id = int(point.get("pos_id", 0))
		current_label = str(point.get("label", "") or "")
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
			point["label"] = new_label.strip()
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

	def closeEvent(self, event: QCloseEvent):
		self._shutdown_thread(self._basemap_thread, "basemap")
		self._shutdown_thread(self._initial_trips_thread, "initial_trips")
		for idx, t in enumerate(list(self._active_threads)):
			self._shutdown_thread(t, f"active_{idx}")
		super().closeEvent(event)

	def on_row_selected(self, selected, deselected):
		if self._suppress_trip_selection_handler:
			return
		rows = sorted(set(index.row() for index in self.table.selectionModel().selectedRows()))
		if rows:
			logger.debug(f"on_row_selected with {len(rows)} selected row(s): {rows[:5]}{'...' if len(rows) > 5 else ''}")
			self._populate_metric_columns(self._get_selected_fileids(rows))
			# Debounce bursty selection events while user is building a multi-row selection.
			self._plot_refresh_timer.start(250)
		else:
			self._populate_metric_columns(None)
			self.stats_label.setText("No trip selected")
