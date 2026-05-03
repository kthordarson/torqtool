from __future__ import annotations

from typing import Any

import pandas as pd
import matplotlib.pyplot as plt
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
		left_layout = QVBoxLayout(left_panel)
		left_layout.setContentsMargins(2, 2, 2, 2)
		left_layout.setSpacing(2)
		left_layout.addWidget(self.groups_table)
		left_layout.addWidget(top_row_widget)

		plot_panel = QWidget()
		plot_layout = QVBoxLayout(plot_panel)
		self.fig = plt.figure(figsize=(8, 6))
		self.ax_map = self.fig.add_subplot(2, 1, 1)
		self.ax_time = self.fig.add_subplot(2, 1, 2)
		self.canvas = FigureCanvas(self.fig)
		plot_layout.addWidget(self.canvas)

		splitter.addWidget(left_panel)
		splitter.addWidget(plot_panel)
		splitter.setSizes([420, 780])
		main_layout.addWidget(splitter)

		self.setCentralWidget(central)
		self.load_data()

	def set_table_font_size(self, value: int):
		self._table_font_size = max(6, min(14, int(value)))
		self.groups_table.setFont(QFont("Monospace", self._table_font_size))

	def _plot_selected_groups(self):
		if self.groups_table.selectionModel() is None or self._grouped_df.empty:
			self._plot_for_fileids([])
			return
		selection_model = self.groups_table.selectionModel()
		rows = selection_model.selectedRows()
		if not rows:
			rows = selection_model.selectedIndexes()
		if not rows and self.groups_table.currentIndex().isValid():
			rows = [self.groups_table.currentIndex()]
		if not rows:
			self._plot_for_fileids([])
			return
		fileids: list[int] = []
		for row in rows:
			group_index = self.groups_table.model().index(int(row.row()), 0)
			group_name = str(self.groups_table.model().data(group_index, Qt.ItemDataRole.DisplayRole) or "")
			if not group_name:
				continue
			fileids.extend(self._group_to_fileids.get(group_name, []))
		if not fileids:
			for row in rows:
				view_idx = int(row.row())
				if 0 <= view_idx < len(self._grouped_df.index):
					group_name = str(self._grouped_df.iloc[view_idx]["group"])
					fileids.extend(self._group_to_fileids.get(group_name, []))
		self._plot_for_fileids(sorted(set(fileids)))

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

	def _group_key(self, row: pd.Series) -> str:
		if self._group_mode == "start":
			return f"S{int(row.get('startid') or 0)} {row.get('start_label', '')}"
		if self._group_mode == "end":
			return f"E{int(row.get('endid') or 0)} {row.get('end_label', '')}"
		return (
			f"S{int(row.get('startid') or 0)} {row.get('start_label', '')}"
			f" -> E{int(row.get('endid') or 0)} {row.get('end_label', '')}"
		)

	def _refresh_group_table(self) -> None:
		if self._detail_df.empty:
			self._grouped_df = pd.DataFrame(columns=["group", "trips", "distance_km", "avg_time_min", "latest_trip"])
			self.groups_table.setModel(PandasModel(self._grouped_df))
			self._group_to_fileids = {}
			return

		df = self._detail_df.copy()
		df["group"] = df.apply(self._group_key, axis=1)
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
		self._group_mode = str(self.group_mode_combo.currentData() or "pair")
		self._refresh_group_table()
		self._plot_for_fileids([])

	def _on_group_selection_changed(self, selected, deselected):
		if self.groups_table.selectionModel() is None or self._grouped_df.empty:
			return
		rows = self.groups_table.selectionModel().selectedRows()
		if not rows:
			self._plot_for_fileids([])
			return
		fileids: list[int] = []
		for row in rows:
			view_idx = int(row.row())
			if view_idx < 0 or view_idx >= len(self._grouped_df.index):
				continue
			group_name = str(self._grouped_df.iloc[view_idx]["group"])
			fileids.extend(self._group_to_fileids.get(group_name, []))
		self._plot_for_fileids(sorted(set(fileids)))

	def _plot_for_fileids(self, fileids: list[int]) -> None:
		self._active_fileids = fileids
		self.ax_map.clear()
		self.ax_time.clear()
		if not fileids:
			self.stats_label.setText("No group selected")
			self.ax_map.set_title("Select one or more groups")
			self.canvas.draw_idle()
			return

		df = self._detail_df[self._detail_df["fileid"].isin(fileids)].copy()
		if df.empty:
			self.canvas.draw_idle()
			return

		start_df = df[df["latstart"].notna() & df["lonstart"].notna()].copy()
		end_df = df[df["latend"].notna() & df["lonend"].notna()].copy()
		if not start_df.empty:
			self.ax_map.scatter(
				pd.to_numeric(start_df["lonstart"], errors="coerce"),
				pd.to_numeric(start_df["latstart"], errors="coerce"),
				s=24,
				c="tab:blue",
				alpha=0.7,
				label="start",
			)
		if not end_df.empty:
			self.ax_map.scatter(
				pd.to_numeric(end_df["lonend"], errors="coerce"),
				pd.to_numeric(end_df["latend"], errors="coerce"),
				s=24,
				c="tab:red",
				alpha=0.7,
				label="end",
			)
		if not start_df.empty or not end_df.empty:
			self.ax_map.legend(loc="best", fontsize=8)
		self.ax_map.set_xlabel("Longitude")
		self.ax_map.set_ylabel("Latitude")
		self.ax_map.set_title(f"Start/End points for {len(fileids)} selected trip(s)")

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
			self.fig.autofmt_xdate(rotation=30)

		distance_km = float(df["trip_distance_m"].fillna(0).sum()) / 1000.0
		self.stats_label.setText(f"Trips: {len(fileids)} | Total distance: {distance_km:.2f} km")
		self.canvas.draw_idle()
