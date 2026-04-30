#!/usr/bin/python3
import sys
import pandas as pd
import PySide6
from typing import Any
from loguru import logger
from PySide6 import QtCore, QtSql
from PySide6.QtCharts import QChart, QChartView, QLineSeries, QScatterSeries
from PySide6.QtCore import QAbstractTableModel, Qt, QObject, QEvent
from PySide6.QtGui import QFont, QPen
from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtCore import QModelIndex, QPersistentModelIndex
import PySide6.QtCharts
import numpy as np
from sqlalchemy import text
from datamodels import Torqlogs, TorqFile, Speeds
from ui_untitled import Ui_main_window
from utils import get_engine_session
from converter import get_args
from numbers import Real

# x = latitude y = longitude !

def _to_float(value: object) -> float | None:
    if isinstance(value, Real) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, (str, bytes, bytearray, memoryview)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None

class Mymodel(QAbstractTableModel):
	pass

mymodel = Mymodel()

class KeyPressFilter(QObject):
	def event_filter(self, widget, event):
		if event.type() == QEvent.Type.KeyPress:
			text = event.text()
			logger.debug(f'Key {text} {event=}')
			if event.modifiers():
				text = event.keyCombination().key().name   # .decode(encoding="utf-8")
				logger.debug(f'event.modifierskeyboard {event.keyCombination().key().name} {event.keyCombination().key()} {event.keyCombination()}')
			# widget.label1.setText(text)
		return False


class TripplotModel(QtSql.QSqlQueryModel):
    def __init__(self, fileid):
        super().__init__()
        self.fileid = fileid
        self.setQuery(f'select latitude, longitude from torqlogs where fileid={self.fileid}')
        self.setHeaderData(1, QtCore.Qt.Orientation.Horizontal, "latitude")
        self.setHeaderData(2, QtCore.Qt.Orientation.Horizontal, "longitude")


class Torqfilemodel(QtSql.QSqlQueryModel):
    def __init__(self):
        super().__init__()
        self.setQuery('select fileid,trip_start,sent_rows from torqfiles ')
        self.setHeaderData(0, QtCore.Qt.Orientation.Horizontal, "fileid")
        self.setHeaderData(1, QtCore.Qt.Orientation.Horizontal, "trip_start")
        self.setHeaderData(2, QtCore.Qt.Orientation.Horizontal, "entries")

class CustomSqlModel(QtSql.QSqlQueryModel):
	def __init__(self) -> None:
		super().__init__()
		self.base_query = ""
		self.sort_columns: list[str] = []

	def data(
		self,
		index: QModelIndex | QPersistentModelIndex,
		role: int = QtCore.Qt.ItemDataRole.DisplayRole,
	) -> Any:
		value = super().data(index, role)

		if value is not None and role == QtCore.Qt.ItemDataRole.DisplayRole:
			if index.column() == 0:
				return f"{value}"
			if index.column() == 2:
				pass

		return value

	def sort(
		self,
		column: int,
		order: QtCore.Qt.SortOrder = QtCore.Qt.SortOrder.AscendingOrder,
	) -> None:
		if not self.base_query or column < 0 or column >= len(self.sort_columns):
			return

		direction = "ASC" if order == QtCore.Qt.SortOrder.AscendingOrder else "DESC"
		sort_column = self.sort_columns[column]
		self.setQuery(f"{self.base_query} ORDER BY {sort_column} {direction}")


class MainApp(QMainWindow):
	def __init__(self, args, dbconn=None, parent=None):
		super(MainApp, self).__init__(parent=parent)
		self.ui = Ui_main_window()
		self.ui.setupUi(self)

		self.con = dbconn
		self.args = args
		session = get_engine_session(self.args)
		self.session = session

		# Add debug output to check data
		try:
			count = self.session.query(TorqFile).count()
			logger.info(f"Database contains {count} TorqFile records")
		except Exception as e:
			logger.error(f"Database error: {e}")

		self.populate_torqfiles()
		self.create_entries_plot()
		self.create_speed_plot()

		# Connect signals
		self.ui.tableView.doubleClicked.connect(self.doubleclicked_table)
		self.trip_plot_view = QChartView()
		self.speed_plot_view = QChartView()
		self.ui.actionExit.triggered.connect(self.appexit)
		self.event_filter = KeyPressFilter(parent=self)
		self.installEventFilter(self.event_filter)

	def appexit(self):
		logger.debug(f'{self} exit')
		self.close()

	def populate_torqfiles(self):
		self.filemodel = CustomSqlModel()
		self.filemodel.base_query = 'select fileid,trip_start, sent_rows from torqfiles'
		self.filemodel.sort_columns = ["fileid", "trip_start", "sent_rows"]
		self.filemodel.setQuery(self.filemodel.base_query)
		self.filemodel.setHeaderData(0, Qt.Orientation.Horizontal, "fileid")
		self.filemodel.setHeaderData(1, Qt.Orientation.Horizontal, "trip_start")
		self.filemodel.setHeaderData(2, Qt.Orientation.Horizontal, "entries")
		self.ui.tableView.setModel(self.filemodel)
		self.ui.tableView.setSortingEnabled(True)
		self.ui.tableView.sortByColumn(0, Qt.SortOrder.AscendingOrder)
		self.ui.tableView.resizeColumnsToContents()

	def doubleclicked_table(self):
		# Get selected file ID
		index = self.ui.tableView.selectedIndexes()[0]
		row = index.row()
		fileid = self.filemodel.data(self.filemodel.index(row, 0))

		logger.debug(f"Double clicked on file ID: {fileid}")

		# Fetch data and ensure it's clean
		try:
			lat_lon_data = self.session.query(Torqlogs.latitude, Torqlogs.longitude).filter(Torqlogs.fileid == fileid).all()
		except Exception as e:
			logger.error(f"Error fetching lat/lon data: {e} {type(e)}")
			return
		lat_lon_df = pd.DataFrame(lat_lon_data).fillna(0)
		try:
			speed_data = self.session.query(
				Speeds.index,
				Speeds.speedgpskmh,
				Speeds.gpsspeedkmh,
				Speeds.speedobdkmh,
			).filter(Speeds.fileid == fileid).all()
		except Exception as e:
			logger.error(f"Error fetching speed data: {e} {type(e)}")
			return
		speed_df = pd.DataFrame(
			speed_data,
			columns=["id", "speedgpskmh", "gpsspeedkmh", "speedobdkmh"],
		).fillna(0)

		# Create chart series
		latlonscatter = QScatterSeries()
		speedgpskmh = QLineSeries()
		gpsspeedkmh = QLineSeries()
		speedobdkmh = QLineSeries()

		# Set up pens for different series
		pens = {
			'blue': QPen(Qt.GlobalColor.blue),
			'green': QPen(Qt.GlobalColor.green),
			'red': QPen(Qt.GlobalColor.red)
		}
		for pen in pens.values():
			pen.setWidth(1)

		speedgpskmh.setPen(pens['blue'])
		gpsspeedkmh.setPen(pens['green'])
		speedobdkmh.setPen(pens['red'])

		# Add lat/lon data points
		for row in lat_lon_df.itertuples():
			lat = _to_float(row.latitude)
			lon = _to_float(row.longitude)
			if lat is not None and lon is not None:
				latlonscatter.append(lat, lon)

		# Add speed data points
		for row in speed_df.itertuples():
			x = _to_float(row.id)
			s1 = _to_float(row.speedgpskmh)
			s2 = _to_float(row.gpsspeedkmh)
			s3 = _to_float(row.speedobdkmh)

			if x is not None and s1 is not None:
				speedgpskmh.append(x, s1)
			if x is not None and s2 is not None:
				gpsspeedkmh.append(x, s2)
			if x is not None and s3 is not None:
				speedobdkmh.append(x, s3)

		# Create and configure trip chart (lat/lon)
		if latlonscatter.count() == 0:
			logger.warning(f"No valid lat/lon data points for file ID {fileid}")
			return
		if latlonscatter.count() > 0:
			logger.debug(f"Creating trip chart with {latlonscatter.count()} lat/lon points for file ID {fileid}")
			# Create new chart
			trip_chart = QChart()
			trip_chart.addSeries(latlonscatter)

			# Calculate ranges for axes
			min_lat = min_lon = float('inf')
			max_lat = max_lon = float('-inf')

			for i in range(latlonscatter.count()):
				point = latlonscatter.at(i)
				min_lat = min(min_lat, point.x())
				max_lat = max(max_lat, point.x())
				min_lon = min(min_lon, point.y())
				max_lon = max(max_lon, point.y())

			# Ensure we have valid ranges
			if min_lat != float('inf') and max_lat != float('-inf'):
				# Add margins to ranges
				lat_margin = (max_lat - min_lat) * 0.1 if max_lat > min_lat else 0.0001
				lon_margin = (max_lon - min_lon) * 0.1 if max_lon > min_lon else 0.0001

				# Create axes
				axis_lat = PySide6.QtCharts.QValueAxis()
				axis_lat.setRange(min_lat - lat_margin, max_lat + lat_margin)
				axis_lat.setLabelFormat("%.6f")
				axis_lat.setTitleText("Latitude")

				axis_lon = PySide6.QtCharts.QValueAxis()
				axis_lon.setRange(min_lon - lon_margin, max_lon + lon_margin)
				axis_lon.setLabelFormat("%.6f")
				axis_lon.setTitleText("Longitude")

				# Add axes to chart
				trip_chart.addAxis(axis_lat, Qt.AlignmentFlag.AlignBottom)
				trip_chart.addAxis(axis_lon, Qt.AlignmentFlag.AlignLeft)

				# Attach series to axes
				latlonscatter.attachAxis(axis_lat)
				latlonscatter.attachAxis(axis_lon)

				# Set chart title and hide legend
				trip_chart.setTitle(f"Trip Map (ID: {fileid})")
				trip_chart.legend().hide()

				# Create chart view
				self.trip_plot_view = QChartView(trip_chart)

				# Clear previous layout content
				while self.ui.main_layout.count() > 0:
					item = self.ui.main_layout.takeAt(0)
					if item is None:
						continue

					w = item.widget()
					if w is not None:
						w.deleteLater()

				# Add chart view to layout
				# self.ui.main_layout.addWidget(self.trip_plot_view)
				# self.ui.trip_chart_layout.addWidget(self.trip_plot_view)
				self.ui.main_layout.addWidget(self.trip_plot_view)
				logger.info(f"Created trip map chart for file ID {fileid}")
			else:
				logger.warning(f"Invalid lat/lon ranges for file ID {fileid}")

	def create_start_stops_plot(self):
		# self.startstopmodel = QSqlQueryModel()
		# x = latitude y = longitude !

		data = np.array(self.session.execute(text('select latstart, lonstart from startpos')).all())
		scatter = QScatterSeries()
		[scatter.append(k[0],k[1]) for k in data]
		[scatter.append(k[2],k[3]) for k in data]
		self.speed_plot = QChart()
		self.speed_plot_view = QChartView(self.speed_plot)
		self.ui.main_layout.addWidget(self.speed_plot_view)
		# self.setLayout(self.ui.main_layout)

		self.speed_plot.addSeries(scatter)
		# scatter.setName('start/end')
		scatter.setMarkerSize(5)
		# self.start_stop_plot.createDefaultAxes()
		# self.start_stop_plot.setTitleFont(QFont('Arial', 10))
		self.speed_plot.setTitle('trip start/end')
		self.speed_plot.legend().hide()
		# self.start_stop_plot.axes()[0].setMax(self.start_stop_plot.axes()[0].max()+1)
		# self.start_stop_plot.axes()[1].setMax(self.start_stop_plot.axes()[1].max()+1)
		# self.start_stop_plot.axes()[0].setMin(self.start_stop_plot.axes()[0].min()-1)
		# self.start_stop_plot.axes()[1].setMin(self.start_stop_plot.axes()[1].min()-1)
		# self.ui.tableView.setModel(self.tripdist_series_model)
		# self.ui.tableView.resizeColumnsToContents()

	def create_speed_plot(self):
		speed_rows = self.session.query(
			Speeds.fileid,
			Speeds.gpsspeedkmh,
			Speeds.speedgpskmh,
			Speeds.speedobdkmh,
			Speeds.gpstime,
		).all()
		data = pd.DataFrame(
			speed_rows,
			columns=["fileid", "gpsspeedkmh", "speedgpskmh", "speedobdkmh", "gpstime"],
		).fillna(0)
		logger.debug(f"Fetched {len(data)} speed records for speed plot. speed_rows: {len(speed_rows)}")
		scatter = QScatterSeries()

		for k in data.itertuples():
			fileid = _to_float(getattr(k, "fileid", None))
			gpsspeed = _to_float(getattr(k, "gpsspeedkmh", None))  # fixed name (no backtick)

			if fileid is not None and gpsspeed is not None:
				scatter.append(fileid, gpsspeed)

			gpstime_val = getattr(k, "gpstime", None)
			if pd.notna(gpstime_val):
				try:
					ms = QtCore.QDateTime.fromString(str(gpstime_val)).toMSecsSinceEpoch()
					if ms > 0 and gpsspeed is not None:
						scatter.append(float(ms), gpsspeed)
				except (TypeError, ValueError):
					pass

		# Only create chart if we have valid data points
		if scatter.count() > 0:
			self.speed_plot = QChart()
			self.speed_plot.addSeries(scatter)
			self.speed_plot_view = QChartView(self.speed_plot)

			# Set explicit ranges instead of relying on auto-range
			min_x = min_y = float('inf')
			max_x = max_y = float('-inf')

			for i in range(scatter.count()):
				point = scatter.at(i)
				min_x = min(min_x, point.x())
				max_x = max(max_x, point.x())
				min_y = min(min_y, point.y())
				max_y = max(max_y, point.y())

			# Add small margin to ranges
			if min_x != float('inf') and max_x != float('-inf'):
				axis_x = PySide6.QtCharts.QValueAxis()
				axis_x.setRange(min_x * 0.9, max_x * 1.1 if max_x > 0 else max_x * 0.9)
				axis_x.setLabelFormat("%d")

				axis_y = PySide6.QtCharts.QValueAxis()
				axis_y.setRange(0, max_y * 1.1)  # Assuming speed is always positive
				axis_y.setLabelFormat("%d")

				self.speed_plot.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
				self.speed_plot.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
				scatter.attachAxis(axis_x)
				scatter.attachAxis(axis_y)

			self.speed_plot.legend().hide()
			self.ui.entrieslayout.addWidget(self.speed_plot_view)
			self.setLayout(self.ui.main_layout)
		else:
			logger.warning("No valid data points for speed plot")

	def create_entries_plot(self):
		self.fileentries_series = QLineSeries()
		data = self.session.query(TorqFile.fileid, TorqFile.sent_rows).all()

		# Early check for data
		if not data:
			logger.warning("No data available for entries plot")
			return

		# Create axes with safer initialization
		axis_x = PySide6.QtCharts.QValueAxis()
		axis_y = PySide6.QtCharts.QValueAxis()

		# Add points and calculate min/max values
		min_x = min_y = float('inf')
		max_x = max_y = float('-inf')

		for k in data:
			if pd.notna(k[0]) and pd.notna(k[1]):
				fileid = float(k[0])
				sent_rows = float(k[1])
				self.fileentries_series.append(fileid, sent_rows)

				# Update min/max
				min_x = min(min_x, fileid)
				max_x = max(max_x, fileid)
				min_y = min(min_y, sent_rows)
				max_y = max(max_y, sent_rows)

		# Only continue if we have valid data points
		if self.fileentries_series.count() > 0 and min_x != float('inf') and max_x != float('-inf'):
			# Add margins to ranges
			x_margin = (max_x - min_x) * 0.1 if max_x > min_x else 1
			y_margin = (max_y - min_y) * 0.1 if max_y > min_y else 1

			# Configure axes
			axis_x.setRange(min_x - x_margin, max_x + x_margin)
			axis_y.setRange(max(0, min_y - y_margin), max_y + y_margin)

			# Set axis properties
			font = QFont('Arial', 8)
			font.setPixelSize(8)

			axis_x.setTickCount(10)
			axis_x.setTitleFont(font)
			axis_x.setLabelsFont(font)
			axis_x.setTitleText('ID')
			axis_x.setLabelFormat("%d")

			axis_y.setTickCount(10)
			axis_y.setTitleFont(font)
			axis_y.setLabelsFont(font)
			axis_y.setTitleText('Count')
			axis_y.setLabelFormat("%d")

			# Create chart and view
			self.entries_chart = QChart()
			self.entries_chart.addSeries(self.fileentries_series)
			self.entries_chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
			self.entries_chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)

			# Attach series to axes
			self.fileentries_series.attachAxis(axis_x)
			self.fileentries_series.attachAxis(axis_y)

			# Configure chart
			self.entries_chart.setTitle("File Entries")
			self.entries_chart.legend().hide()

			# Create and add view to layout
			self.entries_view = QChartView(self.entries_chart)
			self.ui.entrieslayout.addWidget(self.entries_view)

			logger.info(f"Created entries plot with {self.fileentries_series.count()} data points")
		else:
			logger.warning("Invalid data range for entries plot")

def create_connection(args):
	con = None
	if args.dbmode == 'sqlite':
		con = QtSql.QSqlDatabase.addDatabase('QSQLITE')
		con.setDatabaseName(args.dbfile)
	elif args.dbmode == 'mariadb':
		con = QtSql.QSqlDatabase.addDatabase('QMARIADB')
		con.setDatabaseName(args.dbname)
		con.setHostName(args.dbhost)
		con.setUserName(args.dbuser)
		con.setPassword(args.dbpass)
	elif args.dbmode == 'psql':
		con = QtSql.QSqlDatabase.addDatabase('QPSQL')
		con.setDatabaseName(args.dbname)
		con.setHostName(args.dbhost)
		con.setUserName(args.dbuser)
		con.setPassword(args.dbpass)
	if con is None or not con.open():
		# QMessageBox.critical(None, "Cannot open database",			con.lastError().text())
		return False
	return con

# df = pd.DataFrame([k.__dict__ for k in trips])
if __name__ == '__main__':
	args = get_args(appname='testgui')
	session = get_engine_session(args)
	app = QApplication(sys.argv)
	c = create_connection(args)
	w = MainApp(args=args, dbconn=c)
	w.show()
	sys.exit(app.exec())
