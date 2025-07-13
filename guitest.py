#!/usr/bin/python3
import sys
import pandas as pd
import PySide6
from loguru import logger
from PySide6 import QtCore, QtSql, QtGui
from PySide6.QtCharts import QChart, QChartView, QLineSeries, QScatterSeries
from PySide6.QtCore import QAbstractTableModel, Qt, QObject, QEvent
from PySide6.QtGui import QFont, QPen
from PySide6.QtSql import QSqlQueryModel
from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget
import PySide6.QtCharts
import numpy as np
from sqlalchemy import text
from datamodels import Torqlogs, TorqFile
from ui_untitled import UiMainWindow
from utils import get_engine_session
from converter import get_args
# x = latitude y = longitude !

class Mymodel(QAbstractTableModel):
	pass

mymodel = Mymodel()

class KeyPressFilter(QObject):
	def event_filter(self, widget, event):
		if event.type() == QEvent.KeyPress:
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
		self.setHeaderData(1, QtCore.Qt.Horizontal, "latitude")
		self.setHeaderData(2, QtCore.Qt.Horizontal, "longitude")


class Torqfilemodel(QtSql.QSqlQueryModel):
	def __init__(self):
		super().__init__()
		self.setQuery('select fileid,trip_start,sent_rows from torqfiles ')
		self.setHeaderData(0, QtCore.Qt.Horizontal, "fileid")
		self.setHeaderData(1, QtCore.Qt.Horizontal, "trip_start")
		self.setHeaderData(2, QtCore.Qt.Horizontal, "entries")

class CustomSqlModel(QtSql.QSqlQueryModel):
	def data(self, index, role):
		value = super(CustomSqlModel, self).data(index, role)
		if value is not None and role == QtCore.Qt.DisplayRole:
			if index.column() == 0:
				return '#%d' % value
			elif index.column() == 2:
				return value  # .upper()
		if role == QtCore.Qt.ForegroundRole and index.column() == 1:
			return QtGui.QColor(QtCore.Qt.blue)
		return value


class MainApp(QMainWindow):
	def __init__(self, args=None, dbconn=None, parent=None):
		super(MainApp, self).__init__(parent=parent)
		self.ui = UiMainWindow()
		self.ui.setup_ui(self)

		# Ensure we have proper layouts for charts
		if not hasattr(self.ui, 'main_layout'):
			self.ui.main_layout = QVBoxLayout()
			self.ui.centralwidget.setLayout(self.ui.main_layout)

		# Layout for trip map
		self.ui.trip_chart_widget = QWidget()
		self.ui.trip_chart_layout = QVBoxLayout()
		self.ui.trip_chart_widget.setLayout(self.ui.trip_chart_layout)

		if not hasattr(self.ui, 'entrieslayout'):
			self.ui.entrieslayout = QVBoxLayout()
			# Create a widget for this layout
			entries_widget = QWidget()
			entries_widget.setLayout(self.ui.entrieslayout)
			self.ui.main_layout.addWidget(entries_widget)

			# Add widgets to main layout in the order you want them displayed
			self.ui.main_layout.addWidget(self.ui.trip_chart_widget)  # Trip chart at top
			self.ui.main_layout.addWidget(entries_widget)            # Entries chart below

		self.con = dbconn
		self.args = args
		engine, session = get_engine_session(self.args)
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
		self.filemodel = QSqlQueryModel()
		self.filemodel.setQuery('select fileid,trip_start, sent_rows from torqfiles ')
		self.filemodel.setHeaderData(0, QtCore.Qt.Horizontal, "fileid")
		self.filemodel.setHeaderData(1, QtCore.Qt.Horizontal, "trip_start")
		self.filemodel.setHeaderData(2, QtCore.Qt.Horizontal, "entries")
		self.ui.tableView.setModel(self.filemodel)
		self.ui.tableView.resizeColumnsToContents()

	def doubleclicked_table(self):
		# Get selected file ID
		index = self.ui.tableView.selectedIndexes()[0]
		row = index.row()
		fileid = self.filemodel.data(self.filemodel.index(row, 0))

		logger.debug(f"Double clicked on file ID: {fileid}")

		# Fetch data and ensure it's clean
		lat_lon_data = self.session.query(Torqlogs.latitude, Torqlogs.longitude).filter(Torqlogs.fileid == fileid).all()
		lat_lon_df = pd.DataFrame(lat_lon_data).fillna(0)

		speed_data = self.session.query(Torqlogs.id, Torqlogs.speedgpskmh, Torqlogs.gpsspeedkmh, Torqlogs.speedobdkmh).filter(Torqlogs.fileid == fileid).all()
		speed_df = pd.DataFrame(speed_data).fillna(0)

		# Create chart series
		latlonscatter = QScatterSeries()
		speedgpskmh = QLineSeries()
		gpsspeedkmh = QLineSeries()
		speedobdkmh = QLineSeries()

		# Set up pens for different series
		pens = {
			'blue': QPen(Qt.blue),
			'green': QPen(Qt.green),
			'red': QPen(Qt.red)
		}
		for pen in pens.values():
			pen.setWidth(1)

		speedgpskmh.setPen(pens['blue'])
		gpsspeedkmh.setPen(pens['green'])
		speedobdkmh.setPen(pens['red'])

		# Add lat/lon data points
		for row in lat_lon_df.itertuples():
			if pd.notna(row.latitude) and pd.notna(row.longitude):
				latlonscatter.append(float(row.latitude), float(row.longitude))

		# Add speed data points
		for row in speed_df.itertuples():
			if pd.notna(row.id) and pd.notna(row.speedgpskmh):
				speedgpskmh.append(float(row.id), float(row.speedgpskmh))
			if pd.notna(row.id) and pd.notna(row.gpsspeedkmh):
				gpsspeedkmh.append(float(row.id), float(row.gpsspeedkmh))
			if pd.notna(row.id) and pd.notna(row.speedobdkmh):
				speedobdkmh.append(float(row.id), float(row.speedobdkmh))

		# Create and configure trip chart (lat/lon)
		if latlonscatter.count() > 0:
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
				trip_chart.addAxis(axis_lat, Qt.AlignBottom)
				trip_chart.addAxis(axis_lon, Qt.AlignLeft)

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
					if item.widget():
						item.widget().deleteLater()

				# Add chart view to layout
				# self.ui.main_layout.addWidget(self.trip_plot_view)
				self.ui.trip_chart_layout.addWidget(self.trip_plot_view)
				logger.info(f"Created trip map chart for file ID {fileid}")
			else:
				logger.warning(f"Invalid lat/lon ranges for file ID {fileid}")
		else:
			logger.warning(f"No valid lat/lon data points for file ID {fileid}")

	def create_start_stops_plot(self):
		# self.startstopmodel = QSqlQueryModel()
		# x = latitude y = longitude !

		data = np.array(session.execute(text('select latstart, lonstart from startpos')).all())
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
		# Fetch data and ensure numeric columns are filled with zeros for NaN values
		data = pd.DataFrame(session.execute(text('select * from speeds')).all())
		data = data.fillna(0)  # Fill NaN values with zeros

		scatter = QScatterSeries()

		# Check for valid data before plotting
		for k in data.itertuples():
			# Only add points with valid data
			if pd.notna(k.fileid) and pd.notna(k.gpsspeedkmh):
				scatter.append(float(k.fileid), float(k.gpsspeedkmh))

			# Handle timestamp conversion separately
			if pd.notna(k.gpstime):
				try:
					kgpstime_ = str(k.gpstime)
					kgpstime = QtCore.QDateTime.fromString(kgpstime_).toMSecsSinceEpoch()
					if kgpstime > 0 and pd.notna(k.gpsspeedkmh):  # Ensure valid timestamp and speed
						scatter.append(float(kgpstime), float(k.gpsspeedkmh))
				except (TypeError, ValueError) as e:
					logger.warning(f'Time conversion error: {e} for {k.gpstime}')

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

				self.speed_plot.addAxis(axis_x, Qt.AlignBottom)
				self.speed_plot.addAxis(axis_y, Qt.AlignLeft)
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
			self.entries_chart.addAxis(axis_x, Qt.AlignBottom)
			self.entries_chart.addAxis(axis_y, Qt.AlignLeft)

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
	if not con.open():
		# QMessageBox.critical(None, "Cannot open database",			con.lastError().text())
		return False
	return con

# df = pd.DataFrame([k.__dict__ for k in trips])
if __name__ == '__main__':
	args = get_args(appname='testgui')
	engine, session = get_engine_session(args)
	app = QApplication(sys.argv)
	c = create_connection(args)
	w = MainApp(args=args, dbconn=c)
	w.show()
	sys.exit(app.exec())
