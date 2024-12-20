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
import PySide6.QtCharts
# from PySide6.QtWidgets import (QApplication, QSizePolicy, QWidget)
from PySide6.QtWidgets import QApplication, QMainWindow
from sqlalchemy import text
import numpy as np

from datamodels import Torqlogs, TorqFile
from ui_untitled import Ui_MainWindow
from utils import get_engine_session
from converter import get_args
# x = latitude y = longitude !

class Mymodel(QAbstractTableModel):
	pass

mymodel = Mymodel()

class KeyPressFilter(QObject):
	def eventFilter(self, widget, event):
		if event.type() == QEvent.KeyPress:
			text = event.text()
			print(f'Key {text} {event=}')
			if event.modifiers():
				text = event.keyCombination().key().name   # .decode(encoding="utf-8")
				print(f'event.modifierskeyboard {event.keyCombination().key().name} {event.keyCombination().key()} {event.keyCombination()}')
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
		self.ui = Ui_MainWindow()
		self.ui.setupUi(self)
		self.con = dbconn
		self.args = args
		engine, session = get_engine_session(self.args)
		self.session = session
		self.populate_torqfiles()
		self.create_entries_plot()
		self.create_speed_plot()
		self.create_start_stops_plot()
		self.ui.tableView.doubleClicked.connect(self.doubleClicked_table)
		self.trip_plot_view = QChartView()
		self.speed_plot_view = QChartView()
		self.ui.actionExit.triggered.connect(self.appexit)
		self.eventFilter = KeyPressFilter(parent=self)
		self.installEventFilter(self.eventFilter)

	def appexit(self):
		print(f'{self} exit')
		self.close()

	def populate_torqfiles(self):
		self.filemodel = QSqlQueryModel()
		self.filemodel.setQuery('select fileid,trip_start, sent_rows from torqfiles ')
		self.filemodel.setHeaderData(0, QtCore.Qt.Horizontal, "fileid")
		self.filemodel.setHeaderData(1, QtCore.Qt.Horizontal, "trip_start")
		self.filemodel.setHeaderData(2, QtCore.Qt.Horizontal, "entries")
		self.ui.tableView.setModel(self.filemodel)
		self.ui.tableView.resizeColumnsToContents()

	def doubleClicked_table(self):
		font = QFont('Ariel', 8)
		font.setPixelSize(8)
		index = self.ui.tableView.selectedIndexes()[0]
		row = self.ui.tableView.selectedIndexes()[0].row()
		celldata = self.ui.tableView.model().data(index)
		fileid = row+1
		lat_lon_data = self.session.query(Torqlogs.latitude, Torqlogs.longitude).filter(Torqlogs.fileid == fileid).all()
		lat_lon_data = pd.DataFrame(lat_lon_data).fillna(0)
		speed_data = self.session.query(Torqlogs.id, Torqlogs.speedgpskmh, Torqlogs.gpsspeedkmh, Torqlogs.speedobdkmh).filter(Torqlogs.fileid == fileid).all()
		speed_data = pd.DataFrame(speed_data).fillna(0)
		logger.debug(f'{fileid=} row={row}  data={celldata} lld={len(lat_lon_data)} spd={len(speed_data)}')
		font = QFont('Ariel', 8)
		font.setPixelSize(8)
		axis_x = PySide6.QtCharts.QValueAxis(titleFont=font)
		axis_y = PySide6.QtCharts.QValueAxis(titleFont=font)
		axis_y.setTickCount(10)
		axis_y.setTitleText('y')
		axis_y.setLabelFormat("%1i")
		axis_y.applyNiceNumbers()

		axis_x.setTickCount(10)
		axis_x.setTitleText('x')
		axis_x.setLabelFormat("%1i")
		axis_x.applyNiceNumbers()

		latlonscatter = QScatterSeries()

		speedgpskmh = QLineSeries()
		pen = QPen()
		pen.setWidth(1)
		pen.setColor('blue')
		speedgpskmh.setPen(pen)
		gpsspeedkmh = QLineSeries()
		pen.setColor('green')
		gpsspeedkmh.setPen(pen)
		speedobdkmh = QLineSeries()
		pen.setColor('red')
		speedobdkmh.setPen(pen)
		# [latlonscatter.append(lat_lon_data[k].values[0],lat_lon_data[k].values[1]) for k in lat_lon_data]
		[latlonscatter.append(k.latitude,k.longitude) for k in lat_lon_data.itertuples()]

		try:
			for k in speed_data.itertuples():
				[speedgpskmh.append(k.id,k.speedgpskmh) for k in k[speed_data].itertuples()]
				[gpsspeedkmh.append(k.id,k.gpsspeedkmh) for k in k[speed_data].itertuples()]
				[speedobdkmh.append(k.id,k.speedobdkmh) for k in k[speed_data].itertuples()]
		except TypeError as e:
			logger.error(f'{e}')
			# [speedgpskmh.append(speed_data[k].values[0],speed_data[k].values[1]) for k in speed_data]
			# [speedgpskmh.append(speed_data[k].values[0],speed_data[k].values[2]) for k in speed_data]
			# [speedgpskmh.append(speed_data[k].values[0],speed_data[k].values[3]) for k in speed_data]
		try:
			self.ui.triplayout.removeWidget(self.trip_plot_view)
			self.ui.speedlayout.removeWidget(self.speed_plot_view)
		except AttributeError as e:
			logger.warning(f'{e}')
		self.trip_plot = QChart()
		self.speed_plot = QChart()
		self.trip_plot.addSeries(latlonscatter)
		self.trip_plot.setTitle('lat/lon')
		# self.trip_plot.createDefaultAxes()
		axis_x = PySide6.QtCharts.QValueAxis(titleFont=font, labelsFont=font)
		axis_y = PySide6.QtCharts.QValueAxis(titleFont=font, labelsFont=font)
		axis_x.setLabelFormat("%.2f")
		axis_x.setTitleText('lat')
		axis_y.setTitleText('lon')
		axis_y.setLabelFormat("%.2f")
		self.trip_plot.addAxis(axis_x, Qt.AlignBottom)
		self.trip_plot.addAxis(axis_y, Qt.AlignLeft)
		[k.attachAxis(axis_x) for k in self.trip_plot.series()]
		[k.attachAxis(axis_y) for k in self.trip_plot.series()]

		self.speed_plot.addSeries(speedgpskmh)
		self.speed_plot.addSeries(gpsspeedkmh)
		self.speed_plot.addSeries(speedobdkmh)
		# self.speed_plot.createDefaultAxes()
		axis_x = PySide6.QtCharts.QValueAxis(titleFont=font, labelsFont=font)
		axis_y = PySide6.QtCharts.QValueAxis(titleFont=font, labelsFont=font)
		axis_x.setLabelFormat("%d")
		axis_x.setTitleText('id')
		axis_y.setLabelFormat("%d")
		axis_y.setTitleText('speed')
		self.speed_plot.addAxis(axis_x, Qt.AlignBottom)
		self.speed_plot.addAxis(axis_y, Qt.AlignLeft)
		[k.attachAxis(axis_x) for k in self.speed_plot.series()]
		[k.attachAxis(axis_y) for k in self.speed_plot.series()]

		# self.speed_plot.legend().hide()
		# self.trip_plot.legend().hide()
		self.trip_plot_view = QChartView(self.trip_plot)
		self.speed_plot_view = QChartView(self.speed_plot)
		# scatter.setModel(self.tripplotmodel)
		self.ui.triplayout.addWidget(self.trip_plot_view)
		self.ui.speedlayout.addWidget(self.speed_plot_view)
		self.setLayout(self.ui.triplayout)
		latlonscatter.setMarkerSize(5)
		speedgpskmh.setMarkerSize(5)

	def create_start_stops_plot(self):
		# self.startstopmodel = QSqlQueryModel()
		# x = latitude y = longitude !

		data = np.array(session.execute(text('select latmin,lonmin,latmax,lonmax from startends')).all())
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
		# data = np.array(session.execute(text('select * from speeds')).all())
		data = pd.DataFrame(session.execute(text('select * from speeds')).all())
		# data = np.array(df.fillna(0))
		scatter = QScatterSeries()
		# [scatter.append(k.gpstime,k.speed) for k in data.itertuples()]
		for k in data.itertuples():
			kgpstime = None
			kgpstime_ = str(k.gpstime)  # .toMSecsSinceEpoch()
			try:
				kgpstime = QtCore.QDateTime.fromString(kgpstime_).toMSecsSinceEpoch()
				scatter.append(k.fileid,k.gpsspeedkmh)
				scatter.append(kgpstime,k.gpsspeedkmh)
			except TypeError as e:
				logger.warning(f'{e} {kgpstime=} {k=} {k.gpstime=}')
		# [scatter.append(k.fileid,k.speed) for k in data.itertuples()]
		# [scatter.append(k[3],k[2]) for k in data ]
		# [scatter.append(k[0],k[2]) for k in data ]
		scatter.setMarkerSize(5)
		# [scatter.append(k[0]) for k in (data.fileid, data.speed) ]
		# [scatter.append(k[0],k[1]) for k in (data.fileid, data.speed) ]
		# [scatter.append(k[0],str(k[2])) for k in data if k[0] and k[1] and k[2]]
		self.speed_plot = QChart()
		self.speed_plot.addSeries(scatter)
		self.speed_plot_view = QChartView(self.speed_plot)
		self.ui.main_layout.addWidget(self.speed_plot_view)
		# self.setLayout(self.ui.main_layout)
		# scatter.setName('start/end')
		# self.start_stop_plot.createDefaultAxes()
		# self.start_stop_plot.setTitleFont(QFont('Arial', 10))
		# self.start_stop_plot.setTitle('trip start/end')
		self.speed_plot.legend().hide()
		self.ui.entrieslayout.addWidget(self.speed_plot_view)
		self.setLayout(self.ui.main_layout)
		# self.start_stop_plot.axes()[0].setMax(self.start_stop_plot.axes()[0].max()+1)
		# self.start_stop_plot.axes()[1].setMax(self.start_stop_plot.axes()[1].max()+1)
		# self.start_stop_plot.axes()[0].setMin(self.start_stop_plot.axes()[0].min()-1)
		# self.start_stop_plot.axes()[1].setMin(self.start_stop_plot.axes()[1].min()-1)
		# self.ui.tableView.setModel(self.tripdist_series_model)
		# self.ui.tableView.resizeColumnsToContents()

	def xcreate_speed_plot(self):
		# lat_lon_data = self.session.query(Torqlogs.latitude, Torqlogs.longitude).filter(Torqlogs.fileid==fileid).all()
		self.speedmodel = QSqlQueryModel()
		self.speedmodel.setQuery('select * from speeds')
		self.speedmodel.setHeaderData(0, QtCore.Qt.Horizontal, "fileid")
		self.speedmodel.setHeaderData(1, QtCore.Qt.Horizontal, "speed")
		self.speedmodel.setHeaderData(2, QtCore.Qt.Horizontal, "gpstime")
		self.speed_series = QLineSeries()

	def create_entries_plot(self):
		self.fileentries_series = QLineSeries()
		data = self.session.query(TorqFile.fileid,TorqFile.sent_rows).all()
		axis_x = PySide6.QtCharts.QValueAxis()
		axis_y = PySide6.QtCharts.QValueAxis()
		font = QFont('Ariel', 8)
		font.setPixelSize(8)
		axis_y.setTickCount(10)
		axis_y.setTitleFont(font)
		axis_y.setLabelsFont(font)
		axis_y.setTitleText('count')
		axis_y.setLabelFormat("%1i")
		axis_y.applyNiceNumbers()

		font = QFont('Ariel', 8)
		font.setPixelSize(8)
		axis_x.setTickCount(10)
		axis_x.setTitleFont(font)
		axis_x.setLabelsFont(font)
		axis_x.setTitleText('id')
		axis_x.setLabelFormat("%2d")
		axis_x.applyNiceNumbers()
		for k in data:
			self.fileentries_series.append(k[0],k[1])
		self.entries_chart = QChart()

		self.entries_view = QChartView(self.entries_chart)
		self.entries_chart.addSeries(self.fileentries_series)
		self.entries_chart.addAxis(axis_x, Qt.AlignBottom)
		self.entries_chart.addAxis(axis_y, Qt.AlignLeft)
		[k.attachAxis(axis_x) for k in self.entries_chart.series()]
		[k.attachAxis(axis_y) for k in self.entries_chart.series()]
		self.entries_chart.legend().hide()
		self.ui.entrieslayout.addWidget(self.entries_view)
		self.setLayout(self.ui.main_layout)

def createConnection(args):
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
	c = createConnection(args)
	w = MainApp(args=args, dbconn=c)
	w.show()
	sys.exit(app.exec())
