from PySide6.QtWidgets import QMainWindow, QTabWidget

from .main_window import MainWindow
from .position_manager_window import PositionManagerWindow
from .start_end_window import StartEndWindow
from .trip_stats_window import TripStatsWindow


class AppTabsWindow(QMainWindow):
	def __init__(self, args, engine):
		super().__init__()
		self.args = args
		self.engine = engine
		self.setWindowTitle("TorqFiles Workspace")
		self.resize(1440, 900)

		self.tabs = QTabWidget()
		self.main_window_tab = MainWindow(args, engine)
		self.pos_manager_tab = PositionManagerWindow(args, engine, self)
		self.start_end_tab = StartEndWindow(args, engine, self)
		self.trip_stats_tab = TripStatsWindow(args, engine, self)

		self.tabs.addTab(self.main_window_tab, "Main")
		self.tabs.addTab(self.pos_manager_tab, "Positions")
		self.tabs.addTab(self.start_end_tab, "Start/End")
		self.tabs.addTab(self.trip_stats_tab, "Trip Stats")
		self.setCentralWidget(self.tabs)
