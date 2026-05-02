import matplotlib.pyplot as plt
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas


class TimeSeriesCanvas(FigureCanvas):
	def __init__(self, parent=None):
		fig, self.ax = plt.subplots(figsize=(6, 4))
		fig.subplots_adjust(bottom=0.22, top=0.90, left=0.13, right=0.97)
		super().__init__(fig)
		self.setParent(parent)
