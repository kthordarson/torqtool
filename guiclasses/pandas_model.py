from loguru import logger
from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex, QPersistentModelIndex


class PandasModel(QAbstractTableModel):
	"""Minimal Qt model for pandas DataFrame for QTableView."""
	def __init__(
		self,
		data,
		display_columns: list[str] | None = None,
		sort_overrides: dict[str, str] | None = None,
	):
		super().__init__()
		self._data = data
		self._display_columns = list(display_columns) if display_columns else list(self._data.columns)
		self._sort_overrides = dict(sort_overrides) if sort_overrides else {}
		logger.debug(f"PandasModel initialized with {self._data.shape[0]} rows and {self._data.shape[1]} columns")

	def sort(self, column: int, order: Qt.SortOrder = Qt.SortOrder.AscendingOrder) -> None:
		if column < 0 or column >= len(self._display_columns):
			return
		display_col = self._display_columns[column]
		colname = self._sort_overrides.get(display_col, display_col)
		if colname not in self._data.columns:
			return
		self.layoutAboutToBeChanged.emit()
		self._data.sort_values(by=colname, ascending=(order == Qt.SortOrder.AscendingOrder), inplace=True)
		self._data.reset_index(inplace=True)
		self._data.set_index(self._data.columns[0], inplace=True)
		self.layoutChanged.emit()

	def rowCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
		return self._data.shape[0]

	def columnCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
		return len(self._display_columns)

	def data(self, index: QModelIndex | QPersistentModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> object:
		if role == Qt.ItemDataRole.DisplayRole:
			colname = self._display_columns[index.column()]
			return str(self._data.iloc[index.row()][colname])
		return None

	def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> object:
		if role == Qt.ItemDataRole.DisplayRole:
			if orientation == Qt.Orientation.Horizontal:
				return self._display_columns[section]
			else:
				return str(section)
		return None
