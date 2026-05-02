import pandas as pd
from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex, QPersistentModelIndex
from loguru import logger

class PositionTableModel(QAbstractTableModel):
	def __init__(self, source_df: pd.DataFrame):
		super().__init__()
		self._source = source_df
		self._columns = ['pos_type', 'pos_id', 'latitude', 'longitude', 'count', 'label']
		self._view_order = list(source_df.index)

	def rowCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
		return len(self._view_order)

	def columnCount(self, parent: QModelIndex | QPersistentModelIndex = QModelIndex()) -> int:
		return len(self._columns)

	def data(self, index: QModelIndex | QPersistentModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> object:
		if not index.isValid() or role != Qt.ItemDataRole.DisplayRole:
			return None
		source_row = self._view_order[index.row()]
		col = self._columns[index.column()]
		value = self._source.at[source_row, col]
		return str(value)

	def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> object:
		if role != Qt.ItemDataRole.DisplayRole:
			return None
		if orientation == Qt.Orientation.Horizontal:
			return self._columns[section]
		return str(section)

	def sort(self, column: int, order: Qt.SortOrder = Qt.SortOrder.AscendingOrder) -> None:
		if not self._view_order:
			logger.warning(f'{self} sort called with empty view order. col={column} order={order}')
			return
		col = self._columns[column]
		ascending = order == Qt.SortOrder.AscendingOrder
		self.layoutAboutToBeChanged.emit()
		tmp = self._source.loc[self._view_order, [col]].copy()
		tmp["_src"] = self._view_order
		if col == "label":
			tmp["_sort_key"] = tmp[col].fillna("").astype(str).str.casefold()
			sort_col = "_sort_key"
		elif col in ('pos_id', 'latitude', 'longitude', 'count'):
			tmp["_sort_key"] = pd.to_numeric(tmp[col], errors="coerce")
			sort_col = "_sort_key"
		else:
			sort_col = col
		tmp.sort_values(by=sort_col, ascending=ascending, inplace=True, kind="mergesort")
		self._view_order = [int(x) for x in tmp["_src"].tolist()]
		self.layoutChanged.emit()

	def source_row_for_view_row(self, view_row: int) -> int | None:
		if view_row < 0 or view_row >= len(self._view_order):
			return None
		return int(self._view_order[view_row])

	def view_row_for_source_row(self, source_row: int) -> int | None:
		try:
			return self._view_order.index(source_row)
		except ValueError:
			return None

	def source_rows(self) -> list[int]:
		return [int(x) for x in self._view_order]

	def set_view_order(self, source_rows: list[int]):
		self.layoutAboutToBeChanged.emit()
		valid = set(int(idx) for idx in self._source.index.tolist())
		self._view_order = [int(idx) for idx in source_rows if int(idx) in valid]
		self.layoutChanged.emit()
