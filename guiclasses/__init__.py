from importlib import import_module

_LAZY_EXPORTS = {
	"FoliumMapView": "guiclasses.map_canvas",
	"MapCanvas": "guiclasses.map_canvas",
	"TimeSeriesCanvas": "guiclasses.time_series_canvas",
	"TripListWorker": "guiclasses.trip_list_worker",
	"PositionTableModel": "guiclasses.position_table_model",
	"PositionLoadWorker": "guiclasses.position_load_worker",
	"PositionManagerWindow": "guiclasses.position_manager_window",
	"MainWindow": "guiclasses.main_window",
	"StartEndWindow": "guiclasses.start_end_window",
	"AppTabsWindow": "guiclasses.app_tabs_window",
	"PandasModel": "guiclasses.pandas_model",
}


def __getattr__(name: str):
	module_name = _LAZY_EXPORTS.get(name)
	if module_name is None:
		raise AttributeError(f"module 'guiclasses' has no attribute '{name}'")
	module = import_module(module_name)
	value = getattr(module, name)
	globals()[name] = value
	return value

__all__ = [
	"FoliumMapView",
	"MapCanvas",
	"TimeSeriesCanvas",
	"TripListWorker",
	"PositionTableModel",
	"PositionLoadWorker",
	"PositionManagerWindow",
	"MainWindow",
	"StartEndWindow",
	"AppTabsWindow",
	"PandasModel",
]
