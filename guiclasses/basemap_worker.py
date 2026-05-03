import socket
from typing import Any, cast

import contextily as ctx
from loguru import logger
from PySide6.QtCore import QObject, Signal


class BasemapWorker(QObject):
	finished = Signal(object, object, int)
	error = Signal(str, int)

	def __init__(self, bounds, zoom: int, request_id: int):
		super().__init__()
		self.bounds = bounds
		self.zoom = zoom
		self.request_id = request_id
		logger.debug(f"BasemapWorker initialized with bounds={bounds}, zoom={zoom}, request_id={request_id}")

	def run(self):
		try:
			west, east, south, north = self.bounds
			source = None
			try:
				source = cast(Any, ctx.providers).OpenStreetMap.Mapnik
			except Exception as e:
				logger.warning(f"Basemap provider resolution failed: {e} ({type(e)})")
				source = None
			# Apply a per-socket timeout so tile fetches cannot block indefinitely.
			# n_connections=4 fetches tiles in parallel; max_retries=1 allows one retry.
			kwargs: dict[str, Any] = {
				"zoom": cast(Any, self.zoom),
				"wait": 0.5,
				"max_retries": 1,
				# Keep tile fetch single-threaded to avoid joblib/loky callbacks during app shutdown.
				"n_connections": 1,
			}
			if source is not None:
				kwargs["source"] = source
			old_socket_timeout = socket.getdefaulttimeout()
			socket.setdefaulttimeout(5.0)
			try:
				try:
					img, ext = ctx.bounds2img(west, south, east, north, **kwargs)
				except TypeError as e:
					# Older contextily versions may not accept all timeout/retry kwargs.
					logger.warning(f"Basemap fetch failed with TypeError: {e} ({type(e)})")
					fallback_kwargs: dict[str, Any] = {"zoom": cast(Any, self.zoom)}
					if source is not None:
						fallback_kwargs["source"] = source
					img, ext = ctx.bounds2img(west, south, east, north, **fallback_kwargs)
			finally:
				socket.setdefaulttimeout(old_socket_timeout)
			self.finished.emit(img, ext, self.request_id)
			logger.debug(f"BasemapWorker finished fetching basemap for request_id={self.request_id}")
		except Exception as e:
			logger.error(f"BasemapWorker error for request_id={self.request_id}: {e} ({type(e)})")
			self.error.emit(f'{e} {type(e)}', self.request_id)
