import pandas as pd
from sqlalchemy import create_engine
from PySide6.QtCore import QObject, Signal
from loguru import logger

class PositionLoadWorker(QObject):
    finished = Signal(object)
    error = Signal(str)

    def __init__(self, engine_url: str, pos_type: str | None = None, trip_id: int | None = None):
        super().__init__()
        self.engine_url = engine_url
        self.pos_type = pos_type
        self.trip_id = trip_id

    def run(self):
        engine = None
        try:
            engine = create_engine(self.engine_url)

            def _load_for_type(pos_type: str) -> pd.DataFrame:
                if pos_type == 'start':
                    table = 'startpos'
                    id_col = 'startid'
                    lat_col = 'latstart'
                    lon_col = 'lonstart'
                else:
                    table = 'endpos'
                    id_col = 'endid'
                    lat_col = 'latend'
                    lon_col = 'lonend'
                query = (
                    f"SELECT {id_col} AS pos_id, "
                    f"{lat_col} AS latitude, "
                    f"{lon_col} AS longitude, "
                    f"count, label FROM {table}"
                )
                try:
                    df_part = pd.read_sql(query, engine)
                    df_part.insert(0, "pos_type", pos_type)
                except Exception as e:
                    logger.error(f"Failed to load {pos_type} positions with query '{query}': {e} ({type(e)})")
                    df_part = pd.DataFrame(columns=['pos_type', 'pos_id', 'latitude', 'longitude', 'count', 'label'])
                return df_part

            if self.pos_type in ("start", "end"):
                df = _load_for_type(self.pos_type)
            else:
                df_start = _load_for_type("start")
                df_end = _load_for_type("end")
                df = pd.concat([df_start, df_end], ignore_index=True)

            self.finished.emit(df)
        except Exception as e:
            pos_type = self.pos_type if self.pos_type in ("start", "end") else "start/end"
            self.error.emit(f"Failed to load {pos_type} positions: {e} ({type(e)})")
        finally:
            if engine is not None:
                engine.dispose()
