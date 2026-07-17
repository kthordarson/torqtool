import sys
import hashlib
from datetime import datetime
from loguru import logger
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Text,
    text,
    String,
    LargeBinary,
    UniqueConstraint,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


def stable_fileid_from_csvhash(csvhash: str) -> int:
    """
    Generate a deterministic signed 31-bit integer id from file hash.
    Using 31-bit keeps compatibility with existing INTEGER columns.
    """
    digest = hashlib.sha256(csvhash.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF


class Filestats(Base):
    __tablename__ = "filestats"
    index: Mapped[int] = mapped_column(primary_key=True)
    fileid: Mapped[int] = mapped_column(ForeignKey("torqfiles.fileid"))
    column_name = Column("column_name", Text)
    nulls = Column("nulls", Integer, default=0, unique=False)
    nullratio = Column("nullratio", Float, default=0, unique=False)


class Speeds(Base):
    __tablename__ = "speeds"
    index: Mapped[int] = mapped_column(primary_key=True)
    fileid: Mapped[int] = mapped_column(ForeignKey("torqfiles.fileid"))
    gpsspeedkmh = Column("gpsspeedkmh", Float, default=0, unique=False)
    speedobdkmh = Column("speedobdkmh", Float, default=0, unique=False)
    speedgpskmh = Column("speedgpskmh", Float, default=0, unique=False)
    gpstime = Column("gpstime", DateTime)


class Startpos(Base):
    __tablename__ = "startpos"
    startid: Mapped[int] = mapped_column(Integer, primary_key=True)
    latstart: Mapped[float | None] = mapped_column(Float, nullable=True)
    lonstart: Mapped[float | None] = mapped_column(Float, nullable=True)
    count: Mapped[int] = mapped_column(Integer, default=0)
    label: Mapped[str | None] = mapped_column(Text, nullable=True)


class Endpos(Base):
    __tablename__ = "endpos"
    endid: Mapped[int] = mapped_column(primary_key=True)
    latend: Mapped[float | None] = mapped_column(Float, nullable=True)
    lonend: Mapped[float | None] = mapped_column(Float, nullable=True)
    count: Mapped[int] = mapped_column(Integer, default=0)
    label: Mapped[str | None] = mapped_column(Text, nullable=True)

class Label(Base):
    __tablename__ = "labels"
    labelid: Mapped[int] = mapped_column(primary_key=True)
    label: Mapped[str | None] = mapped_column(Text, nullable=True)
    count: Mapped[int] = mapped_column(Integer, default=0)

class Position(Base):
    __tablename__ = "positions"
    positionid: Mapped[int] = mapped_column(primary_key=True)
    labelid: Mapped[int | None] = mapped_column(ForeignKey("labels.labelid"), nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    count: Mapped[int] = mapped_column(Integer, default=0)

class TorqFile(Base):
    __tablename__ = "torqfiles"
    fileid: Mapped[int] = mapped_column(primary_key=True)
    startid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    endid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    csvfile = Column("csvfile", Text)
    # csvhash = Column('csvhash', Text, unique=True, nullable=False)
    csvhash: Mapped[str] = mapped_column(String, unique=True, nullable=True)
    import_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    trip_start: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    trip_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    trip_duration: Mapped[float | None] = mapped_column(Float, nullable=True)
    trip_distance: Mapped[int | None] = mapped_column(Integer, nullable=True)
    readtime: Mapped[float | None] = mapped_column(Float, nullable=True)
    sendtime: Mapped[float | None] = mapped_column(Float, nullable=True)
    startlon: Mapped[float | None] = mapped_column(Float, nullable=True)
    startlat: Mapped[float | None] = mapped_column(Float, nullable=True)
    endlon: Mapped[float | None] = mapped_column(Float, nullable=True)
    endlat: Mapped[float | None] = mapped_column(Float, nullable=True)
    sent_rows = Column("sent_rows", Integer, default=0, unique=False)

    def __init__(self, csvfile, csvhash, fileid=None):
        self.fileid = (
            fileid if fileid is not None else stable_fileid_from_csvhash(str(csvhash))
        )
        self.csvfile = csvfile
        self.csvhash = csvhash
        self.import_date = datetime.now()


class MapImageCache(Base):
    __tablename__ = "mapimagecache"
    __table_args__ = (
        UniqueConstraint(
            "selection_key", "zoom", "colormap", name="uq_mapimagecache_key"
        ),
    )
    cacheid: Mapped[int] = mapped_column(primary_key=True)
    fileid: Mapped[int | None] = mapped_column(
        ForeignKey("torqfiles.fileid"), nullable=True
    )
    selection_key: Mapped[str] = mapped_column(Text, nullable=False)
    zoom: Mapped[int] = mapped_column(Integer, nullable=False)
    colormap: Mapped[str] = mapped_column(Text, nullable=False)
    image_png: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    hit_count: Mapped[int] = mapped_column(Integer, default=0)
    last_used: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class Torqtrips(Base):
    __tablename__ = "torqtrips"
    id: Mapped[int] = mapped_column(primary_key=True)
    fileid: Mapped[int] = mapped_column(ForeignKey("torqfiles.fileid"))
    trip_distance = Column("trip_distance", Integer)
    tripdate = Column("tripdate", DateTime)
    profile = Column("profile", Text)
    time = Column("time", Integer)
    profile_fuelused = Column("profile_fuelused", Float)
    profile_fuelcost = Column("profile_fuelcost", Float)
    profile_time = Column("profile_time", Float)
    profile_distanceWhilstConnectedToOBD = Column("profile_distanceWhilstConnectedToOBD", Float)
    profile_distance = Column("profile_distance", Float)
    profile_date = Column("profile_date", DateTime)

    def __init__(self, fileid):
        self.fileid = fileid


class Torqlogs(Base):
    __tablename__ = "torqlogs"
    id: Mapped[int] = mapped_column(primary_key=True)
    fileid: Mapped[int] = mapped_column(ForeignKey("torqfiles.fileid"))
    gpstime = Column("gpstime", DateTime)
    devicetime = Column("devicetime", DateTime)

    # if DB column is "Longitude", map it to python attr "longitude"
    longitude: Mapped[float | None] = mapped_column("longitude", Float, nullable=True)
    latitude: Mapped[float | None] = mapped_column("latitude", Float, nullable=True)
    speedgpskmh: Mapped[float | None] = mapped_column(
        "speedgpskmh", Float, nullable=True
    )
    gpsspeedkmh: Mapped[float | None] = mapped_column(
        "gpsspeedkmh", Float, nullable=True
    )

    def __init__(self, fileid):
        self.fileid = fileid


def database_dropall(engine):  # drop all tables
    logger.warning(f"[database_dropall] engine:{engine}")
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def database_init(engine):  # create tables
    try:
        Base.metadata.create_all(bind=engine)
        with engine.begin() as conn:
            logger.debug(f"dbinit for: {conn.dialect.name} engine {engine}")
            # Keep legacy databases aligned: enforce stable identity by hash.
            # conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_torqfiles_csvhash ON torqfiles(csvhash)"))

            if conn.dialect.name != "sqlite":
                # Ensure torqfiles is properly linked to start/end position tables when supported by backend.
                try:
                    conn.execute(text("""
                                    DO $$
                                    BEGIN
                                        IF NOT EXISTS (
                                            SELECT 1
                                            FROM pg_constraint
                                            WHERE conname = 'torqfiles_startid_fkey'
                                        ) THEN
                                            ALTER TABLE torqfiles
                                            ADD CONSTRAINT torqfiles_startid_fkey
                                            FOREIGN KEY (startid) REFERENCES startpos(startid)
                                            ON UPDATE CASCADE ON DELETE SET NULL;
                                        END IF;
                                        IF NOT EXISTS (
                                            SELECT 1
                                            FROM pg_constraint
                                            WHERE conname = 'torqfiles_endid_fkey'
                                        ) THEN
                                            ALTER TABLE torqfiles
                                            ADD CONSTRAINT torqfiles_endid_fkey
                                            FOREIGN KEY (endid) REFERENCES endpos(endid)
                                            ON UPDATE CASCADE ON DELETE SET NULL;
                                        END IF;
                                    END $$;
                                    """))
                except Exception as e:
                    # SQLite and older DB variants may not support PL/pgSQL blocks.
                    logger.warning(
                        f"Skipping optional torqfiles FK constraint migration: {e} ({type(e)})"
                    )

            # Unified view for start/end position analytics and grouping in GUI tools.
            conn.execute(text("DROP VIEW IF EXISTS trip_start_end_summary"))
            conn.execute(text("""
                    CREATE VIEW trip_start_end_summary AS
                    SELECT
                        tf.fileid,
                        tf.startid,
                        tf.endid,
                        sp.latstart,
                        sp.lonstart,
                        sp.label AS start_label,
                        ep.latend,
                        ep.lonend,
                        ep.label AS end_label,
                        tt.tripdate,
                        tt.time AS trip_time_s,
                        tt.trip_distance AS trip_distance_m
                    FROM torqfiles tf
                    LEFT JOIN startpos sp ON sp.startid = tf.startid
                    LEFT JOIN endpos ep ON ep.endid = tf.endid
                    LEFT JOIN torqtrips tt ON tt.fileid = tf.fileid
                    """))

            # Structural indexes not covered by the per-metric partial indexes.
            # torqlogs(fileid): plain index for all general WHERE fileid = / IN queries.
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_torqlogs_fileid ON torqlogs (fileid)"
                )
            )
            # torqfiles FK lookup columns used in JOINs to startpos/endpos.
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_torqfiles_startid ON torqfiles (startid)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_torqfiles_endid ON torqfiles (endid)"
                )
            )
            # torqtrips(fileid): used in the trip_start_end_summary view JOIN.
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_torqtrips_fileid ON torqtrips (fileid)"
                )
            )
            # filestats(fileid): FK column for per-file stat lookups.
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_filestats_fileid ON filestats (fileid)"
                )
            )
            # startpos/endpos spatial range queries (map bounds BETWEEN filtering).
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_startpos_latlon ON startpos (latstart, lonstart)"
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_endpos_latlon ON endpos (latend, lonend)"
                )
            )
            # startpos/endpos label columns for label-group trip selection queries.
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_startpos_label ON startpos (label)")
            )
            conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_endpos_label ON endpos (label)")
            )
            logger.debug("Structural indexes ensured")
    except (OperationalError, AssertionError) as e:
        logger.error(f"[dbinit] {type(e)} {e}")
        sys.exit(-1)
    except Exception as e:
        logger.error(f"[dbinit] Unexpected error: {type(e)} {e}")
        sys.exit(-1)


if __name__ == "__main__":
    pass
