import sys
import re
import hashlib
from datetime import datetime
import pandas as pd
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

COLUMN_TYPES = {
    "GPS_Time": String,
    "Device_Time": String,
    "longitude": Float,
    "latitude": Float,
    "GPS_Speed_Meterssecond": Float,
    "Horizontal_Dilution_of_Precision": Float,
    "Altitude": Float,
    "Bearing": Float,
    "Gx": Float,
    "Gy": Float,
    "Gz": Float,
    "Gcalibrated": Float,
    "Acceleration_SensorTotalg": Float,
    "Acceleration_SensorX_axisg": Float,
    "Acceleration_SensorY_axisg": Float,
    "Acceleration_SensorZ_axisg": Float,
    "Actual_engine_torque": Float,
    "Air_Fuel_RatioMeasured1": Float,
    "Android_device_Battery_Level": Integer,
    "Average_trip_speedwhilst_moving_onlykmh": Float,
    "Average_trip_speedwhilst_stopped_or_movingkmh": Float,
    "Barometric_pressure_from_vehiclepsi": Float,
    "CO_in_gkm_Averagegkm": Float,
    "CO_in_gkm_Instantaneousgkm": Float,
    "Distance_to_empty_Estimatedkm": Float,
    "Distance_travelled_with_MILCEL_litkm": Float,
    "Engine_Coolant_TemperatureC": Float,
    "Engine_kW_At_the_wheelskW": Float,
    "Engine_Load": Float,
    "Engine_RPMrpm": Float,
    "Fuel_cost_tripcost": Float,
    "Fuel_flow_ratehourlhr": Float,
    "Fuel_flow_rateminuteccmin": Float,
    "Fuel_Rail_Pressurepsi": Float,
    "Fuel_Remaining_Calculated_from_vehicle_profile": Float,
    "Fuel_used_tripl": Float,
    "GPS_Accuracym": Float,
    "GPS_Altitudem": Float,
    "GPS_Bearing": Float,
    "GPS_Latitude": Float,
    "GPS_Longitude": Float,
    "GPS_Satellites": Integer,
    "GPS_vs_OBD_Speed_differencekmh": Float,
    "Horsepower_At_the_wheelshp": Float,
    "Intake_Air_TemperatureC": Float,
    "Intake_Manifold_Pressurepsi": Float,
    "Kilometers_Per_LitreInstantkpl": Float,
    "Kilometers_Per_LitreLong_Term_Averagekpl": Float,
    "Litres_Per_100_KilometerInstantl100km": Float,
    "Litres_Per_100_KilometerLong_Term_Averagel100km": Float,
    "Mass_Air_Flow_Rategs": Float,
    "Miles_Per_GallonInstantmpg": Float,
    "Miles_Per_GallonLong_Term_Averagempg": Float,
    "O2_Sensor1_Wide_Range_CurrentmA": Float,
    "O2_Bank_1_Sensor_1_Wide_Range_Equivalence_Ratio": Float,
    "O2_Bank_1_Sensor_1_Wide_Range_VoltageV": Float,
    "Speed_GPSkmh": Float,
    "Speed_OBDkmh": Float,
    "TorqueNm": Float,
    "Trip_average_KPLkpl": Float,
    "Trip_average_Litres100_KMl100km": Float,
    "Trip_average_MPGmpg": Float,
    "Trip_Distancekm": Float,
    "Trip_distance_stored_in_vehicle_profilekm": Float,
    "Trip_TimeSince_journey_starts": Float,
    "Trip_timewhilst_movings": Float,
    "Trip_timewhilst_stationarys": Float,
    "Turbo_Boost_Vacuum_Gaugepsi": Float,
    "Voltage_OBD_AdapterV": Float,
    "Volumetric_Efficiency_Calculated": Float,
    "Ambient_air_tempC": Float,
    "Cost_per_milekm_Instantkm": Float,
    "Cost_per_milekm_Tripkm": Float,
    "Positive_Kinetic_Energy_PKEkmhr": Float,
    "Throttle_PositionManifold": Float,
    "Voltage_Control_ModuleV": Float,
    "O2_Sensor1_Wide_Range_Equivalence_Ratio": Float,
    "O2_Sensor1_Wide_Range_VoltageV": Float,
    "gpstime": String,
    "devicetime": String,
    "gpsspeedmeterssecond": Float,
    "horizontaldilutionofprecision": Float,
    "altitude": Float,
    "bearing": Float,
    "gx": Float,
    "gy": Float,
    "gz": Float,
    "gcalibrated": Float,
    "accelerationsensortotalg": Float,
    "accelerationsensorxaxisg": Float,
    "accelerationsensoryaxisg": Float,
    "accelerationsensorzaxisg": Float,
    "actualenginetorque": Float,
    "airfuelratiomeasured1": Float,
    "androiddevicebatterylevel": Integer,
    "averagetripspeedwhilstmovingonlykmh": Float,
    "averagetripspeedwhilststoppedormovingkmh": Float,
    "barometricpressurefromvehiclepsi": Float,
    "coingkmaveragegkm": Float,
    "coingkminstantaneousgkm": Float,
    "distancetoemptyestimatedkm": Float,
    "distancetravelledwithmilcellitkm": Float,
    "enginecoolanttemperaturec": Float,
    "enginekwatthewheelskw": Float,
    "engineload": Float,
    "enginerpmrpm": Float,
    "fuelcosttripcost": Float,
    "fuelflowratehourlhr": Float,
    "fuelflowrateminuteccmin": Float,
    "fuelrailpressurepsi": Float,
    "fuelremainingcalculatedfromvehicleprofile": Float,
    "fuelusedtripl": Float,
    "gpsaccuracym": Float,
    "gpsaltitudem": Float,
    "gpsbearing": Float,
    "gpslatitude": Float,
    "gpslongitude": Float,
    "gpssatellites": Integer,
    "gpsvsobdspeeddifferencekmh": Float,
    "horsepoweratthewheelshp": Float,
    "intakeairtemperaturec": Float,
    "intakemanifoldpressurepsi": Float,
    "kilometersperlitreinstantkpl": Float,
    "kilometersperlitrelongtermaveragekpl": Float,
    "litresper100kilometerinstantl100km": Float,
    "litresper100kilometerlongtermaveragel100km": Float,
    "massairflowrategs": Float,
    "milespergalloninstantmpg": Float,
    "milespergallonlongtermaveragempg": Float,
    "o2sensor1widerangecurrentma": Float,
    "o2bank1sensor1widerangeequivalenceratio": Float,
    "o2bank1sensor1widerangevoltagev": Float,
    "speedgpskmh": Float,
    "speedobdkmh": Float,
    "torquenm": Float,
    "tripaveragekplkpl": Float,
    "tripaveragelitres100kml100km": Float,
    "tripaveragempgmpg": Float,
    "tripdistancekm": Float,
    "tripdistancestoredinvehicleprofilekm": Float,
    "triptimesincejourneystarts": Float,
    "triptimewhilstmovings": Float,
    "triptimewhilststationarys": Float,
    "turboboostvacuumgaugepsi": Float,
    "voltageobdadapterv": Float,
    "volumetricefficiencycalculated": Float,
    "ambientairtempc": Float,
    "costpermilekminstantkm": Float,
    "costpermilekmtripkm": Float,
    "positivekineticenergypkekmhr": Float,
    "throttlepositionmanifold": Float,
    "voltagecontrolmodulev": Float,
    "o2sensor1widerangeequivalenceratio": Float,
    "o2sensor1widerangevoltagev": Float,
    "barometer_on_android_devicemb": Float,
    "barometricpressurefromvehiclekpa": Float,
    "catalyst_temperature_bank_1_sensor_1c": Float,
    "catalyst_temperature_bank_1_sensor_1f": Float,
    "catalyst_temperature_bank_1_sensor_2c": Float,
    "catalyst_temperature_bank_1_sensor_2f": Float,
    "catalyst_temperature_bank_2_sensor_1c": Float,
    "catalyst_temperature_bank_2_sensor_1f": Float,
    "catalyst_temperature_bank_2_sensor_2c": Float,
    "catalyst_temperature_bank_2_sensor_2f": Float,
    "charge_air_cooler_temperature_cactc": Float,
    "charge_air_cooler_temperature_cactf": Float,
    "commanded_equivalence_ratiolambda": Float,
    "cost_per_milekm_instantkm": Float,
    "cost_per_milekm_tripkm": Float,
    "distance_travelled_since_codes_clearedkm": Float,
    "dpf_pressurebar": Float,
    "dpf_pressurepsi": Float,
    "dpf_temperaturec": Float,
    "dpf_temperaturef": Float,
    "drivers_demand_engine__torque": Float,
    "egr_commanded": Float,
    "egr_error": Float,
    "eighthMileTime": Float,
    "engine_loadabsolute": Float,
    "engine_oil_temperaturec": Float,
    "engine_oil_temperaturef": Float,
    "engine_reference_torquenm": Float,
    "enginecoolanttemperaturef": Float,
    "ethanol_fuel_": Float,
    "evap_system_vapour_pressurepa": Float,
    "exhaust_gas_temp_bank_1_sensor_1c": Float,
    "exhaust_gas_temp_bank_1_sensor_1f": Float,
    "exhaust_gas_temp_bank_1_sensor_2c": Float,
    "exhaust_gas_temp_bank_1_sensor_2f": Float,
    "exhaust_gas_temp_bank_1_sensor_3c": Float,
    "exhaust_gas_temp_bank_1_sensor_3f": Float,
    "exhaust_gas_temp_bank_1_sensor_4c": Float,
    "exhaust_gas_temp_bank_1_sensor_4f": Float,
    "exhaust_gas_temp_bank_2_sensor_1c": Float,
    "exhaust_gas_temp_bank_2_sensor_1f": Float,
    "exhaust_gas_temp_bank_2_sensor_2c": Float,
    "exhaust_gas_temp_bank_2_sensor_2f": Float,
    "exhaust_gas_temp_bank_2_sensor_3c": Float,
    "exhaust_gas_temp_bank_2_sensor_3f": Float,
    "exhaust_gas_temp_bank_2_sensor_4c": Float,
    "exhaust_gas_temp_bank_2_sensor_4f": Float,
    "exhaust_pressurebar": Float,
    "exhaust_pressurepsi": Float,
    "fuel_level_from_engine_ecu": Float,
    "fuel_pressurepsi": Float,
    "fuel_rail_pressure_relative_to_manifold_vacuumkpa": Float,
    "fuel_rail_pressure_relative_to_manifold_vacuumpsi": Float,
    "fuel_rate_direct_from_eculm": Float,
    "fuel_trim_bank_1_long_term": Float,
    "fuel_trim_bank_1_sensor_1": Float,
    "fuelpressurekpa": Float,
    "fuelrailpressurekpa": Float,
    "gpsspeedkmh": Float,
    "gravityx": Float,
    "gravityxg": Float,
    "gravityy": Float,
    "gravityyg": Float,
    "gravityz": Float,
    "gravityzg": Float,
    "hybrid_battery_charge_": Float,
    "intakeairtemperaturef": Float,
    "intakemanifoldpressurekpa": Float,
    "kphTime0-100": Float,
    "kphTime0-200": Float,
    "kphTime0200": Float,
    "kphTime100-0": Float,
    "kphTime100-200": Float,
    "kphTime1000": Float,
    "kphTime100200": Float,
    "kphTime80-120": Float,
    "miletimes14": Float,
    "miletimes18": Float,
    "mphTime0-100": Float,
    "mphTime0-30": Float,
    "mphTime0-60": Float,
    "mphTime40-60": Float,
    "mphTime60-0": Float,
    "mphTime60-120": Float,
    "mphTime60-130": Float,
    "mphTime60-80": Float,
    "mphTime80-100": Float,
    "mphtimes01008": Float,
    "mphtimes030": Float,
    "mphtimes060": Float,
    "mphtimes60120": Float,
    "mphtimes60130": Float,
    "mphtimes6080": Float,
    "nox_post_scrppm": Float,
    "nox_pre_scrppm": Float,
    "o2_sensor1_equivalence_ratio": Float,
    "o2_sensor1_equivalence_ratioalternate": Float,
    "percentage_of_highway_driving": Float,
    "percentageofcitydriving": Float,
    "percentageofidledriving": Float,
    "quarterMileTime": Float,
    "relative_accelerator_pedal_position": Float,
    "relative_throttle_position": Float,
    "run_time_since_engine_starts": Float,
    "timing_advance": Float,
    "torqueftlb": Float,
    "transmission_temperaturemethod_1c": Float,
    "transmission_temperaturemethod_1f": Float,
    "transmission_temperaturemethod_2c": Float,
    "transmission_temperaturemethod_2f": Float,
    "turbo_pressure_controlbar": Float,
    "turbo_pressure_controlpsi": Float,
    "turboboostvacuumgaugebar": Float,
    "absolute_throttle_position_b": Float,
    "accelerator_pedalposition_d": Float,
    "accelerator_pedalposition_e": Float,
    "accelerator_pedalposition_f": Float,
    "air_fuel_ratiocommanded1": Float,
    "altitudem": Float,
    "ambientairtempf": Float
}

# Also support normalized Torq header variants (for example GPS_Time -> gpstime).
for _col_name, _col_type in list(COLUMN_TYPES.items()):
    _normalized = re.sub(r"[^A-Za-z0-9]+", "", _col_name).lower()
    COLUMN_TYPES.setdefault(_normalized, _col_type)


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


async def send_torqfiles(filelist, session, debug=False):  # returns list of new files
    """
    send list of files to db
    returns list of TorqFile objects to be processed and sent to db
    """
    torqdbfiles = session.query(TorqFile).all()  # get list of files from db
    hlist = pd.DataFrame(session.query(TorqFile.csvhash).all())  # type: ignore
    if debug:
        logger.debug(
            f"filelist: {len(filelist)} dbfiles: {len(torqdbfiles)}  hashes: {len(hlist)} fl: {len(filelist)}"
        )
    newfiles = []
    for idx, tf in enumerate(filelist):
        csvfile = str(tf["csvfile"])
        csvhash = tf["csvhash"]
        stable_fileid = stable_fileid_from_csvhash(csvhash)
        if csvhash in hlist.values:  # [k.csvhash for k in torqdbfiles]:
            # check existing entry
            fid = session.execute(
                text(f'select fileid from torqfiles where csvhash="{csvhash}"')
            ).one()[0]
            check = session.execute(
                text(f"select count(*) from torqlogs where fileid={fid}")
            ).one()[0]
            if debug:
                logger.warning(
                    f"[st {idx}/{len(filelist)}] {csvfile} {fid=} already in db with {check}"
                )  # {tf}')
        else:
            existing_by_id = (
                session.query(TorqFile).filter(TorqFile.fileid == stable_fileid).first()
            )
            if existing_by_id and existing_by_id.csvhash != csvhash:
                logger.error(
                    f"stable fileid collision for {csvfile}: fileid={stable_fileid} "
                    f"existing_hash={existing_by_id.csvhash} new_hash={csvhash}"
                )
                continue
            torqfile = TorqFile(csvfile=csvfile, csvhash=csvhash, fileid=stable_fileid)
            session.add(torqfile)
            if debug:
                pass  # logger.info(f'[st {idx}/{len(filelist)}] {csvfile} not in db tf: {tf} torqfile: {torqfile}')
            newfiles.append(torqfile)
    session.commit()
    torqdbfiles = session.query(TorqFile).all()
    # newfiles = [k for k in filelist if k['csvhash'] not in hlist]
    logger.info(
        f"[st] done sending {len(newfiles)} newfilelist torqdbfiles: {len(torqdbfiles)}"
    )
    return newfiles  # return list of new files


if __name__ == "__main__":
    pass
