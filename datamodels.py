import sys
from datetime import datetime
import pandas as pd
from loguru import logger
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, Text, text, String
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

COLUMN_TYPES = {
		'GPS_Time': String,
		'Device_Time': String,
		'Longitude': Float,
		'Latitude': Float,
		'GPS_Speed_Meterssecond': Float,
		'Horizontal_Dilution_of_Precision': Float,
		'Altitude': Float,
		'Bearing': Float,
		'Gx': Float,
		'Gy': Float,
		'Gz': Float,
		'Gcalibrated': Float,
		'Acceleration_SensorTotalg': Float,
		'Acceleration_SensorX_axisg': Float,
		'Acceleration_SensorY_axisg': Float,
		'Acceleration_SensorZ_axisg': Float,
		'Actual_engine_torque': Float,
		'Air_Fuel_RatioMeasured1': Float,
		'Android_device_Battery_Level': Integer,
		'Average_trip_speedwhilst_moving_onlykmh': Float,
		'Average_trip_speedwhilst_stopped_or_movingkmh': Float,
		'Barometric_pressure_from_vehiclepsi': Float,
		'CO_in_gkm_Averagegkm': Float,
		'CO_in_gkm_Instantaneousgkm': Float,
		'Distance_to_empty_Estimatedkm': Float,
		'Distance_travelled_with_MILCEL_litkm': Float,
		'Engine_Coolant_TemperatureC': Float,
		'Engine_kW_At_the_wheelskW': Float,
		'Engine_Load': Float,
		'Engine_RPMrpm': Float,
		'Fuel_cost_tripcost': Float,
		'Fuel_flow_ratehourlhr': Float,
		'Fuel_flow_rateminuteccmin': Float,
		'Fuel_Rail_Pressurepsi': Float,
		'Fuel_Remaining_Calculated_from_vehicle_profile': Float,
		'Fuel_used_tripl': Float,
		'GPS_Accuracym': Float,
		'GPS_Altitudem': Float,
		'GPS_Bearing': Float,
		'GPS_Latitude': Float,
		'GPS_Longitude': Float,
		'GPS_Satellites': Integer,
		'GPS_vs_OBD_Speed_differencekmh': Float,
		'Horsepower_At_the_wheelshp': Float,
		'Intake_Air_TemperatureC': Float,
		'Intake_Manifold_Pressurepsi': Float,
		'Kilometers_Per_LitreInstantkpl': Float,
		'Kilometers_Per_LitreLong_Term_Averagekpl': Float,
		'Litres_Per_100_KilometerInstantl100km': Float,
		'Litres_Per_100_KilometerLong_Term_Averagel100km': Float,
		'Mass_Air_Flow_Rategs': Float,
		'Miles_Per_GallonInstantmpg': Float,
		'Miles_Per_GallonLong_Term_Averagempg': Float,
		'O2_Sensor1_Wide_Range_CurrentmA': Float,
		'O2_Bank_1_Sensor_1_Wide_Range_Equivalence_Ratio': Float,
		'O2_Bank_1_Sensor_1_Wide_Range_VoltageV': Float,
		'Speed_GPSkmh': Float,
		'Speed_OBDkmh': Float,
		'TorqueNm': Float,
		'Trip_average_KPLkpl': Float,
		'Trip_average_Litres100_KMl100km': Float,
		'Trip_average_MPGmpg': Float,
		'Trip_Distancekm': Float,
		'Trip_distance_stored_in_vehicle_profilekm': Float,
		'Trip_TimeSince_journey_starts': Float,
		'Trip_timewhilst_movings': Float,
		'Trip_timewhilst_stationarys': Float,
		'Turbo_Boost_Vacuum_Gaugepsi': Float,
		'Voltage_OBD_AdapterV': Float,
		'Volumetric_Efficiency_Calculated': Float,
		'Ambient_air_tempC': Float,
		'Cost_per_milekm_Instantkm': Float,
		'Cost_per_milekm_Tripkm': Float,
		'Positive_Kinetic_Energy_PKEkmhr': Float,
		'Throttle_PositionManifold': Float,
		'Voltage_Control_ModuleV': Float,
		'O2_Sensor1_Wide_Range_Equivalence_Ratio': Float,
		'O2_Sensor1_Wide_Range_VoltageV': Float,
	}

class Base(DeclarativeBase):
	pass

class Filestats(Base):
	__tablename__ = 'filestats'
	index: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	column = Column('column', Text)
	nulls = Column('nulls', Integer, default=0, unique=False)
	nullratio = Column('nullratio', Float, default=0, unique=False)

class Speeds(Base):
	__tablename__ = 'speeds'
	index: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	gpsspeedkmh = Column('gpsspeedkmh', Float, default=0, unique=False)
	speedobdkmh = Column('speedobdkmh', Float, default=0, unique=False)
	speedgpskmh = Column('speedgpskmh', Float, default=0, unique=False)
	gpstime = Column('gpstime', DateTime)

class Startpos(Base):
	__tablename__ = 'startpos'
	startid: Mapped[int] = mapped_column(primary_key=True)
	# fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	latstart = Column('latstart', Float, default=0, unique=False)
	lonstart = Column('lonstart', Float, default=0, unique=False)
	count = Column('count', Integer, default=0, unique=False)
	label = Column('label', Text)


class Endpos(Base):
	__tablename__ = 'endpos'
	endid: Mapped[int] = mapped_column(primary_key=True)
	# fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	latend = Column('latend', Float, default=0, unique=False)
	lonend = Column('lonend', Float, default=0, unique=False)
	count = Column('count', Integer, default=0, unique=False)
	label = Column('label', Text)

class TorqFile(Base):
	__tablename__ = 'torqfiles'
	fileid: Mapped[int] = mapped_column(primary_key=True)
	# startid: Mapped[int] = mapped_column(ForeignKey('startpos.startid'))
	startid = Column('startid', Integer, default=0, unique=False)
	endid = Column('endid', Integer, default=0, unique=False)
	csvfile = Column('csvfile', Text)
	csvhash = Column('csvhash', Text)
	import_date = Column('import_date', DateTime)
	trip_start = Column('trip_start', DateTime)
	trip_end = Column('trip_end', DateTime)
	trip_duration = Column('trip_duration', Float)
	trip_distance = Column('trip_distance', Integer)
	readtime = Column('readtime', Float)
	sendtime = Column('sendtime', Float)
	# startlon = Column('startlon', Float)
	# startlat = Column('startlat', Float)
	# endlon = Column('endlon', Float)
	# endlat = Column('endlat', Float)
	startlon: Mapped[float | None] = mapped_column(Float, nullable=True)
	startlat: Mapped[float | None] = mapped_column(Float, nullable=True)
	endlon: Mapped[float | None] = mapped_column(Float, nullable=True)
	endlat: Mapped[float | None] = mapped_column(Float, nullable=True)
	sent_rows = Column('sent_rows', Integer, default=0, unique=False)

	def __init__(self, csvfile, csvhash):
		self.csvfile = csvfile
		self.csvhash = csvhash
		self.import_date = datetime.now()

class Torqtrips(Base):
	__tablename__ = 'torqtrips'
	id: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	csvfile = Column('csvfile', Text)
	csvhash = Column('csvhash', Text)
	distance = Column('distance', Integer)
	trip_distance = Column('trip_distance', Integer)
	fuelcost = Column('fuelcost', Integer)
	fuelused = Column('fuelused', Integer)
	distancewhilstconnectedtoobd = Column('distancewhilstconnectedtoobd', Integer)
	tripdate = Column('tripdate', DateTime)
	profile = Column('profile', Text)
	time = Column('time', Integer)
	triptime = Column('triptime', Integer)

	def __init__(self, fileid=0, csvfile=None, csvhash=None, distance=None, fuelcost=None, fuelused=None, distancewhilstconnectedtoobd=None, tripdate=None, profile=None, triptime=None):
		self.fileid = fileid
		self.csvfile = csvfile
		self.csvhash = csvhash
		self.distance = distance
		self.trip_distance = distance
		self.fuelcost = fuelcost
		self.fuelused = fuelused
		self.distancewhilstconnectedtoobd = distancewhilstconnectedtoobd
		self.tripdate = tripdate
		self.profile = profile
		self.time = triptime
		self.triptime = triptime
	# def __repr__(self):
	# 	return f'<Torqtrips id:{self.id} file:{self.fileid} {self.csvfile}>'

class Torqlogs(Base):
	__tablename__ = 'torqlogs'
	id: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))

	# if DB column is "Longitude", map it to python attr "longitude"
	longitude: Mapped[float | None] = mapped_column("Longitude", Float, nullable=True)
	latitude: Mapped[float | None] = mapped_column("Latitude", Float, nullable=True)
	speedgpskmh: Mapped[float | None] = mapped_column("speedgpskmh", Float, nullable=True)
	gpsspeedkmh: Mapped[float | None] = mapped_column("gpsspeedkmh", Float, nullable=True)

	def __init__(self, fileid):
		self.fileid = fileid

def database_dropall(engine):  # drop all tables
	logger.warning(f'[database_dropall] engine:{engine}')
	Base.metadata.drop_all(bind=engine)
	Base.metadata.create_all(bind=engine)

def database_init(engine):  # create tables
	try:
		Base.metadata.create_all(bind=engine)
	except (OperationalError, AssertionError) as e:
		logger.error(f'[dbinit] {type(e)} {e}')
		sys.exit(-1)

async def send_torqfiles(filelist, session, debug=False):  # returns list of new files
	"""
	send list of files to db
	returns list of TorqFile objects to be processed and sent to db
	"""
	torqdbfiles = session.query(TorqFile).all()  # get list of files from db
	hlist = pd.DataFrame(session.query(TorqFile.csvhash).all())  # type: ignore
	if debug:
		logger.debug(f'filelist: {len(filelist)} dbfiles: {len(torqdbfiles)}  hashes: {len(hlist)} fl: {len(filelist)}')
	newfiles = []
	for idx,tf in enumerate(filelist):
		csvfile = str(tf['csvfile'])
		csvhash = tf['csvhash']
		if csvhash in hlist.values:  # [k.csvhash for k in torqdbfiles]:
			# check existing entry
			fid = session.execute(text(f'select fileid from torqfiles where csvhash="{csvhash}"')).one()[0]
			check = session.execute(text(f'select count(*) from torqlogs where fileid={fid}')).one()[0]
			if debug:
				logger.warning(f'[st {idx}/{len(filelist)}] {csvfile} {fid=} already in db with {check}')  # {tf}')
		else:
			torqfile = TorqFile(csvfile=csvfile, csvhash=csvhash)
			session.add(torqfile)
			if debug:
				pass   # logger.info(f'[st {idx}/{len(filelist)}] {csvfile} not in db tf: {tf} torqfile: {torqfile}')
			newfiles.append(torqfile)
	session.commit()
	torqdbfiles = session.query(TorqFile).all()
	# newfiles = [k for k in filelist if k['csvhash'] not in hlist]
	logger.info(f'[st] done sending {len(newfiles)} newfilelist torqdbfiles: {len(torqdbfiles)}')
	return newfiles   # return list of new files


if __name__ == '__main__':
	pass
