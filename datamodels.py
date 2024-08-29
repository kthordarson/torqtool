import sys
import uuid
from datetime import datetime
import pandas as pd
from loguru import logger
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, Text, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.sqltypes import Double
class Base(DeclarativeBase):
	pass

def genuuid():
	return str(uuid.uuid4())

# x = latitude y = longitude !

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
	#fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
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
	#startid: Mapped[int] = mapped_column(ForeignKey('startpos.startid'))
	startid = Column('startid', Integer, default=0, unique=False)
	endid = Column('endid', Integer, default=0, unique=False)
	csvfile = Column('csvfile', Text)
	csvhash = Column('csvhash', Text)
	import_date = Column('import_date', DateTime)
	trip_start = Column('trip_start', DateTime)
	trip_end = Column('trip_end', DateTime)
	trip_duration = Column('trip_duration', Float)
	readtime = Column('readtime', Float)
	sendtime = Column('sendtime', Float)
	startlon = Column('startlon', Float)
	startlat = Column('startlat', Float)
	endlon = Column('endlon', Float)
	endlat = Column('endlat', Float)
	sent_rows = Column('sent_rows', Integer, default=0, unique=False)
	read_flag = Column('read_flag', Integer, default=0, unique=False)  # 0 = not read, 1 = read
	send_flag = Column('send_flag', Integer, default=0, unique=False)  # 0 = not sent, 1 = sent
	fixed_flag = Column('fixed_flag', Integer, default=0, unique=False)  # 0 = not fixed, 1 = fixed
	data_flag = Column('data_flag', Integer, default=0, unique=False)	# 0 = need tripdata, 1 = have tripdata
	error_flag = Column('error_flag', Integer, default=0, unique=False)
	# 0 = no error,
	# 1 = dupe entry in db,
	# 2 = need split
	# 3 = header error,
	# 4 = fixerror,
	# 5 = senderror,
	# 6 = polarreaderror
	# 7 = unknowncolumnerror

	def __init__(self, csvfile, csvhash):
		self.csvfile = csvfile
		self.csvhash = csvhash
		self.read_flag = 0
		self.send_flag = 0
		self.fixed_flag = 0
		self.error_flag = 0
		self.data_flag = 0
		self.import_date = datetime.now()

class Torqtrips(Base):
	__tablename__ = 'torqtrips'
	id: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	csvfile = Column('csvfile', Text)
	csvhash = Column('csvhash', Text)
	distance = Column('distance', Integer)
	fuelcost = Column('fuelcost', Integer)
	fuelused = Column('fuelused', Integer)
	distancewhilstconnectedtoobd = Column('distancewhilstconnectedtoobd', Integer)
	tripdate = Column('tripdate', DateTime)
	profile = Column('profile', Text)
	time = Column('time', Integer)

	def __init__(self, fileid=None, csvfile=None, csvhash=None, distance=None, fuelcost=None, fuelused=None, distancewhilstconnectedtoobd=None, tripdate=None, profile=None, triptime=None):
		self.fileid = fileid
		self.csvfile = csvfile
		self.csvhash = csvhash
		self.distance = distance
		self.fuelcost = fuelcost
		self.fuelused = fuelused
		self.distancewhilstconnectedtoobd = distancewhilstconnectedtoobd
		self.tripdate = tripdate
		self.profile = profile
		self.time = triptime
	# def __repr__(self):
	# 	return f'<Torqtrips id:{self.id} file:{self.fileid} {self.csvfile}>'

class Torqlogs(Base):
	__tablename__ = 'torqlogs'
	id: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	# fileid = Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid')) #
	# fileid = Column('fileid', Integer)  # Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	gpstime = Column('gpstime', DateTime)
	devicetime = Column('devicetime', DateTime)
	longitude = Column('longitude', Double)  # 'longitude':np.float64,
	latitude = Column('latitude', Double)
	horizontaldilutionofprecision = Column('horizontaldilutionofprecision', Double)
	bearing = Column('bearing', Double)
	coingkmaveragegkm = Column('coingkmaveragegkm', Double)
	coingkminstantaneousgkm = Column('coingkminstantaneousgkm', Double)
	fuelflowrateminuteccmin = Column('fuelflowrateminuteccmin', Double)
	milespergalloninstantmpg = Column('milespergalloninstantmpg', Double)
	milespergallonlongtermaveragempg = Column('milespergallonlongtermaveragempg', Double)
	accelerationsensortotalg = Column('accelerationsensortotalg', Double)
	accelerationsensorxaxisg = Column('accelerationsensorxaxisg', Double)
	accelerationsensoryaxisg = Column('accelerationsensoryaxisg', Double)
	accelerationsensorzaxisg = Column('accelerationsensorzaxisg', Double)
	androiddevicebatterylevel = Column('androiddevicebatterylevel', Double)
	distancetoemptyestimatedkm = Column('distancetoemptyestimatedkm', Double)
	engineload = Column('engineload', Double)
	enginerpmrpm = Column('enginerpmrpm', Double)
	fuelcosttripcost = Column('fuelcosttripcost', Double)
	fuelflowratehourlhr = Column('fuelflowratehourlhr', Double)
	fuelremainingcalculatedfromvehicleprofile = Column('fuelremainingcalculatedfromvehicleprofile', Double)
	fuelusedtripl = Column('fuelusedtripl', Double)
	gpsaccuracym = Column('gpsaccuracym', Double)
	gpsaltitudem = Column('gpsaltitudem', Double)
	gpsbearing = Column('gpsbearing', Double)
	gpslatitude = Column('gpslatitude', Double)
	gpslongitude = Column('gpslongitude', Double)
	gpssatellites = Column('gpssatellites', Double)
	gpsvsobdspeeddifferencekmh = Column('gpsvsobdspeeddifferencekmh', Double)
	horsepoweratthewheelshp = Column('horsepoweratthewheelshp', Double)
	kilometersperlitreinstantkpl = Column('kilometersperlitreinstantkpl', Double)
	litresper100kilometerinstantl100km = Column('litresper100kilometerinstantl100km', Double)
	massairflowrategs = Column('massairflowrategs', Double)
	speedobdkmh = Column('speedobdkmh', Double)
	voltageobdadapterv = Column('voltageobdadapterv', Double)
	volumetricefficiencycalculated = Column('volumetricefficiencycalculated', Double)
	speedgpskmh = Column('speedgpskmh', Double)
	actualenginetorque = Column('actualenginetorque', Double)
	averagetripspeedwhilststoppedormovingkmh = Column('averagetripspeedwhilststoppedormovingkmh', Double)
	distancetravelledwithmilcellitkm = Column('distancetravelledwithmilcellitkm', Double)
	kilometersperlitrelongtermaveragekpl = Column('kilometersperlitrelongtermaveragekpl', Double)
	litresper100kilometerlongtermaveragel100km = Column('litresper100kilometerlongtermaveragel100km', Double)
	tripaveragekplkpl = Column('tripaveragekplkpl', Double)
	tripaveragelitres100kml100km = Column('tripaveragelitres100kml100km', Double)
	tripaveragempgmpg = Column('tripaveragempgmpg', Double)
	tripdistancekm = Column('tripdistancekm', Double)
	tripdistancestoredinvehicleprofilekm = Column('tripdistancestoredinvehicleprofilekm', Double)
	triptimesincejourneystarts = Column('triptimesincejourneystarts', Double)
	triptimewhilstmovings = Column('triptimewhilstmovings', Double)
	triptimewhilststationarys = Column('triptimewhilststationarys', Double)
	gpsspeedkmh = Column('gpsspeedkmh', Double)
	altitudem = Column('altitudem', Double)
	gravityxg = Column('gravityxg', Double)
	gravityyg = Column('gravityyg', Double)
	gravityzg = Column('gravityzg', Double)
	enginecoolanttemperaturef = Column('enginecoolanttemperaturef', Double)
	fuelrailpressurekpa = Column('fuelrailpressurekpa', Double)
	intakeairtemperaturef = Column('intakeairtemperaturef', Double)
	intakemanifoldpressurekpa = Column('intakemanifoldpressurekpa', Double)
	torqueftlb = Column('torqueftlb', Double)
	turboboostvacuumgaugebar = Column('turboboostvacuumgaugebar', Double)
	enginekwatthewheelskw = Column('enginekwatthewheelskw', Double)
	averagetripspeedwhilstmovingonlykmh = Column('averagetripspeedwhilstmovingonlykmh', Double)
	airfuelratiomeasured1 = Column('airfuelratiomeasured1', Double)
	o2sensor1widerangecurrentma = Column('o2sensor1widerangecurrentma', Double)
	o2bank1sensor1widerangeequivalenceratio = Column('o2bank1sensor1widerangeequivalenceratio', Double)
	o2bank1sensor1widerangevoltagev = Column('o2bank1sensor1widerangevoltagev', Double)
	positivekineticenergypkekmhr = Column('positivekineticenergypkekmhr', Double)
	throttlepositionmanifold = Column('throttlepositionmanifold', Double)
	barometricpressurefromvpsi = Column('barometricpressurefromvpsi', Double)
	voltagecontrolmodulev = Column('voltagecontrolmodulev', Double)
	costpermilekminstantkm = Column('costpermilekminstantkm', Double)
	costpermilekmtripkm = Column('costpermilekmtripkm', Double)  # costpermilekmtripntkm
	gpsspeedmeterssecond = Column('gpsspeedmeterssecond', Double)
	altitude = Column('altitude', Double)
	gx = Column('gx', Double)
	gy = Column('gy', Double)
	gz = Column('gz', Double)
	gcalibrated = Column('gcalibrated', Double)
	airfuelrationmeasure = Column('airfuelrationmeasure', Double)
	torquefnm = Column('torquefnm', Double)
	enginecoolanttemperaturec = Column('enginecoolanttemperaturec', Double)
	fuelrailpressurepsi = Column('fuelrailpressurepsi', Double)
	intakeairtemperaturec = Column('intakeairtemperaturec', Double)
	intakemanifoldpressurepsi = Column('intakemanifoldpressurepsi', Double)
	torquenm = Column('torquenm', Double)
	turboboostvacuumgaugepsi = Column('turboboostvacuumgaugepsi', Double)
	barometricpressurefromvehiclekpa = Column('barometricpressurefromvehiclekpa', Double)
	ambientairtempf = Column('ambientairtempf', Double)
	barometricpressurefromvehiclepsi = Column('barometricpressurefromvehiclepsi', Double)
	ambientairtempc = Column('ambientairtempc', Double)
	fuelpressurekpa = Column('fuelpressurekpa', Double)
	percentageofcitydriving = Column('percentageofcitydriving', Double)
	percentageofhighwaydriving = Column('percentageofhighwaydriving', Double)
	percentageofidledriving = Column('percentageofidledriving', Double)
	fuelpressurepsi = Column('fuelpressurepsi', Double)
	airfuelratiocommanded1 = Column('airfuelratiocommanded1', Double)
	distancetravelledsincecodesclearedkm = Column('distancetravelledsincecodesclearedkm', Double)
	dpfpressurepsi = Column('dpfpressurepsi', Double)
	dpftemperaturec = Column('dpftemperaturec', Double)
	dpfpressurebar = Column('dpfpressurebar', Double)
	driversdemandenginetorque = Column('driversdemandenginetorque', Double)
	o2sensor1widerangeequivalenceratio = Column('o2sensor1widerangeequivalenceratio', Double)
	o2sensor1widerangevoltagev = Column('o2sensor1widerangevoltagev', Double)
	voltagecontrolmodulev = Column('voltagecontrolmodulev', Double)
	volumetricefficiencycalculated = Column('volumetricefficiencycalculated', Double)
	# 0200kphtimes dpfpressurepsi 030mphtimes 060mphtimes 14miletimes 18miletimes 1000kphtimes

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

def send_torqfiles(filelist=[], session=None, debug=False):  # returns list of new files
	"""
	send list of files to db
	returns list of TorqFile objects to be processed and sent to db
	"""
	torqdbfiles = session.query(TorqFile).all()  # get list of files from db
	hlist = pd.DataFrame(session.query(TorqFile.csvhash).all())
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
			torqfile.send_flag = 1
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
