import sys
import uuid

import pandas as pd
from loguru import logger
from sqlalchemy import VARCHAR, Column, DateTime, Float, ForeignKey, Integer, Text, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.sqltypes import Double
class Base(DeclarativeBase):
    pass

def genuuid():
	return str(uuid.uuid4())
# tables = ['torqtrips', 'torqlogs', 'torqfiles', 'torqdata']

class TorqFile(Base):
	__tablename__ = 'torqfiles'
	fileid: Mapped[int] = mapped_column(primary_key=True)
	csvfile = Column('csvfile', Text)
	csvhash = Column('csvhash', Text)
	sent_rows = Column('sent_rows', Integer, default=0, unique=False)
	read_flag = Column('read_flag', Integer, default=0, unique=False) # 0 = not read, 1 = read
	send_flag = Column('send_flag', Integer, default=0, unique=False) # 0 = not sent, 1 = sent
	fixed_flag = Column('fixed_flag', Integer, default=0, unique=False) # 0 = not fixed, 1 = fixed

	data_flag = Column('data_flag', Integer, default=0, unique=False)
	# 0 = need tripdata, 1 = have tripdata

	error_flag = Column('error_flag', Integer, default=0, unique=False)
	# 0 = no error, 1 = has error, 2 = need split
	# 3 = header error, 4 = fixerror, 5 = senderror, 6 = polarreaderror

	def __init__(self, csvfile, csvhash):
		self.csvfile = csvfile
		self.csvhash = csvhash
		self.read_flag = 0
		self.send_flag = 0
		self.fixed_flag = 0
		self.error_flag = 0
		self.data_flag = 0

	# def __repr__(self):
	# 	# fn = self.csvfile.split('/')[-1]
	# 	return f'<TorqFile id: {self.fileid}  csvfile=( {self.csvfile} ) r:{self.read_flag} s:{self.send_flag} f:{self.fixed_flag} d:{self.data_flag} >'

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
	# fileid = Column('fileid', Integer) # Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))
	gpstime = Column('gpstime', DateTime)
	devicetime = Column('devicetime', DateTime)
	longitude = Column('longitude', Float) # 'longitude':np.float64,
	latitude = Column('latitude', Float)
	horizontaldilutionofprecision = Column('horizontaldilutionofprecision', Float)
	bearing = Column('bearing', Float)
	coingkmaveragegkm = Column('coingkmaveragegkm', Float)
	coingkminstantaneousgkm = Column('coingkminstantaneousgkm', Float)
	fuelflowrateminuteccmin = Column('fuelflowrateminuteccmin', Float)
	milespergalloninstantmpg = Column('milespergalloninstantmpg', Float)
	milespergallonlongtermaveragempg = Column('milespergallonlongtermaveragempg', Float)
	accelerationsensortotalg = Column('accelerationsensortotalg', Float)
	accelerationsensorxaxisg = Column('accelerationsensorxaxisg', Float)
	accelerationsensoryaxisg = Column('accelerationsensoryaxisg', Float)
	accelerationsensorzaxisg = Column('accelerationsensorzaxisg', Float)
	androiddevicebatterylevel = Column('androiddevicebatterylevel', Float)
	distancetoemptyestimatedkm = Column('distancetoemptyestimatedkm', Float)
	engineload = Column('engineload', Float)
	enginerpmrpm = Column('enginerpmrpm', Float)
	fuelcosttripcost = Column('fuelcosttripcost', Float)
	fuelflowratehourlhr = Column('fuelflowratehourlhr', Float)
	fuelremainingcalculatedfromvehicleprofile = Column('fuelremainingcalculatedfromvehicleprofile', Float)
	fuelusedtripl = Column('fuelusedtripl', Float)
	gpsaccuracym = Column('gpsaccuracym', Float)
	gpsaltitudem = Column('gpsaltitudem', Float)
	gpsbearing = Column('gpsbearing', Float)
	gpslatitude = Column('gpslatitude', Float)
	gpslongitude = Column('gpslongitude', Float)
	gpssatellites = Column('gpssatellites', Float)
	gpsvsobdspeeddifferencekmh = Column('gpsvsobdspeeddifferencekmh', Float)
	horsepoweratthewheelshp = Column('horsepoweratthewheelshp', Float)
	kilometersperlitreinstantkpl = Column('kilometersperlitreinstantkpl', Float)
	litresper100kilometerinstantl100km = Column('litresper100kilometerinstantl100km', Float)
	massairflowrategs = Column('massairflowrategs', Float)
	speedobdkmh = Column('speedobdkmh', Float)
	voltageobdadapterv = Column('voltageobdadapterv', Float)
	volumetricefficiencycalculated = Column('volumetricefficiencycalculated', Float)
	speedgpskmh = Column('speedgpskmh', Float)
	actualenginetorque = Column('actualenginetorque', Float)
	averagetripspeedwhilststoppedormovingkmh = Column('averagetripspeedwhilststoppedormovingkmh', Float)
	distancetravelledwithmilcellitkm = Column('distancetravelledwithmilcellitkm', Float)
	kilometersperlitrelongtermaveragekpl = Column('kilometersperlitrelongtermaveragekpl', Float)
	litresper100kilometerlongtermaveragel100km = Column('litresper100kilometerlongtermaveragel100km', Float)
	tripaveragekplkpl = Column('tripaveragekplkpl', Float)
	tripaveragelitres100kml100km = Column('tripaveragelitres100kml100km', Float)
	tripaveragempgmpg = Column('tripaveragempgmpg', Float)
	tripdistancekm = Column('tripdistancekm', Float)
	tripdistancestoredinvehicleprofilekm = Column('tripdistancestoredinvehicleprofilekm', Float)
	triptimesincejourneystarts = Column('triptimesincejourneystarts', Float)
	triptimewhilstmovings = Column('triptimewhilstmovings', Float)
	triptimewhilststationarys = Column('triptimewhilststationarys', Float)
	gpsspeedkmh = Column('gpsspeedkmh', Float)
	altitudem = Column('altitudem', Float)
	gravityxg = Column('gravityxg', Float)
	gravityyg = Column('gravityyg', Float)
	gravityzg = Column('gravityzg', Float)
	enginecoolanttemperaturef = Column('enginecoolanttemperaturef', Double)
	fuelrailpressurekpa = Column('fuelrailpressurekpa', Float)
	intakeairtemperaturef = Column('intakeairtemperaturef', Double)
	intakemanifoldpressurekpa = Column('intakemanifoldpressurekpa', Float)
	torqueftlb = Column('torqueftlb', Float)
	turboboostvacuumgaugebar = Column('turboboostvacuumgaugebar', Float)
	enginekwatthewheelskw = Column('enginekwatthewheelskw', Float)
	averagetripspeedwhilstmovingonlykmh = Column('averagetripspeedwhilstmovingonlykmh', Float)
	airfuelratiomeasured1 = Column('airfuelratiomeasured1', Float)
	o2sensor1widerangecurrentma = Column('o2sensor1widerangecurrentma', Float)
	o2bank1sensor1widerangeequivalenceratio = Column('o2bank1sensor1widerangeequivalenceratio', Float)
	o2bank1sensor1widerangevoltagev = Column('o2bank1sensor1widerangevoltagev', Float)
	positivekineticenergypkekmhr = Column('positivekineticenergypkekmhr', Float)
	throttlepositionmanifold = Column('throttlepositionmanifold', Float)
	barometricpressurefromvpsi = Column('barometricpressurefromvpsi', Float)
	voltagecontrolmodulev = Column('voltagecontrolmodulev', Float)
	costpermilekminstantkm = Column('costpermilekminstantkm', Float)
	costpermilekmtripkm = Column('costpermilekmtripkm', Float) # costpermilekmtripntkm
	gpsspeedmeterssecond = Column('gpsspeedmeterssecond', Float)
	altitude = Column('altitude', Float)
	gx = Column('gx', Float)
	gy = Column('gy', Float)
	gz = Column('gz', Float)
	gcalibrated = Column('gcalibrated', Float)
	airfuelrationmeasure = Column('airfuelrationmeasure', Float)
	torquefnm = Column('torquefnm', Float)
	enginecoolanttemperaturec = Column('enginecoolanttemperaturec', Float)
	fuelrailpressurepsi = Column('fuelrailpressurepsi', Float)
	intakeairtemperaturec = Column('intakeairtemperaturec', Float)
	intakemanifoldpressurepsi = Column('intakemanifoldpressurepsi', Float)
	torquenm = Column('torquenm', Float)
	turboboostvacuumgaugepsi = Column('turboboostvacuumgaugepsi', Float)
	barometricpressurefromvehiclekpa = Column('barometricpressurefromvehiclekpa', Float)
	ambientairtempf = Column('ambientairtempf', Float)
	barometricpressurefromvehiclepsi = Column('barometricpressurefromvehiclepsi', Float)
	ambientairtempc = Column('ambientairtempc', Float)
	fuelpressurekpa = Column('fuelpressurekpa', Float)
	percentageofcitydriving = Column('percentageofcitydriving', Float)
	percentageofhighwaydriving = Column('percentageofhighwaydriving', Float)
	percentageofidledriving = Column('percentageofidledriving', Float)


	def __init__(self, fileid):
		self.fileid = fileid

	# def __repr__(self):
	# 	return f'<Torqdata {self.id}  file:{self.fileid}>'

class Torqdata(Base):
	__tablename__ = 'torqdata'
	id: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.fileid'))

	# def __repr__(self):
	# 	return f'<Torqtrips id:{self.id} f:{self.fileid} >'

	def __init__(self, fileid):
		self.fileid = fileid

def database_dropall(engine): # drop all tables
	logger.warning(f'[database_dropall] engine:{engine}')
	Base.metadata.drop_all(bind=engine)
	Base.metadata.create_all(bind=engine)

def database_init(engine): # create tables
	try:
		Base.metadata.create_all(bind=engine)
	except (OperationalError, AssertionError) as e:
		logger.error(f'[dbinit] {type(e)} {e}')
		sys.exit(-1)

def send_torqfiles(filelist=[], session=None, debug=False): # returns list of new files
	"""
	send list of files to db
	returns list of TorqFile objects to be processed and sent to db
	"""
	#torqdbfiles = session.execute(text(f'select * from torqfiles;')).all()
	# torqdbfiles = session.query(TorqFile).all() # get list of files from db
	torqdbfiles = session.query(TorqFile).all() # get list of files from db
	# hlist = pd.DataFrame([session.query(TorqFile.csvhash).all()])
	hlist = pd.DataFrame(session.query(TorqFile.csvhash).all())
	if debug:
		logger.debug(f'filelist: {len(filelist)} dbfiles: {len(torqdbfiles)}  hashes: {len(hlist)} fl: {len(filelist)}')
	newfiles = []
	for idx,tf in enumerate(filelist):
		csvfile = str(tf['csvfile'])
		csvhash = tf['csvhash']
		if csvhash in hlist.values: #[k.csvhash for k in torqdbfiles]:
			# check existing entry
			# fid = session.execute(text('select fileid from torqfiles where csvhash=:csvhash'), {'csvhash':csvhash}).one()[0]
			fid = session.execute(text(f'select fileid from torqfiles where csvhash="{csvhash}"')).one()[0]
			check = session.execute(text(f'select count(*) from torqlogs where fileid={fid}')).one()[0]
			if debug:
				logger.warning(f'[st {idx}/{len(filelist)}] {csvfile} {fid=} already in db with {check}') # {tf}')
		else:
			torqfile = TorqFile(csvfile=csvfile,  csvhash=csvhash)
			torqfile.send_flag = 1
			session.add(torqfile)

			if debug:
				pass # logger.info(f'[st {idx}/{len(filelist)}] {csvfile} not in db tf: {tf} torqfile: {torqfile}')
			newfiles.append(torqfile)
	session.commit()
	torqdbfiles = session.query(TorqFile).all()
	# newfiles = [k for k in filelist if k['csvhash'] not in hlist]
	logger.info(f'[st] done sending {len(newfiles)} newfilelist torqdbfiles: {len(torqdbfiles)}')
	return newfiles # return list of new files


if __name__ == '__main__':
	pass
