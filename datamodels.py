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
	longitude = Column('longitude', Double) # 'longitude':np.float64,
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
	costpermilekmtripkm = Column('costpermilekmtripkm', Double) # costpermilekmtripntkm
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
