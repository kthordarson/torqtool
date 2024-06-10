import sys
import uuid

import pandas as pd
from loguru import logger
from sqlalchemy import VARCHAR, Column, DateTime, Float, ForeignKey, Integer, Text, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

#  select fileid,count(id) as entrycount from torqlogs group by fileid order by entrycount;
# reg = registry()
# SELECT (select count(*) from torqlogs where fileid=101) as count, (SELECT gpstime FROM torqlogs WHERE fileid=101 ORDER BY gpstime LIMIT 1) as 'first',(SELECT gpstime FROM torqlogs WHERE fileid=101 ORDER BY gpstime DESC LIMIT 1) as 'last';
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
	longitude = Column('longitude', VARCHAR(30))
	latitude = Column('latitude', VARCHAR(30))
	horizontaldilutionofprecision = Column('horizontaldilutionofprecision', VARCHAR(35))
	bearing = Column('bearing', VARCHAR(35))
	coingkmaveragegkm = Column('coingkmaveragegkm', VARCHAR(35))
	coingkminstantaneousgkm = Column('coingkminstantaneousgkm', VARCHAR(35))
	fuelflowrateminuteccmin = Column('fuelflowrateminuteccmin', VARCHAR(35))
	milespergalloninstantmpg = Column('milespergalloninstantmpg', VARCHAR(35))
	milespergallonlongtermaveragempg = Column('milespergallonlongtermaveragempg', VARCHAR(35))
	accelerationsensortotalg = Column('accelerationsensortotalg', VARCHAR(35))
	accelerationsensorxaxisg = Column('accelerationsensorxaxisg', VARCHAR(35))
	accelerationsensoryaxisg = Column('accelerationsensoryaxisg', VARCHAR(35))
	accelerationsensorzaxisg = Column('accelerationsensorzaxisg', VARCHAR(35))
	androiddevicebatterylevel = Column('androiddevicebatterylevel', VARCHAR(35))
	distancetoemptyestimatedkm = Column('distancetoemptyestimatedkm', VARCHAR(35))
	engineload = Column('engineload', VARCHAR(35))
	enginerpmrpm = Column('enginerpmrpm', VARCHAR(35))
	fuelcosttripcost = Column('fuelcosttripcost', VARCHAR(35))
	fuelflowratehourlhr = Column('fuelflowratehourlhr', VARCHAR(35))
	fuelremainingcalculatedfromvehicleprofile = Column('fuelremainingcalculatedfromvehicleprofile', VARCHAR(35))
	fuelusedtripl = Column('fuelusedtripl', VARCHAR(35))
	gpsaccuracym = Column('gpsaccuracym', VARCHAR(35))
	gpsaltitudem = Column('gpsaltitudem', VARCHAR(35))
	gpsbearing = Column('gpsbearing', VARCHAR(35))
	gpslatitude = Column('gpslatitude', VARCHAR(35))
	gpslongitude = Column('gpslongitude', VARCHAR(35))
	gpssatellites = Column('gpssatellites', VARCHAR(35))
	gpsvsobdspeeddifferencekmh = Column('gpsvsobdspeeddifferencekmh', VARCHAR(35))
	horsepoweratthewheelshp = Column('horsepoweratthewheelshp', VARCHAR(35))
	kilometersperlitreinstantkpl = Column('kilometersperlitreinstantkpl', VARCHAR(35))
	litresper100kilometerinstantl100km = Column('litresper100kilometerinstantl100km', VARCHAR(35))
	massairflowrategs = Column('massairflowrategs', VARCHAR(35))
	speedobdkmh = Column('speedobdkmh', VARCHAR(35))
	voltageobdadapterv = Column('voltageobdadapterv', VARCHAR(35))
	volumetricefficiencycalculated = Column('volumetricefficiencycalculated', VARCHAR(35))
	speedgpskmh = Column('speedgpskmh', VARCHAR(35))
	actualenginetorque = Column('actualenginetorque', VARCHAR(35))
	averagetripspeedwhilststoppedormovingkmh = Column('averagetripspeedwhilststoppedormovingkmh', VARCHAR(35))
	distancetravelledwithmilcellitkm = Column('distancetravelledwithmilcellitkm', VARCHAR(35))
	kilometersperlitrelongtermaveragekpl = Column('kilometersperlitrelongtermaveragekpl', VARCHAR(35))
	litresper100kilometerlongtermaveragel100km = Column('litresper100kilometerlongtermaveragel100km', VARCHAR(35))
	tripaveragekplkpl = Column('tripaveragekplkpl', VARCHAR(35))
	tripaveragelitres100kml100km = Column('tripaveragelitres100kml100km', VARCHAR(35))
	tripaveragempgmpg = Column('tripaveragempgmpg', VARCHAR(35))
	tripdistancekm = Column('tripdistancekm', VARCHAR(35))
	tripdistancestoredinvehicleprofilekm = Column('tripdistancestoredinvehicleprofilekm', VARCHAR(35))
	triptimesincejourneystarts = Column('triptimesincejourneystarts', VARCHAR(35))
	triptimewhilstmovings = Column('triptimewhilstmovings', VARCHAR(35))
	triptimewhilststationarys = Column('triptimewhilststationarys', VARCHAR(35))
	gpsspeedkmh = Column('gpsspeedkmh', Text)
	altitudem = Column('altitudem', VARCHAR(35))
	gravityxg = Column('gravityxg', VARCHAR(35))
	gravityyg = Column('gravityyg', VARCHAR(35))
	gravityzg = Column('gravityzg', VARCHAR(35))
	enginecoolanttemperaturef = Column('enginecoolanttemperaturef', VARCHAR(35))
	fuelrailpressurekpa = Column('fuelrailpressurekpa', VARCHAR(35))
	intakeairtemperaturef = Column('intakeairtemperaturef', VARCHAR(35))
	intakemanifoldpressurekpa = Column('intakemanifoldpressurekpa', VARCHAR(35))
	torqueftlb = Column('torqueftlb', VARCHAR(35))
	turboboostvacuumgaugebar = Column('turboboostvacuumgaugebar', VARCHAR(35))
	enginekwatthewheelskw = Column('enginekwatthewheelskw', VARCHAR(35))
	averagetripspeedwhilstmovingonlykmh = Column('averagetripspeedwhilstmovingonlykmh', VARCHAR(35))
	airfuelratiomeasured1 = Column('airfuelratiomeasured1', VARCHAR(35))
	o2sensor1widerangecurrentma = Column('o2sensor1widerangecurrentma', VARCHAR(35))
	o2bank1sensor1widerangeequivalenceratio = Column('o2bank1sensor1widerangeequivalenceratio', VARCHAR(35))
	o2bank1sensor1widerangevoltagev = Column('o2bank1sensor1widerangevoltagev', VARCHAR(35))
	positivekineticenergypkekmhr = Column('positivekineticenergypkekmhr', VARCHAR(35))
	throttlepositionmanifold = Column('throttlepositionmanifold', VARCHAR(35))
	barometricpressurefromvpsi = Column('barometricpressurefromvpsi', VARCHAR(35))
	voltagecontrolmodulev = Column('voltagecontrolmodulev', VARCHAR(35))
	costpermilekminstantkm = Column('costpermilekminstantkm', VARCHAR(35))
	costpermilekmtripkm = Column('costpermilekmtripkm', VARCHAR(35)) # costpermilekmtripntkm
	gpsspeedmeterssecond = Column('gpsspeedmeterssecond', Text)
	altitude = Column('altitude', VARCHAR(35))
	gx = Column('gx', VARCHAR(35))
	gy = Column('gy', VARCHAR(35))
	gz = Column('gz', VARCHAR(35))
	gcalibrated = Column('gcalibrated', VARCHAR(35))
	airfuelrationmeasure = Column('airfuelrationmeasure', VARCHAR(35))
	torquefnm = Column('torquefnm', VARCHAR(35))
	enginecoolanttemperaturec = Column('enginecoolanttemperaturec', VARCHAR(35))
	fuelrailpressurepsi = Column('fuelrailpressurepsi', VARCHAR(35))
	intakeairtemperaturec = Column('intakeairtemperaturec', VARCHAR(35))
	intakemanifoldpressurepsi = Column('intakemanifoldpressurepsi', VARCHAR(35))
	torquenm = Column('torquenm', VARCHAR(35))
	turboboostvacuumgaugepsi = Column('turboboostvacuumgaugepsi', VARCHAR(35))
	barometricpressurefromvehiclekpa = Column('barometricpressurefromvehiclekpa', VARCHAR(35))
	ambientairtempf = Column('ambientairtempf', VARCHAR(35))
	barometricpressurefromvehiclepsi = Column('barometricpressurefromvehiclepsi', VARCHAR(35))
	ambientairtempc = Column('ambientairtempc', VARCHAR(35))
	fuelpressurekpa = Column('fuelpressurekpa', VARCHAR(35))

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
