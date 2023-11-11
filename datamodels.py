import sys
import os
import uuid
from datetime import datetime
from hashlib import md5
from pathlib import Path

from loguru import logger
from sqlalchemy import (BigInteger, Column, DateTime, Float, ForeignKey, Integer, String, Table, inspect, select, text)
from sqlalchemy.exc import (OperationalError, ProgrammingError)
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, sessionmaker)
from sqlalchemy_utils import create_database, database_exists


class Base(DeclarativeBase):
    pass

def genuuid():
	return str(uuid.uuid4())
# tables = ['torqtrips', 'torqlogs', 'torqfiles', 'torqdata']

class TorqFile(Base):
	__tablename__ = 'torqfiles'
	id: Mapped[int] = mapped_column(primary_key=True)
	tripid = Column('tripid', Integer)
	# tripid: Mapped[int] = mapped_column(ForeignKey('torqtrips.id'))
	csvfilename = Column('csvfilename', String(255))
	csvfilefixed = Column('csvfilefixed', String(255))
	csvhash = Column('csvhash', String(255))
	fixedhash = Column('fixedhash', String(255))
	read_flag = Column('read_flag', Integer)
	send_flag = Column('send_flag', Integer)

	def __init__(self, csvfilename, csvfilefixed, csvhash, fixedhash):
		self.csvfilename = csvfilename
		self.csvfilefixed = csvfilefixed
		self.csvhash = csvhash
		self.fixedhash = fixedhash
		self.read_flag = 0
		self.send_flag = 0
	def __repr__(self):
		return f'<TorqFile id:{self.id} {self.csvfilefixed} >'

class Torqtrips(Base):
	__tablename__ = 'torqtrips'
	id: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.id'))
	csvfilename = Column('csvfilename', String(255))
	csvhash = Column('csvhash', String(255))
	distance = Column('distance', BigInteger)
	fuelcost = Column('fuelcost', BigInteger)
	fuelused = Column('fuelused', BigInteger)
	distancewhilstconnectedtoobd = Column('distancewhilstconnectedtoobd', BigInteger)
	tripdate = Column('tripdate', DateTime)
	profile = Column('profile', String(255))
	time = Column('time', BigInteger)
	#tt = Torqtrips(fileid=torqfile.id, csvfilename=str(torqfile.csvfilename), csvhash=torqfile.csvhash, distance=trip['distance'], fuelcost=trip['fuelcost'], fuelused=trip['fuelused'], distancewhilstconnectedtoobd=trip['distancewhilstconnectedtoobd'], tripdate=trip['tripdate'], profile=trip['profile'], triptime=trip['time'])
	def __init__(self, fileid=None, csvfilename=None, csvhash=None, distance=None, fuelcost=None, fuelused=None, distancewhilstconnectedtoobd=None, tripdate=None, profile=None, triptime=None):
		self.fileid = fileid
		self.csvfilename = csvfilename
		self.csvhash = csvhash
		self.distance = distance
		self.fuelcost = fuelcost
		self.fuelused = fuelused
		self.distancewhilstconnectedtoobd = distancewhilstconnectedtoobd
		self.tripdate = tripdate
		self.profile = profile
		self.time = triptime
	def __repr__(self):
		return f'<Torqtrips id:{self.id} file:{self.fileid} {self.csvfilename}>'

class Torqlogs(Base):
	__tablename__ = 'torqlogs'
	id: Mapped[int] = mapped_column(primary_key=True)
	tripid = Column('tripid', Integer) #Mapped[int] = mapped_column(ForeignKey('torqtrips.id'))
	fileid = Column('fileid', Integer) #Mapped[int] = mapped_column(ForeignKey('torqfiles.id'))
	gpstime = Column('gpstime', DateTime)
	devicetime = Column('devicetime', DateTime)
	longitude = Column('longitude', Float)
	latitude = Column('latitude', Float)
	gpsspeedkmh  = Column('gpsspeedkmh', Float)
	horizontaldilutionofprecision = Column('horizontaldilutionofprecision', Float)
	altitudem = Column('altitudem', Float)
	bearing = Column('bearing', Float)
	gravityxg = Column('gravityxg', Float)
	gravityyg = Column('gravityyg', Float)
	gravityzg = Column('gravityzg', Float)
	accelerationsensortotalg = Column('accelerationsensortotalg', Float)
	accelerationsensorxaxisg = Column('accelerationsensorxaxisg', Float)
	accelerationsensoryaxisg = Column('accelerationsensoryaxisg', Float)
	accelerationsensorzaxisg = Column('accelerationsensorzaxisg', Float)
	actualenginetorque = Column('actualenginetorque', Float)
	androiddevicebatterylevel = Column('androiddevicebatterylevel', Float)
	averagetripspeedwhilststoppedormovingkmh = Column('averagetripspeedwhilststoppedormovingkmh', Float)
	coingkmaveragegkm = Column('coingkmaveragegkm', Float)
	coingkminstantaneousgkm = Column('coingkminstantaneousgkm', Float)
	distancetoemptyestimatedkm = Column('distancetoemptyestimatedkm', Float)
	distancetravelledwithmilcellitkm = Column('distancetravelledwithmilcellitkm', Float)
	enginecoolanttemperaturef = Column('enginecoolanttemperaturef', Float)
	engineload = Column('engineload', Float)
	enginerpmrpm = Column('enginerpmrpm', Float)
	fuelcosttripcost = Column('fuelcosttripcost', Float)
	fuelflowratehourlhr = Column('fuelflowratehourlhr', Float)
	fuelflowrateminuteccmin = Column('fuelflowrateminuteccmin', Float)
	fuelrailpressurekpa = Column('fuelrailpressurekpa', Float)
	fuelremainingcalculatedfromvehicleprofile = Column('fuelremainingcalculatedfromvehicleprofile', Float)
	fuelusedtripl = Column('fuelusedtripl', Float)
	fuelpressurekpa = Column('fuelpressurekpa', Float)
	gpsaccuracym = Column('gpsaccuracym', Float)
	gpsaltitudem = Column('gpsaltitudem', Float)
	gpsbearing = Column('gpsbearing', Float)
	gpslatitude = Column('gpslatitude', Float)
	gpslongitude = Column('gpslongitude', Float)
	gpssatellites = Column('gpssatellites', Float)
	gpsvsobdspeeddifferencekmh = Column('gpsvsobdspeeddifferencekmh', Float)
	horsepoweratthewheelshp = Column('horsepoweratthewheelshp', Float)
	kilometersperlitreinstantkpl = Column('kilometersperlitreinstantkpl', Float)
	kilometersperlitrelongtermaveragekpl = Column('kilometersperlitrelongtermaveragekpl', Float)
	litresper100kilometerinstantl100km = Column('litresper100kilometerinstantl100km', Float)
	litresper100kilometerlongtermaveragel100km = Column('litresper100kilometerlongtermaveragel100km', Float)
	massairflowrategs = Column('massairflowrategs', Float)
	milespergalloninstantmpg = Column('milespergalloninstantmpg', Float)
	milespergallonlongtermaveragempg = Column('milespergallonlongtermaveragempg', Float)
	speedgpskmh = Column('speedgpskmh', Float)
	speedobdkmh = Column('speedobdkmh', Float)
	torqueftlb = Column('torqueftlb', Float)
	tripaveragekplkpl = Column('tripaveragekplkpl', Float)
	tripaveragelitres100kml100km = Column('tripaveragelitres100kml100km', Float)
	tripaveragempgmpg = Column('tripaveragempgmpg', Float)
	tripdistancekm = Column('tripdistancekm', Float)
	tripdistancestoredinvehicleprofilekm = Column('tripdistancestoredinvehicleprofilekm', Float)
	triptimesincejourneystarts = Column('triptimesincejourneystarts', Float)
	triptimewhilstmovings = Column('triptimewhilstmovings', Float)
	triptimewhilststationarys = Column('triptimewhilststationarys', Float)
	turboboostvacuumgaugebar = Column('turboboostvacuumgaugebar', Float)
	voltageobdadapterv = Column('voltageobdadapterv', Float)
	volumetricefficiencycalculated = Column('volumetricefficiencycalculated', Float)
	enginekwatthewheelskw = Column('enginekwatthewheelskw', Float)
	averagetripspeedwhilstmovingonlykmh = Column('averagetripspeedwhilstmovingonlykmh', Float)
	costpermilekminstantkm = Column('costpermilekminstantkm', Float)
	costpermilekmtripkm = Column('costpermilekmtripkm', Float)
	intakeairtemperaturef = Column('intakeairtemperaturef', Float)
	intakemanifoldpressurekpa = Column('intakemanifoldpressurekpa', Float)
	airfuelratiomeasured1 = Column('airfuelratiomeasured1', Float)
	ambientairtempf = Column('ambientairtempf', Float)
	barometricpressurefromvehiclekpa = Column('barometricpressurefromvehiclekpa', Float)
	o2sensor1widerangecurrentma = Column('o2sensor1widerangecurrentma', Float)
	o2bank1sensor1widerangeequivalenceratio = Column('o2bank1sensor1widerangeequivalenceratio', Float)
	o2bank1sensor1widerangevoltagev = Column('o2bank1sensor1widerangevoltagev', Float)
	positivekineticenergypkekmhr = Column('positivekineticenergypkekmhr', Float)
	throttlepositionmanifold = Column('throttlepositionmanifold', Float)
	voltagecontrolmodulev = Column('voltagecontrolmodulev', Float)

	def __init__(self, tripid, fileid):
		self.tripid = tripid
		self.fileid = fileid

	def __repr__(self):
		return f'<Torqdata {self.id} trip:{self.tripid} file:{self.fileid}>'

class Torqdata(Base):
	__tablename__ = 'torqdata'
	id: Mapped[int] = mapped_column(primary_key=True)
	tripdate = Column('tripdate', DateTime)
	distance = Column('distance', Float)
	fuelused = Column('fuelused', Float)
	fuelcost = Column('fuelcost', Float)
	time = Column('time', Float)
	distancewhilstconnectedtoobd = Column('distancewhilstconnectedtoobd', Float)
	minlongitude = Column('minlongitude', Float)
	maxlongitude = Column('maxlongitude', Float)
	avglongitude = Column('avglongitude', Float)
	minlatitude = Column('minlatitude', Float)
	maxlatitude = Column('maxlatitude', Float)
	avglatitude = Column('avglatitude', Float)
	mingpsspeedkmh = Column('mingpsspeedkmh', Float)
	maxgpsspeedkmh = Column('maxgpsspeedkmh', Float)
	avggpsspeedkmh = Column('avggpsspeedkmh', Float)
	minhorizontaldilutionofprecision = Column('minhorizontaldilutionofprecision', Float)
	maxhorizontaldilutionofprecision = Column('maxhorizontaldilutionofprecision', Float)
	avghorizontaldilutionofprecision = Column('avghorizontaldilutionofprecision', Float)
	minaltitudem = Column('minaltitudem', Float)
	maxaltitudem = Column('maxaltitudem', Float)
	avgaltitudem = Column('avgaltitudem', Float)
	minbearing = Column('minbearing', Float)
	maxbearing = Column('maxbearing', Float)
	avgbearing = Column('avgbearing', Float)
	mingravityxg = Column('mingravityxg', Float)
	maxgravityxg = Column('maxgravityxg', Float)
	avggravityxg = Column('avggravityxg', Float)
	mingravityyg = Column('mingravityyg', Float)
	maxgravityyg = Column('maxgravityyg', Float)
	avggravityyg = Column('avggravityyg', Float)
	mingravityzg = Column('mingravityzg', Float)
	maxgravityzg = Column('maxgravityzg', Float)
	avggravityzg = Column('avggravityzg', Float)
	minaccelerationsensortotalg = Column('minaccelerationsensortotalg', Float)
	maxaccelerationsensortotalg = Column('maxaccelerationsensortotalg', Float)
	avgaccelerationsensortotalg = Column('avgaccelerationsensortotalg', Float)
	minaccelerationsensorxaxisg = Column('minaccelerationsensorxaxisg', Float)
	maxaccelerationsensorxaxisg = Column('maxaccelerationsensorxaxisg', Float)
	avgaccelerationsensorxaxisg = Column('avgaccelerationsensorxaxisg', Float)
	minaccelerationsensoryaxisg = Column('minaccelerationsensoryaxisg', Float)
	maxaccelerationsensoryaxisg = Column('maxaccelerationsensoryaxisg', Float)
	avgaccelerationsensoryaxisg = Column('avgaccelerationsensoryaxisg', Float)
	minaccelerationsensorzaxisg = Column('minaccelerationsensorzaxisg', Float)
	maxaccelerationsensorzaxisg = Column('maxaccelerationsensorzaxisg', Float)
	avgaccelerationsensorzaxisg = Column('avgaccelerationsensorzaxisg', Float)
	minactualenginetorque = Column('minactualenginetorque', Float)
	maxactualenginetorque = Column('maxactualenginetorque', Float)
	avgactualenginetorque = Column('avgactualenginetorque', Float)
	minandroiddevicebatterylevel = Column('minandroiddevicebatterylevel', Float)
	maxandroiddevicebatterylevel = Column('maxandroiddevicebatterylevel', Float)
	avgandroiddevicebatterylevel = Column('avgandroiddevicebatterylevel', Float)
	minaveragetripspeedwhilststoppedormovingkmh = Column('minaveragetripspeedwhilststoppedormovingkmh', Float)
	maxaveragetripspeedwhilststoppedormovingkmh = Column('maxaveragetripspeedwhilststoppedormovingkmh', Float)
	avgaveragetripspeedwhilststoppedormovingkmh = Column('avgaveragetripspeedwhilststoppedormovingkmh', Float)
	mincoingkmaveragegkm = Column('mincoingkmaveragegkm', Float)
	maxcoingkmaveragegkm = Column('maxcoingkmaveragegkm', Float)
	avgcoingkmaveragegkm = Column('avgcoingkmaveragegkm', Float)
	mincoingkminstantaneousgkm = Column('mincoingkminstantaneousgkm', Float)
	maxcoingkminstantaneousgkm = Column('maxcoingkminstantaneousgkm', Float)
	avgcoingkminstantaneousgkm = Column('avgcoingkminstantaneousgkm', Float)
	mindistancetoemptyestimatedkm = Column('mindistancetoemptyestimatedkm', Float)
	maxdistancetoemptyestimatedkm = Column('maxdistancetoemptyestimatedkm', Float)
	avgdistancetoemptyestimatedkm = Column('avgdistancetoemptyestimatedkm', Float)
	mindistancetravelledwithmilcellitkm = Column('mindistancetravelledwithmilcellitkm', Float)
	maxdistancetravelledwithmilcellitkm = Column('maxdistancetravelledwithmilcellitkm', Float)
	avgdistancetravelledwithmilcellitkm = Column('avgdistancetravelledwithmilcellitkm', Float)
	minenginecoolanttemperaturef = Column('minenginecoolanttemperaturef', Float)
	maxenginecoolanttemperaturef = Column('maxenginecoolanttemperaturef', Float)
	avgenginecoolanttemperaturef = Column('avgenginecoolanttemperaturef', Float)
	minengineload = Column('minengineload', Float)
	maxengineload = Column('maxengineload', Float)
	avgengineload = Column('avgengineload', Float)
	minenginerpmrpm = Column('minenginerpmrpm', Float)
	maxenginerpmrpm = Column('maxenginerpmrpm', Float)
	avgenginerpmrpm = Column('avgenginerpmrpm', Float)
	minfuelcosttripcost = Column('minfuelcosttripcost', Float)
	maxfuelcosttripcost = Column('maxfuelcosttripcost', Float)
	avgfuelcosttripcost = Column('avgfuelcosttripcost', Float)
	minfuelflowratehourlhr = Column('minfuelflowratehourlhr', Float)
	maxfuelflowratehourlhr = Column('maxfuelflowratehourlhr', Float)
	avgfuelflowratehourlhr = Column('avgfuelflowratehourlhr', Float)
	minfuelflowrateminuteccmin = Column('minfuelflowrateminuteccmin', Float)
	maxfuelflowrateminuteccmin = Column('maxfuelflowrateminuteccmin', Float)
	avgfuelflowrateminuteccmin = Column('avgfuelflowrateminuteccmin', Float)
	minfuelrailpressurekpa = Column('minfuelrailpressurekpa', Float)
	maxfuelrailpressurekpa = Column('maxfuelrailpressurekpa', Float)
	avgfuelrailpressurekpa = Column('avgfuelrailpressurekpa', Float)
	minfuelremainingcalculatedfromvehicleprofile = Column('minfuelremainingcalculatedfromvehicleprofile', Float)
	maxfuelremainingcalculatedfromvehicleprofile = Column('maxfuelremainingcalculatedfromvehicleprofile', Float)
	avgfuelremainingcalculatedfromvehicleprofile = Column('avgfuelremainingcalculatedfromvehicleprofile', Float)
	minfuelusedtripl = Column('minfuelusedtripl', Float)
	maxfuelusedtripl = Column('maxfuelusedtripl', Float)
	avgfuelusedtripl = Column('avgfuelusedtripl', Float)
	mingpsaccuracym = Column('mingpsaccuracym', Float)
	maxgpsaccuracym = Column('maxgpsaccuracym', Float)
	avggpsaccuracym = Column('avggpsaccuracym', Float)
	mingpsaltitudem = Column('mingpsaltitudem', Float)
	maxgpsaltitudem = Column('maxgpsaltitudem', Float)
	avggpsaltitudem = Column('avggpsaltitudem', Float)
	mingpsbearing = Column('mingpsbearing', Float)
	maxgpsbearing = Column('maxgpsbearing', Float)
	avggpsbearing = Column('avggpsbearing', Float)
	mingpslatitude = Column('mingpslatitude', Float)
	maxgpslatitude = Column('maxgpslatitude', Float)
	avggpslatitude = Column('avggpslatitude', Float)
	mingpslongitude = Column('mingpslongitude', Float)
	maxgpslongitude = Column('maxgpslongitude', Float)
	avggpslongitude = Column('avggpslongitude', Float)
	mingpssatellites = Column('mingpssatellites', Float)
	maxgpssatellites = Column('maxgpssatellites', Float)
	avggpssatellites = Column('avggpssatellites', Float)
	mingpsvsobdspeeddifferencekmh = Column('mingpsvsobdspeeddifferencekmh', Float)
	maxgpsvsobdspeeddifferencekmh = Column('maxgpsvsobdspeeddifferencekmh', Float)
	avggpsvsobdspeeddifferencekmh = Column('avggpsvsobdspeeddifferencekmh', Float)
	minhorsepoweratthewheelshp = Column('minhorsepoweratthewheelshp', Float)
	maxhorsepoweratthewheelshp = Column('maxhorsepoweratthewheelshp', Float)
	avghorsepoweratthewheelshp = Column('avghorsepoweratthewheelshp', Float)
	minkilometersperlitrelongtermaveragekpl = Column('minkilometersperlitrelongtermaveragekpl', Float)
	maxkilometersperlitrelongtermaveragekpl = Column('maxkilometersperlitrelongtermaveragekpl', Float)
	avgkilometersperlitrelongtermaveragekpl = Column('avgkilometersperlitrelongtermaveragekpl', Float)
	minlitresper100kilometerinstantl100km = Column('minlitresper100kilometerinstantl100km', Float)
	maxlitresper100kilometerinstantl100km = Column('maxlitresper100kilometerinstantl100km', Float)
	avglitresper100kilometerinstantl100km = Column('avglitresper100kilometerinstantl100km', Float)
	minlitresper100kilometerlongtermaveragel100km = Column('minlitresper100kilometerlongtermaveragel100km', Float)
	maxlitresper100kilometerlongtermaveragel100km = Column('maxlitresper100kilometerlongtermaveragel100km', Float)
	avglitresper100kilometerlongtermaveragel100km = Column('avglitresper100kilometerlongtermaveragel100km', Float)
	minmassairflowrategs = Column('minmassairflowrategs', Float)
	maxmassairflowrategs = Column('maxmassairflowrategs', Float)
	avgmassairflowrategs = Column('avgmassairflowrategs', Float)
	minmilespergalloninstantmpg = Column('minmilespergalloninstantmpg', Float)
	maxmilespergalloninstantmpg = Column('maxmilespergalloninstantmpg', Float)
	avgmilespergalloninstantmpg = Column('avgmilespergalloninstantmpg', Float)
	minmilespergallonlongtermaveragempg = Column('minmilespergallonlongtermaveragempg', Float)
	maxmilespergallonlongtermaveragempg = Column('maxmilespergallonlongtermaveragempg', Float)
	avgmilespergallonlongtermaveragempg = Column('avgmilespergallonlongtermaveragempg', Float)
	maxspeedgpskmh = Column('maxspeedgpskmh', Float)
	avgspeedgpskmh = Column('avgspeedgpskmh', Float)
	maxspeedobdkmh = Column('maxspeedobdkmh', Float)
	avgspeedobdkmh = Column('avgspeedobdkmh', Float)
	mintorqueftlb = Column('mintorqueftlb', Float)
	maxtorqueftlb = Column('maxtorqueftlb', Float)
	avgtorqueftlb = Column('avgtorqueftlb', Float)
	mintripaveragekplkpl = Column('mintripaveragekplkpl', Float)
	maxtripaveragekplkpl = Column('maxtripaveragekplkpl', Float)
	avgtripaveragekplkpl = Column('avgtripaveragekplkpl', Float)
	mintripaveragelitres100kml100km = Column('mintripaveragelitres100kml100km', Float)
	maxtripaveragelitres100kml100km = Column('maxtripaveragelitres100kml100km', Float)
	avgtripaveragelitres100kml100km = Column('avgtripaveragelitres100kml100km', Float)
	mintripaveragempgmpg = Column('mintripaveragempgmpg', Float)
	maxtripaveragempgmpg = Column('maxtripaveragempgmpg', Float)
	avgtripaveragempgmpg = Column('avgtripaveragempgmpg', Float)
	mintripdistancekm = Column('mintripdistancekm', Float)
	maxtripdistancekm = Column('maxtripdistancekm', Float)
	avgtripdistancekm = Column('avgtripdistancekm', Float)
	mintripdistancestoredinvehicleprofilekm = Column('mintripdistancestoredinvehicleprofilekm', Float)
	maxtripdistancestoredinvehicleprofilekm = Column('maxtripdistancestoredinvehicleprofilekm', Float)
	avgtripdistancestoredinvehicleprofilekm = Column('avgtripdistancestoredinvehicleprofilekm', Float)
	mintriptimesincejourneystarts = Column('mintriptimesincejourneystarts', Float)
	maxtriptimesincejourneystarts = Column('maxtriptimesincejourneystarts', Float)
	avgtriptimesincejourneystarts = Column('avgtriptimesincejourneystarts', Float)
	mintriptimewhilstmovings = Column('mintriptimewhilstmovings', Float)
	maxtriptimewhilstmovings = Column('maxtriptimewhilstmovings', Float)
	avgtriptimewhilstmovings = Column('avgtriptimewhilstmovings', Float)
	mintriptimewhilststationarys = Column('mintriptimewhilststationarys', Float)
	maxtriptimewhilststationarys = Column('maxtriptimewhilststationarys', Float)
	avgtriptimewhilststationarys = Column('avgtriptimewhilststationarys', Float)
	minturboboostvacuumgaugebar = Column('minturboboostvacuumgaugebar', Float)
	maxturboboostvacuumgaugebar = Column('maxturboboostvacuumgaugebar', Float)
	avgturboboostvacuumgaugebar = Column('avgturboboostvacuumgaugebar', Float)
	minvoltageobdadapterv = Column('minvoltageobdadapterv', Float)
	maxvoltageobdadapterv = Column('maxvoltageobdadapterv', Float)
	avgvoltageobdadapterv = Column('avgvoltageobdadapterv', Float)
	minvolumetricefficiencycalculated = Column('minvolumetricefficiencycalculated', Float)
	maxvolumetricefficiencycalculated = Column('maxvolumetricefficiencycalculated', Float)
	avgvolumetricefficiencycalculated = Column('avgvolumetricefficiencycalculated', Float)
	minenginekwatthewheelskw = Column('minenginekwatthewheelskw', Float)
	maxenginekwatthewheelskw = Column('maxenginekwatthewheelskw', Float)
	avgenginekwatthewheelskw = Column('avgenginekwatthewheelskw', Float)
	minaveragetripspeedwhilstmovingonlykmh = Column('minaveragetripspeedwhilstmovingonlykmh', Float)
	maxaveragetripspeedwhilstmovingonlykmh = Column('maxaveragetripspeedwhilstmovingonlykmh', Float)
	avgaveragetripspeedwhilstmovingonlykmh = Column('avgaveragetripspeedwhilstmovingonlykmh', Float)
	mincostpermilekminstantkm = Column('mincostpermilekminstantkm', Float)
	maxcostpermilekminstantkm = Column('maxcostpermilekminstantkm', Float)
	avgcostpermilekminstantkm = Column('avgcostpermilekminstantkm', Float)
	mincostpermilekmtripkm = Column('mincostpermilekmtripkm', Float)
	maxcostpermilekmtripkm = Column('maxcostpermilekmtripkm', Float)
	avgcostpermilekmtripkm = Column('avgcostpermilekmtripkm', Float)
	minintakeairtemperaturef = Column('minintakeairtemperaturef', Float)
	maxintakeairtemperaturef = Column('maxintakeairtemperaturef', Float)
	avgintakeairtemperaturef = Column('avgintakeairtemperaturef', Float)
	minintakemanifoldpressurekpa = Column('minintakemanifoldpressurekpa', Float)
	maxintakemanifoldpressurekpa = Column('maxintakemanifoldpressurekpa', Float)
	avgintakemanifoldpressurekpa = Column('avgintakemanifoldpressurekpa', Float)

	def __repr__(self):
		return f'<Torqtrips id:{self.id} date:{self.tripdate} >'

def send_torq_trip(tf=None, tripdict=None, session=None, engine=None):
	# logger.debug(f'[stt] tf:{tf} `td:{tripdict} s:{session} e:{engine}')
	if not tripdict:
		logger.error(f'[stt] no tripdict tf={tf}')
		return None
	fileid = tf.id
	distance = tripdict['distance']
	fuelcost = tripdict['fuelcost']
	fuelused = tripdict['fuelused']
	distancewhilstconnectedtoobd = tripdict['distancewhilstconnectedtoobd']
	time = tripdict['time']
	tripdate = datetime.strptime(tripdict['tripdate'],'%Y-%m-%d %H:%M:%S')
	profile = tripdict['profile']
	csvfilename = str(tripdict['csvfilename'])
	csvhash = tripdict['csvhash']
	torqtrip = Torqtrips(fileid, csvfilename, csvhash, distance, fuelcost, fuelused, distancewhilstconnectedtoobd, tripdate, profile, time)
	session.add(torqtrip)
	session.commit()

def send_trip_profile(torqfile, session):
	filename = Path(torqfile.csvfilename)
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n').lower() for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = datetime.strptime(pdata_date ,'%a %b %d %H:%M:%S %Z%z %Y')
		except (OperationalError, Exception) as e:
			logger.error(f'[readsend] code={e.code} args={e.args[0]}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		fuelcost = float(trip_profile['fuelcost'])
		fuelused = float(trip_profile['fuelused'])
		distancewhilstconnectedtoobd = float(trip_profile['distancewhilstconnectedtoobd'])
		distance = float(trip_profile['distance'])
		triptime = float(trip_profile['time'])
		csvhash = md5(open(filename, 'rb').read()).hexdigest()
		profile = trip_profile['profile']
		# fileid, csvfilenae, csvhash, distance, fuelcost, fuelused, distancewhilstconnectedtoobd, tripdate, profile, time):
		tt = Torqtrips(fileid=torqfile.id, csvfilename=str(filename), csvhash=csvhash, distance=distance, fuelcost=fuelcost, fuelused=fuelused, distancewhilstconnectedtoobd=distancewhilstconnectedtoobd, tripdate=tripdate, profile=profile, triptime=triptime)
		#tt = Torqtrips(fileid=torqfile.id, csvfilename=str(torqfile.csvfilename), csvhash=torqfile.csvhash, distance=trip['distance'], fuelcost=trip['fuelcost'], fuelused=trip['fuelused'], distancewhilstconnectedtoobd=trip['distancewhilstconnectedtoobd'], tripdate=trip['tripdate'], profile=trip['profile'], triptime=trip['time'])
		session.add(tt)
		session.commit()
		return tt
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')

def get_trip_profile(filename):
	filename = Path(filename)
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n').lower() for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			if len(pdata_date) == 28:
				pdata_date = pdata_date.replace('GMT ','')
				tripdate = datetime.strptime(pdata_date ,'%a %b %d %H:%M:%S %Y')
			else:
				tripdate = datetime.strptime(pdata_date ,'%a %b %d %H:%M:%S %Z%z %Y')
			#tripdate = to_datetime(pdata_date)
			#tripdate = tripdate.strftime('%Y-%m-%d %H:%M:%S')
		# logger.info(f'[tripdate] {tripdate}')
		except (OperationalError, Exception, ValueError) as e:
			logger.error(f'[readsend] code={e} pdata_: {pdata_} filename: {filename}') #.code} args={e.args[0]}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		torq_trip = {}
		torq_trip['fuelcost'] = float(trip_profile['fuelcost'])
		torq_trip['fuelused'] = float(trip_profile['fuelused'])
		torq_trip['distancewhilstconnectedtoobd'] = float(trip_profile['distancewhilstconnectedtoobd'])
		torq_trip['distance'] = float(trip_profile['distance'])
		torq_trip['time'] = float(trip_profile['time'])
		torq_trip['filename'] = p_filename
		torq_trip['csvfilename'] = str(filename)
		torq_trip['csvhash'] = md5(open(filename, 'rb').read()).hexdigest()
		torq_trip['tripdate'] = tripdate
		torq_trip['profile'] = trip_profile['profile']
		return torq_trip
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')

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

def send_torqfiles(filelist=None, session=None): # returns list of new files
	#torqdbfiles = session.execute(text(f'select * from torqfiles;')).all()
	torqdbfiles = session.query(TorqFile).all() # get list of files from db
	hlist = [k.csvhash for k in torqdbfiles]
	newfiles = []
	for tf in filelist:
		csvfile = tf['csvfilename']
		csvhash = tf['csvhash']
		csvfilefixed = tf['csvfilefixed']
		fixedhash = tf['fixedhash']
		if csvhash in [k.csvhash for k in torqdbfiles]:
			logger.warning(f'[send_torqfiles] {csvfile} already in db')
		else:
			torqfile = TorqFile(str(csvfile), csvfilefixed, csvhash, fixedhash)
			session.add(torqfile)
			try:
				newfiles.append(torqfile)
			except ProgrammingError as e:
				logger.error(f'[send_torqfiles] {e}')
				session.rollback()
			except OperationalError as e:
				logger.error(f'[send_torqfiles] code={e.code} args={e.args[0]}')
				session.rollback()
	session.commit()
	# newfiles = [k for k in filelist if k['csvhash'] not in hlist]
	logger.info(f'[send_torqfiles] done sending newfilelist: {len(newfiles)}')
	return newfiles # return list of new files

def send_torqtrips(torqfile:TorqFile, session:sessionmaker):
	trip = get_trip_profile(torqfile.csvfilefixed)
	if trip:
		tt = Torqtrips(fileid=torqfile.id, csvfilename=str(torqfile.csvfilefixed), csvhash=torqfile.csvhash, distance=trip['distance'], fuelcost=trip['fuelcost'], fuelused=trip['fuelused'], distancewhilstconnectedtoobd=trip['distancewhilstconnectedtoobd'], tripdate=trip['tripdate'], profile=trip['profile'], triptime=trip['time'])
		session.add(tt)
		ntf = session.query(TorqFile).filter(TorqFile.id == torqfile.id).first()
		ntf.tripid = tt.id
		session.commit()
	else:
		logger.warning(f'[!] no trip profiles from {torqfile.csvfilefixed} ')
