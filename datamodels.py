import sys
import os
import uuid
from datetime import datetime
from hashlib import md5
from pathlib import Path
import pandas as pd
from loguru import logger
from sqlalchemy import (BigInteger, Column, DateTime, Float, ForeignKey, Integer, String, Table, inspect, select, text)
from sqlalchemy.exc import (OperationalError, ProgrammingError)
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, sessionmaker)
from sqlalchemy.orm import Session
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
	csvfile = Column('csvfile', String(255))
	csvhash = Column('csvhash', String(255))
	read_flag = Column('read_flag', Integer)
	send_flag = Column('send_flag', Integer)
	fixed_flag = Column('fixed_flag', Integer)

	def __init__(self, csvfile, csvhash):
		self.csvfile = csvfile
		self.csvhash = csvhash
		self.read_flag = 0
		self.send_flag = 0
		self.fixed_flag = 0

	def __repr__(self):
		fn = self.csvfile.split('/')[-1]
		return f'<TorqFile id:{self.id} {fn} r:{self.read_flag} s:{self.send_flag} f:{self.fixed_flag} >'

class Torqtrips(Base):
	__tablename__ = 'torqtrips'
	id: Mapped[int] = mapped_column(primary_key=True)
	fileid: Mapped[int] = mapped_column(ForeignKey('torqfiles.id'))
	csvfile = Column('csvfile', String(255))
	csvhash = Column('csvhash', String(255))
	distance = Column('distance', BigInteger)
	fuelcost = Column('fuelcost', BigInteger)
	fuelused = Column('fuelused', BigInteger)
	distancewhilstconnectedtoobd = Column('distancewhilstconnectedtoobd', BigInteger)
	tripdate = Column('tripdate', DateTime)
	profile = Column('profile', String(255))
	time = Column('time', BigInteger)
	#tt = Torqtrips(fileid=torqfile.id, csvfile=str(torqfile.csvfile), csvhash=torqfile.csvhash, distance=trip['distance'], fuelcost=trip['fuelcost'], fuelused=trip['fuelused'], distancewhilstconnectedtoobd=trip['distancewhilstconnectedtoobd'], tripdate=trip['tripdate'], profile=trip['profile'], triptime=trip['time'])
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
	def __repr__(self):
		return f'<Torqtrips id:{self.id} file:{self.fileid} {self.csvfile}>'

class Torqlogs(Base):
	__tablename__ = 'torqlogs'
	id: Mapped[int] = mapped_column(primary_key=True)
	tripid = Column('tripid', Integer) #Mapped[int] = mapped_column(ForeignKey('torqtrips.id'))
	fileid = Column('fileid', Integer) #Mapped[int] = mapped_column(ForeignKey('torqfiles.id'))
	gpstime = Column('gpstime', DateTime)
	devicetime = Column('devicetime', DateTime)
	longitude = Column('longitude', Float)
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
	triptimesincejourneystarts = Column('triptimesincejourneystarts', DateTime)
	triptimewhilstmovings = Column('triptimewhilstmovings', DateTime)
	triptimewhilststationarys = Column('triptimewhilststationarys', DateTime)
	gpsspeedkmh = Column('gpsspeedkmh', Float)
	altitudem = Column('altitudem', Float)
	gravityxg = Column('gravityxg', Float)
	gravityyg = Column('gravityyg', Float)
	gravityzg = Column('gravityzg', Float)
	enginecoolanttemperaturef = Column('enginecoolanttemperaturef', Float)
	fuelrailpressurekpa = Column('fuelrailpressurekpa', Float)
	intakeairtemperaturef = Column('intakeairtemperaturef', Float)
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
	voltagecontrolmodulev = Column('voltagecontrolmodulev', Float)
	costpermilekminstantkm = Column('costpermilekminstantkm', Float)
	costpermilekmtripkm = Column('costpermilekmtripkm', Float)
	gpsspeedmeterssecond = Column('gpsspeedmeterssecond', Float)
	altitude = Column('altitude', Float)
	gx = Column('gx', Float)
	gy = Column('gy', Float)
	gz = Column('gz', Float)
	gcalibrated = Column('gcalibrated', Float)
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
		logger.debug(f'filelist: {len(filelist)} dbfiles: {len(torqdbfiles)}  hashes: {len(hlist)}')
	newfiles = []
	for idx,tf in enumerate(filelist):
		csvfile = tf['csvfile']
		csvhash = tf['csvhash']
		if csvhash in hlist.values: #[k.csvhash for k in torqdbfiles]:
			if debug:
				logger.warning(f'[st {idx}/{len(filelist)}] {csvfile} already in db') # {tf}')
		else:
			if debug:
				logger.info(f'[st {idx}/{len(filelist)}] {csvfile} not in db {tf}')
			torqfile = TorqFile(str(csvfile),  csvhash)
			torqfile.send_flag = 1
			session.add(torqfile)
			try:
				newfiles.append(torqfile)
			except ProgrammingError as e:
				logger.error(f'[st {idx}/{len(filelist)}] {e}')
				session.rollback()
			except OperationalError as e:
				logger.error(f'[st {idx}/{len(filelist)}] code={e} args={e.args[0]}')
				session.rollback()
	session.commit()
	# newfiles = [k for k in filelist if k['csvhash'] not in hlist]
	logger.info(f'[st] done sending newfilelist: {len(newfiles)}')
	return newfiles # return list of new files

