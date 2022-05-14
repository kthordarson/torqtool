from fileinput import filename
from pandas import DataFrame, concat
from numpy import nan
from sqlalchemy import ForeignKey, create_engine, Table, MetaData, Column, Integer, String, inspect, select, BigInteger, Numeric, DateTime, text, BIGINT,  Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from loguru import logger
import re
Base = declarative_base()


class TorqEntry(Base):
	__tablename__ = 'torqlogs'
	id =  Column(Integer, primary_key=True)
	file_id = Column(Integer, ForeignKey('torqfiles.id', ondelete='NO ACTION'))
	torqfile = relationship('TorqFile')
	filename = Column(String(255), default='')
	hash = Column(String(255))
	tripid =  Column(String(255))
	profile =  Column(String(255))
	AccelerationSensorTotalg = Column(Float, default=0)
	AccelerationSensorXaxisg = Column(Float, default=0)
	AccelerationSensorYaxisg = Column(Float, default=0)
	AccelerationSensorZaxisg = Column(Float, default=0)
	Actualenginetorque = Column(Numeric, default=0)
	Altitudem = Column(Numeric, default=0)
	AndroiddeviceBatteryLevel = Column(Numeric, default=0)
	Averagetripspeedwhilstmovingonlykmh = Column(Numeric, default=0)
	Averagetripspeedwhilststoppedormovingkmh = Column(Numeric, default=0)
	Bearing = Column(Numeric, default=0)
	COingkmAveragegkm = Column(Numeric, default=0)
	COingkmInstantaneousgkm = Column(Numeric, default=0)
	CostpermilekmInstantkm = Column(Numeric, default=0)
	CostpermilekmTripkm = Column(Numeric, default=0)
	DeviceTime = Column(DateTime, server_default=text('NOW()')) # Column(String(255), default=0)
	DistancetoemptyEstimatedkm = Column(Numeric, default=0)
	DistancetravelledwithMILCELlitkm = Column(Float, default=0)
	EngineCoolantTemperatureF = Column(Float, default=0)
	EnginekWAtthewheelskW = Column(Numeric, default=0)
	EngineLoad = Column(Numeric, default=0)
	EngineRPMrpm = Column(Numeric, default=0)
	Fuelcosttripcost = Column(Numeric, default=0)
	Fuelflowratehourlhr = Column(Numeric, default=0)
	Fuelflowrateminuteccmin = Column(Numeric, default=0)
	Fuelpressurekpa = Column(Numeric, default=0)
	FuelRailPressurekpa = Column(Numeric, default=0)
	FuelRemainingCalculatedfromvehicleprofile = Column(Numeric, default=0)
	Fuelusedtripl = Column(Numeric, default=0)
	GPSAccuracym = Column(Numeric, default=0)
	GPSAltitudem = Column(Numeric, default=0)
	GPSBearing = Column(Numeric, default=0)
	GPSLatitude = Column(Numeric, default=0)
	GPSLongitude = Column(Numeric, default=0)
	GPSSatellites = Column(Numeric, default=0)
	GPSSpeedkmh = Column(Numeric, default=0)
	GPSTime = Column(DateTime, server_default=text('NOW()'))
	GPSvsOBDSpeeddifferencekmh = Column(Numeric, default=0)
	GravityXG = Column(Numeric, default=0)
	GravityYG = Column(Numeric, default=0)
	GravityZG = Column(Numeric, default=0)
	HorizontalDilutionofPrecision = Column(Numeric, default=0)
	HorsepowerAtthewheelshp = Column(Numeric, default=0)
	IntakeAirTemperatureF = Column(Numeric, default=0)
	IntakeManifoldPressurekpa = Column(Float, default=0)
	KilometersPerLitreInstantkpl = Column(Numeric, default=0)
	KilometersPerLitreLongTermAveragekpl = Column(Numeric, default=0)
	Latitude = Column(Numeric, default=0)
	LitresPer100KilometerInstantl100km = Column(Numeric, default=0)
	LitresPer100KilometerLongTermAveragel100km = Column(Numeric, default=0)
	Longitude = Column(Numeric, default=0)
	MassAirFlowRategs = Column(Numeric, default=0)
	MilesPerGallonInstantmpg = Column(Numeric, default=0)
	MilesPerGallonLongTermAveragempg = Column(Numeric, default=0)
	SpeedGPSkmh = Column(Numeric, default=0)
	SpeedOBDkmh = Column(Numeric, default=0)
	Torqueftlb = Column(Numeric, default=0)
	TripaverageKPLkpl = Column(Numeric, default=0)
	TripaverageLitres100KMl100km = Column(Numeric, default=0)
	TripaverageMPGmpg = Column(Numeric, default=0)
	TripDistancekm = Column(Numeric, default=0)
	Tripdistancestoredinvehicleprofilekm = Column(Numeric, default=0)
	TripTimeSincejourneystarts = Column(Numeric, default=0)
	Triptimewhilstmovings = Column(Numeric, default=0)
	Triptimewhilststationarys = Column(Numeric, default=0)
	TurboBoostVacuumGaugebar = Column(Numeric, default=0)
	VoltageOBDAdapterV = Column(Numeric, default=0)
	VolumetricEfficiencyCalculated = Column(Numeric, default=0)

	def __init__(self):
		# self.filename = str(filename)
		# self.tripid = tripid
		# self.hash = csvhash
		self.buffer = DataFrame()
		
	
	def set_data(self, buffer=None, metadata=None, profile=None, csvfile=None, csvhash=None, tripid=None):
		self.filename = DataFrame(data=[str(csvfile)])
		self.filename.replace(nan, str(csvfile), inplace=True)
		buffer['filename'] = self.filename
		self.hash = DataFrame(data=[csvhash])
		self.hash.replace(nan, csvhash, inplace=True)
		buffer['hash'] = self.hash
		self.tripid = DataFrame(data=[tripid])
		self.tripid.replace(nan, tripid, inplace=True)
		buffer['tripid'] = self.tripid
		self.profile = DataFrame(data=[[profile]])
		self.profile.replace(nan, profile, inplace=True)
		buffer['profile'] = self.profile
		# logger.info(f'[sd] tb:{type(buffer)} {self.filename} {self.hash} {self.tripid} {self.profile}')
		for col in buffer.columns:
			newname_ = re.sub('\W', '', col) #.encode('ascii', 'ignore')
			newname = newname_.encode('ascii','ignore').decode()
			buffer.rename(columns={col:newname}, inplace=True)
			tempbuffer = buffer[newname].replace(nan, 0) # .transpose()
			buffer[newname] = tempbuffer.transpose()
			# tempbuffer = tempbuffer.transpose()
			fields = [k.name for k in self.__table__.columns]
			self.__dict__[newname] = tempbuffer
			try:
				eval(f"self.{newname}")
				#logger.info(f'[eval] {newname} {eval(f"self.{newname}")}')				
			except AttributeError as e:
				logger.error(f'{newname}')
		self.buffer = buffer



class TorqFile(Base):
	__tablename__ = 'torqfiles'
	id =  Column(Integer, primary_key=True, autoincrement="auto")
	filename = Column(String(255))
	hash = Column(String(255))
	tripid =  Column(String(255))
	profile = Column(String(255))
	def __init__(self, filename, hash, tripid, profile):
		self.filename = filename
		self.hash = hash
		self.tripid = tripid
		self.profile = profile
	# trip = relationship('TripProfile', back_populates='torqfiles')
	# trip = relationship('TripProfile')

	# def __str__(self):
	# 	return(f'[torqfile] {self.name}')

	# def __repr__(self):
	# 	return(f'{self.name}')


class TorqProfile(Base):
	__tablename__ = 'torqtrips'
	id =  Column(BigInteger, primary_key=True)
	
	filename = Column(String(255))
	fuelCost = Column(Float)
	fuelUsed = Column(Float)
	time = Column(Float)
	distanceWhilstConnectedToOBD = Column(Float)
	distance = Column(Float)
	profile = Column(String(255))
	tripdate = Column(DateTime, server_default=text('NOW()'))
	hash = Column(String(255))
	tripid = Column(String(255))
	# file_id = Column(Integer, ForeignKey("torqfile.id"))
	# torqfile = relationship("Torqfile", foreign_keys=[file_id])
	#torqfile = relationship("Torqfile", back_populates='torqtrips')
	# torqfile = relationship("Torqfile")

	def __init__(self, filename=None):
		self.filename = filename

