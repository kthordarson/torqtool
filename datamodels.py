from pandas import DataFrame, concat, Series
from numpy import nan
from sqlalchemy import ForeignKey, create_engine, Table, MetaData, Column, Integer, String, inspect, select, BigInteger, Float, DateTime, text, BIGINT, Numeric
from sqlalchemy import DDL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from loguru import logger
import re
Base = declarative_base()


def attribute_factory():
    return dict(
        id=Column(Integer, primary_key=True),
        CLASS_VAR=12345678,
    )

class TorqTest(declarative_base()):
    __tablename__ = 'test'
    id = Column('id', Integer, primary_key=True,
                autoincrement="auto", unique=True)
    foobar = Column(String(255))

    def __repr__(self) -> str:
        return self.__tablename__

    def getfoobar(self):
        return self.foobar

    def setfoobar(self, foo):
        self.foobar = foo

    def getcols(self):
        return [k for k in self.__mapper__.tables[0].columns]

    def setcol(self, testcol):
        self.testcol = testcol

    def getcol(self, colname):
        return self.colname

# Table('torqfiles', MetaData(), 
# Column('torqfileid', Integer(), table=<torqfiles>, primary_key=True, nullable=False), 
# Column('tripid', Integer(), ForeignKey('torqtrips.tripid'), table=<torqfiles>), Column('torqfilename', String(length=255), table=<torqfiles>), Column('hash', String(length=255), table=<torqfiles>), Column('profile', String(length=255), table=<torqfiles>), Column('send_time', Numeric(), table=<torqfiles>), Column('read_time', Numeric(), table=<torqfiles>), Column('fix_time', Numeric(), table=<torqfiles>), schema=None)
class TorqFile(Base):
    __tablename__ = 'torqfiles'
    torqfileid = Column('torqfileid', Integer, primary_key=True,
                        autoincrement="auto", unique=False)
    tripid = Column(Integer, ForeignKey('torqtrips.tripid'))
    torqfilename = Column(String(255))
    hash = Column(String(255))
    profile = Column(String(255))
    send_time = Column(Numeric)
    read_time = Column(Numeric)
    fix_time = Column(Numeric)
    # torqlogentries = relationship("TorqEntry", back_populates='torqfile')

    def len(self):
        return 101


class TorqTrip(Base):
    __tablename__ = 'torqtrips'
    tripid = Column('tripid', Integer, primary_key=True,
                    autoincrement="auto", unique=False)
    filename = Column(String(255))
    fuelCost = Column(Float)
    fuelUsed = Column(Float)
    time = Column(Float)
    distanceWhilstConnectedToOBD = Column(Float)
    distance = Column(Float)
    profile = Column(String(255))
    tripdate = Column(DateTime, server_default=text('NOW()'))
    hash = Column(String(255))

    def len(self):
        return 101


class TorqLogEntry(Base):
    __tablename__ = 'torqlogentries'
    torqlog_entry = Column('torqlog_entry', Integer,
                           primary_key=True, autoincrement="auto", unique=False)
    # torqlogid =  Column('torqlogid', Integer, primary_key=True, autoincrement="auto")
    tripid = Column(Integer, ForeignKey('torqtrips.tripid'))
    torqfileid = Column(Integer, ForeignKey('torqfiles.torqfileid'))

    def len(self):
        return 101




class TorqEntry_foo(Base):
    __tablename__ = 'torqlogs'

    id = Column('id', Integer, primary_key=True,
                autoincrement="auto", unique=False)
    tripid = Column(Integer, ForeignKey('torqtrips.tripid'))
    torqtrip = relationship("TorqTrip")
    torqfileid = Column(Integer, ForeignKey('torqfiles.torqfileid'))
    torqfile = relationship("TorqFile")
    torqlog_entry = Column(Integer, ForeignKey('torqlogentries.torqlog_entry'))

    def __init__(self, **kw):
        pass
        # if len(kw) > 0:
        # 	for k in kw['buffer']:
        # 		#k1 = Column(name='')
        # 		#ktemp = DataFrame([kw['buffer'][k].values], index=False) #, kw['buffer'][k].values)
        # 		#logger.info(f'[setattr] {k} {type(ktemp)}')
        # 		c = kw['buffer'][k].items
        # 		# c.table = 'torqlogs'
        # 		try:
        # 			setattr(self, k, {c})
        # 		except (AttributeError, KeyError) as e:
        # 			logger.error(f'[setattr] err {e} k:{k} kw:{kw}')
        # 		logger.info(f'[setattr] {k} {len(kw)} {type(kw)} {type(k)}')
        # 	#setattr(self, k, kw[k])

    def setdata(self, buffer):
        for c in buffer:
            setattr(self, c, buffer[c])
            logger.info(f'[setattr] {c} {len(buffer)}')

# class TorqEntryold(Base):
# 	__tablename__ = 'torqlogs'
# 	id =  Column('id', Integer, primary_key=True, autoincrement="auto",unique=False)
# 	#idx =  Column('idx', Integer)
# 	tripid =  Column(Integer, ForeignKey('torqtrips.tripid'))
# 	torqtrip = relationship("TorqTrip")
# 	torqfileid = Column(Integer, ForeignKey('torqfiles.torqfileid'))
# 	torqfile = relationship("TorqFile")
# 	torqlog_entry = Column(Integer, ForeignKey('torqlogentries.torqlog_entry'))
# 	AccelerationSensorTotalg = Column(Float, default=0)
# 	AccelerationSensorXaxisg = Column(Float, default=0)
# 	AccelerationSensorYaxisg = Column(Float, default=0)
# 	AccelerationSensorZaxisg = Column(Float, default=0)
# 	Actualenginetorque = Column(Float, default=0)
# 	Altitudem = Column(Float, default=0)
# 	AndroiddeviceBatteryLevel = Column(Float, default=0)
# 	Averagetripspeedwhilstmovingonlykmh = Column(Float, default=0)
# 	Averagetripspeedwhilststoppedormovingkmh = Column(Float, default=0)
# 	Bearing = Column(Float, default=0)
# 	COingkmAveragegkm = Column(Float, default=0)
# 	COingkmInstantaneousgkm = Column(Float, default=0)
# 	CostpermilekmInstantkm = Column(Float, default=0)
# 	CostpermilekmTripkm = Column(Float, default=0)
# 	DeviceTime = Column(DateTime, server_default=text('NOW()')) # Column(String(255), default=0)
# 	DistancetoemptyEstimatedkm = Column(Float, default=0)
# 	DistancetravelledwithMILCELlitkm = Column(Float, default=0)
# 	EngineCoolantTemperatureF = Column(Float, default=0)
# 	EnginekWAtthewheelskW = Column(Float, default=0)
# 	EngineLoad = Column(Float, default=0)
# 	EngineRPMrpm = Column(Float, default=0)
# 	Fuelcosttripcost = Column(Float, default=0)
# 	Fuelflowratehourlhr = Column(Float, default=0)
# 	Fuelflowrateminuteccmin = Column(Float, default=0)
# 	Fuelpressurekpa = Column(Float, default=0)
# 	FuelRailPressurekpa = Column(Float, default=0)
# 	FuelRemainingCalculatedfromvehicleprofile = Column(Float, default=0)
# 	Fuelusedtripl = Column(Float, default=0)
# 	GPSAccuracym = Column(Float, default=0)
# 	GPSAltitudem = Column(Float, default=0)
# 	GPSBearing = Column(Float, default=0)
# 	GPSLatitude = Column(Float, default=0)
# 	GPSLongitude = Column(Float, default=0)
# 	GPSSatellites = Column(Float, default=0)
# 	GPSSpeedkmh = Column(Float, default=0)
# 	GPSTime = Column(String(255), server_default=text('NOW()'))
# 	GPSvsOBDSpeeddifferencekmh = Column(Float, default=0)
# 	GravityXG = Column(Float, default=0)
# 	GravityYG = Column(Float, default=0)
# 	GravityZG = Column(Float, default=0)
# 	HorizontalDilutionofPrecision = Column(Float, default=0)
# 	HorsepowerAtthewheelshp = Column(Float, default=0)
# 	IntakeAirTemperatureF = Column(Float, default=0)
# 	IntakeManifoldPressurekpa = Column(Float, default=0)
# 	KilometersPerLitreInstantkpl = Column(Float, default=0)
# 	KilometersPerLitreLongTermAveragekpl = Column(Float, default=0)
# 	Latitude = Column(Float, default=0)
# 	LitresPer100KilometerInstantl100km = Column(Float, default=0)
# 	LitresPer100KilometerLongTermAveragel100km = Column(Float, default=0)
# 	Longitude = Column(Float, default=0)
# 	MassAirFlowRategs = Column(Float, default=0)
# 	MilesPerGallonInstantmpg = Column(Float, default=0)
# 	MilesPerGallonLongTermAveragempg = Column(Float, default=0)
# 	SpeedGPSkmh = Column(Float, default=0)
# 	SpeedOBDkmh = Column(Float, default=0)
# 	Torqueftlb = Column(Float, default=0)
# 	TripaverageKPLkpl = Column(Float, default=0)
# 	TripaverageLitres100KMl100km = Column(Float, default=0)
# 	TripaverageMPGmpg = Column(Float, default=0)
# 	TripDistancekm = Column(Float, default=0)
# 	Tripdistancestoredinvehicleprofilekm = Column(Float, default=0)
# 	TripTimeSincejourneystarts = Column(Float, default=0)
# 	Triptimewhilstmovings = Column(Float, default=0)
# 	Triptimewhilststationarys = Column(Float, default=0)
# 	TurboBoostVacuumGaugebar = Column(Float, default=0)
# 	VoltageOBDAdapterV = Column(Float, default=0)
# 	VolumetricEfficiencyCalculated = Column(Float, default=0)

# 	def __init__(self,buffer):
# 		pass
# 	def len(self):
# 		return 101
