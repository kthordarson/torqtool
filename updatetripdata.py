# todo fix [IntegrityError] trip:63 gkpj pymysql.err.IntegrityError 1452 Cannot add or update a child row: a foreign key constraint fails (`torq`.`torqdata`, CONSTRAINT `torqdata_ibfk_1` FOREIGN KEY (`id`) REFERENCES `torqlogs` (`tripid`))') tripdate=2022-12-20 18:03:04
# todo fix only create tripdata for new trips

from hashlib import md5

from datetime import datetime
import polars as pl
import pandas as pd
from loguru import logger
from pandas import DataFrame
from sqlalchemy import (BIGINT, BigInteger, Column, DateTime, Float, Integer, MetaData, Numeric, String, Table, create_engine, inspect, select, text)
from sqlalchemy.exc import (IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect

from datamodels import TorqFile

mysql_cmds = {
'createfiles' : """
					create table if not exists torqfiles
					(
						id int primary key not null auto_increment,
						csvfile  varchar(255),
						csvhash varchar(255)
					);
 """,
'createlogs' : """
				create table if not exists torqlogs(
					id int primary key not null auto_increment,
					tripid int not null,
					gpstime timestamp default current_timestamp,
					devicetime timestamp default current_timestamp,
					longitude double default 0,
					latitude double default 0,
					gpsspeedkmh double default 0,
					horizontaldilutionofprecision double default 0,
					altitudem double default 0,
					bearing double default 0,
					gravityxg double default 0,
					gravityyg double default 0,
					gravityzg double default 0,
					accelerationsensortotalg double default 0,
					accelerationsensorxaxisg double default 0,
					accelerationsensoryaxisg double default 0,
					accelerationsensorzaxisg double default 0,
					actualenginetorque double default 0,
					androiddevicebatterylevel double default 0,
					averagetripspeedwhilststoppedormovingkmh double default 0,
					coingkmaveragegkm double default 0,
					coingkminstantaneousgkm double default 0,
					distancetoemptyestimatedkm double default 0,
					distancetravelledwithmilcellitkm double default 0,
					enginecoolanttemperaturef double default 0,
					engineload double default 0,
					enginerpmrpm double default 0,
					fuelcosttripcost double default 0,
					fuelflowratehourlhr double default 0,
					fuelflowrateminuteccmin double default 0,
					fuelrailpressurekpa double default 0,
					fuelremainingcalculatedfromvehicleprofile double default 0,
					fuelusedtripl double default 0,
					fuelpressurekpa double default 0,
					gpsaccuracym double default 0,
					gpsaltitudem double default 0,
					gpsbearing double default 0,
					gpslatitude double default 0,
					gpslongitude double default 0,
					gpssatellites double default 0,
					gpsvsobdspeeddifferencekmh double default 0,
					horsepoweratthewheelshp double default 0,
					kilometersperlitreinstantkpl double default 0,
					kilometersperlitrelongtermaveragekpl double default 0,
					litresper100kilometerinstantl100km double default 0,
					litresper100kilometerlongtermaveragel100km double default 0,
					massairflowrategs double default 0,
					milespergalloninstantmpg double default 0,
					milespergallonlongtermaveragempg double default 0,
					speedgpskmh double default 0,
					speedobdkmh double default 0,
					torqueftlb double default 0,
					tripaveragekplkpl double default 0,
					tripaveragelitres100kml100km double default 0,
					tripaveragempgmpg double default 0,
					tripdistancekm double default 0,
					tripdistancestoredinvehicleprofilekm double default 0,
					triptimesincejourneystarts double default 0,
					triptimewhilstmovings double default 0,
					triptimewhilststationarys double default 0,
					turboboostvacuumgaugebar double default 0,
					voltageobdadapterv double default 0,
					volumetricefficiencycalculated double default 0,
					enginekwatthewheelskw double default 0,
					averagetripspeedwhilstmovingonlykmh double default 0,
					costpermilekminstantkm double default 0,
					costpermilekmtripkm double default 0,
					intakeairtemperaturef double default 0,
					intakemanifoldpressurekpa double default 0,
					key torqlogs_fk(tripid),
					foreign key(tripid) references torqfiles(id));

 """,
'createtrips': """
				create table if not exists torqtrips
				(
					id int primary key not null auto_increment,
					csvfile varchar(255),
					csvhash varchar(255),
					distance double,
					fuelcost double,
					fuelused double,
					distancewhilstconnectedtoobd double,
					tripdate timestamp,
					profile varchar(255),
					time double,
					foreign key (id) references torqfiles (id),
					foreign key (id) references torqlogs (tripid)  );
					""",
'createdata': """
					create table if not exists torqdata (
					id int primary key not null auto_increment,
					tripdate timestamp,
					distance double default 0,
					fuelused double default 0,
					fuelcost double default 0,
					time double default 0,
					distancewhilstconnectedtoobd double default 0,
					minlongitude double default 0,
					maxlongitude double default 0,
					avglongitude double default 0,
					minlatitude double default 0,
					maxlatitude double default 0,
					avglatitude double default 0,
					mingpsspeedkmh double default 0,
					maxgpsspeedkmh double default 0,
					avggpsspeedkmh double default 0,
					minhorizontaldilutionofprecision double default 0,
					maxhorizontaldilutionofprecision double default 0,
					avghorizontaldilutionofprecision double default 0,
					minaltitudem double default 0,
					maxaltitudem double default 0,
					avgaltitudem double default 0,
					minbearing double default 0,
					maxbearing double default 0,
					avgbearing double default 0,
					mingravityxg double default 0,
					maxgravityxg double default 0,
					avggravityxg double default 0,
					mingravityyg double default 0,
					maxgravityyg double default 0,
					avggravityyg double default 0,
					mingravityzg double default 0,
					maxgravityzg double default 0,
					avggravityzg double default 0,
					minaccelerationsensortotalg double default 0,
					maxaccelerationsensortotalg double default 0,
					avgaccelerationsensortotalg double default 0,
					minaccelerationsensorxaxisg double default 0,
					maxaccelerationsensorxaxisg double default 0,
					avgaccelerationsensorxaxisg double default 0,
					minaccelerationsensoryaxisg double default 0,
					maxaccelerationsensoryaxisg double default 0,
					avgaccelerationsensoryaxisg double default 0,
					minaccelerationsensorzaxisg double default 0,
					maxaccelerationsensorzaxisg double default 0,
					avgaccelerationsensorzaxisg double default 0,
					minactualenginetorque double default 0,
					maxactualenginetorque double default 0,
					avgactualenginetorque double default 0,
					minandroiddevicebatterylevel double default 0,
					maxandroiddevicebatterylevel double default 0,
					avgandroiddevicebatterylevel double default 0,
					minaveragetripspeedwhilststoppedormovingkmh double default 0,
					maxaveragetripspeedwhilststoppedormovingkmh double default 0,
					avgaveragetripspeedwhilststoppedormovingkmh double default 0,
					mincoingkmaveragegkm double default 0,
					maxcoingkmaveragegkm double default 0,
					avgcoingkmaveragegkm double default 0,
					mincoingkminstantaneousgkm double default 0,
					maxcoingkminstantaneousgkm double default 0,
					avgcoingkminstantaneousgkm double default 0,
					mindistancetoemptyestimatedkm double default 0,
					maxdistancetoemptyestimatedkm double default 0,
					avgdistancetoemptyestimatedkm double default 0,
					mindistancetravelledwithmilcellitkm double default 0,
					maxdistancetravelledwithmilcellitkm double default 0,
					avgdistancetravelledwithmilcellitkm double default 0,
					minenginecoolanttemperaturef double default 0,
					maxenginecoolanttemperaturef double default 0,
					avgenginecoolanttemperaturef double default 0,
					minengineload double default 0,
					maxengineload double default 0,
					avgengineload double default 0,
					minenginerpmrpm double default 0,
					maxenginerpmrpm double default 0,
					avgenginerpmrpm double default 0,
					minfuelcosttripcost double default 0,
					maxfuelcosttripcost double default 0,
					avgfuelcosttripcost double default 0,
					minfuelflowratehourlhr double default 0,
					maxfuelflowratehourlhr double default 0,
					avgfuelflowratehourlhr double default 0,
					minfuelflowrateminuteccmin double default 0,
					maxfuelflowrateminuteccmin double default 0,
					avgfuelflowrateminuteccmin double default 0,
					minfuelrailpressurekpa double default 0,
					maxfuelrailpressurekpa double default 0,
					avgfuelrailpressurekpa double default 0,
					minfuelremainingcalculatedfromvehicleprofile double default 0,
					maxfuelremainingcalculatedfromvehicleprofile double default 0,
					avgfuelremainingcalculatedfromvehicleprofile double default 0,
					minfuelusedtripl double default 0,
					maxfuelusedtripl double default 0,
					avgfuelusedtripl double default 0,
					mingpsaccuracym double default 0,
					maxgpsaccuracym double default 0,
					avggpsaccuracym double default 0,
					mingpsaltitudem double default 0,
					maxgpsaltitudem double default 0,
					avggpsaltitudem double default 0,
					mingpsbearing double default 0,
					maxgpsbearing double default 0,
					avggpsbearing double default 0,
					mingpslatitude double default 0,
					maxgpslatitude double default 0,
					avggpslatitude double default 0,
					mingpslongitude double default 0,
					maxgpslongitude double default 0,
					avggpslongitude double default 0,
					mingpssatellites double default 0,
					maxgpssatellites double default 0,
					avggpssatellites double default 0,
					mingpsvsobdspeeddifferencekmh double default 0,
					maxgpsvsobdspeeddifferencekmh double default 0,
					avggpsvsobdspeeddifferencekmh double default 0,
					minhorsepoweratthewheelshp double default 0,
					maxhorsepoweratthewheelshp double default 0,
					avghorsepoweratthewheelshp double default 0,
					minkilometersperlitrelongtermaveragekpl double default 0,
					maxkilometersperlitrelongtermaveragekpl double default 0,
					avgkilometersperlitrelongtermaveragekpl double default 0,
					minlitresper100kilometerinstantl100km double default 0,
					maxlitresper100kilometerinstantl100km double default 0,
					avglitresper100kilometerinstantl100km double default 0,
					minlitresper100kilometerlongtermaveragel100km double default 0,
					maxlitresper100kilometerlongtermaveragel100km double default 0,
					avglitresper100kilometerlongtermaveragel100km double default 0,
					minmassairflowrategs double default 0,
					maxmassairflowrategs double default 0,
					avgmassairflowrategs double default 0,
					minmilespergalloninstantmpg double default 0,
					maxmilespergalloninstantmpg double default 0,
					avgmilespergalloninstantmpg double default 0,
					minmilespergallonlongtermaveragempg double default 0,
					maxmilespergallonlongtermaveragempg double default 0,
					avgmilespergallonlongtermaveragempg double default 0,
					maxspeedgpskmh double default 0,
					avgspeedgpskmh double default 0,
					maxspeedobdkmh double default 0,
					avgspeedobdkmh double default 0,
					mintorqueftlb double default 0,
					maxtorqueftlb double default 0,
					avgtorqueftlb double default 0,
					mintripaveragekplkpl double default 0,
					maxtripaveragekplkpl double default 0,
					avgtripaveragekplkpl double default 0,
					mintripaveragelitres100kml100km double default 0,
					maxtripaveragelitres100kml100km double default 0,
					avgtripaveragelitres100kml100km double default 0,
					mintripaveragempgmpg double default 0,
					maxtripaveragempgmpg double default 0,
					avgtripaveragempgmpg double default 0,
					mintripdistancekm double default 0,
					maxtripdistancekm double default 0,
					avgtripdistancekm double default 0,
					mintripdistancestoredinvehicleprofilekm double default 0,
					maxtripdistancestoredinvehicleprofilekm double default 0,
					avgtripdistancestoredinvehicleprofilekm double default 0,
					mintriptimesincejourneystarts double default 0,
					maxtriptimesincejourneystarts double default 0,
					avgtriptimesincejourneystarts double default 0,
					mintriptimewhilstmovings double default 0,
					maxtriptimewhilstmovings double default 0,
					avgtriptimewhilstmovings double default 0,
					mintriptimewhilststationarys double default 0,
					maxtriptimewhilststationarys double default 0,
					avgtriptimewhilststationarys double default 0,
					minturboboostvacuumgaugebar double default 0,
					maxturboboostvacuumgaugebar double default 0,
					avgturboboostvacuumgaugebar double default 0,
					minvoltageobdadapterv double default 0,
					maxvoltageobdadapterv double default 0,
					avgvoltageobdadapterv double default 0,
					minvolumetricefficiencycalculated double default 0,
					maxvolumetricefficiencycalculated double default 0,
					avgvolumetricefficiencycalculated double default 0,
					minenginekwatthewheelskw text collate utf8mb4_unicode_ci default 0,
					maxenginekwatthewheelskw text collate utf8mb4_unicode_ci default 0,
					avgenginekwatthewheelskw text collate utf8mb4_unicode_ci default 0,
					minaveragetripspeedwhilstmovingonlykmh text collate utf8mb4_unicode_ci default 0,
					maxaveragetripspeedwhilstmovingonlykmh text collate utf8mb4_unicode_ci default 0,
					avgaveragetripspeedwhilstmovingonlykmh text collate utf8mb4_unicode_ci default 0,
					mincostpermilekminstantkm text collate utf8mb4_unicode_ci default 0,
					maxcostpermilekminstantkm text collate utf8mb4_unicode_ci default 0,
					avgcostpermilekminstantkm text collate utf8mb4_unicode_ci default 0,
					mincostpermilekmtripkm text collate utf8mb4_unicode_ci default 0,
					maxcostpermilekmtripkm text collate utf8mb4_unicode_ci default 0,
					avgcostpermilekmtripkm text collate utf8mb4_unicode_ci default 0,
					minintakeairtemperaturef double default 0,
					maxintakeairtemperaturef double default 0,
					avgintakeairtemperaturef double default 0,
					minintakemanifoldpressurekpa double default 0,
					maxintakemanifoldpressurekpa double default 0,
					avgintakemanifoldpressurekpa double default 0,
					key torqdata_fk (id),
					foreign key (id) references torqlogs (tripid),
					foreign key (id) references torqfiles (id));

	  """
}

sqlite_cmds = {
'createfiles' : """
	create table if not exists torqfiles
	(
	id           integer primary key,
	csvfile  varchar(255),
	csvhash      varchar(255)
	);
""",
'createtrips' : """
	create table if not exists torqtrips	(
	id int integer primary key,
	csvfile varchar(255),
	csvhash varchar(255),
	distance int,
	fuelcost int,
	fuelused int,
	distancewhilstconnectedtoobd int,
	tripdate datetime,
	profile varchar(255),
	time int
	);
	""",
'createlogs' : """
	create table if not exists torqlogs
	(
		id integer primary key,
		tripid int ,
		gpstime timestamp default current_timestamp,
		devicetime timestamp default current_timestamp,
		longitude float default 0,
		latitude float default 0,
		gpsspeedkmh float default 0,
		horizontaldilutionofprecision float default 0,
		altitudem float default 0,
		bearing float default 0,
		gravityxg float default 0,
		gravityyg float default 0,
		gravityzg float default 0,
		accelerationsensortotalg float default 0,
		accelerationsensorxaxisg float default 0,
		accelerationsensoryaxisg float default 0,
		accelerationsensorzaxisg float default 0,
		actualenginetorque float default 0,
		androiddevicebatterylevel float default 0,
		averagetripspeedwhilststoppedormovingkmh float default 0,
		coingkmaveragegkm float default 0,
		coingkminstantaneousgkm float default 0,
		distancetoemptyestimatedkm float default 0,
		distancetravelledwithmilcellitkm float default 0,
		enginecoolanttemperaturef float default 0,
		engineload float default 0,
		enginerpmrpm float default 0,
		fuelcosttripcost float default 0,
		fuelflowratehourlhr float default 0,
		fuelflowrateminuteccmin float default 0,
		fuelrailpressurekpa float default 0,
		fuelremainingcalculatedfromvehicleprofile float default 0,
		fuelusedtripl float default 0,
		gpsaccuracym float default 0,
		gpsaltitudem float default 0,
		gpsbearing float default 0,
		gpslatitude float default 0,
		gpslongitude float default 0,
		gpssatellites float default 0,
		gpsvsobdspeeddifferencekmh float default 0,
		horsepoweratthewheelshp float default 0,
		floatakeairtemperaturef float default 0,
		floatakemanifoldpressurekpa float default 0,
		kilometersperlitreinstantkpl float default 0,
		kilometersperlitrelongtermaveragekpl float default 0,
		litresper100kilometerinstantl100km float default 0,
		litresper100kilometerlongtermaveragel100km float default 0,
		massairflowrategs float default 0,
		milespergalloninstantmpg float default 0,
		milespergallonlongtermaveragempg float default 0,
		speedgpskmh float default 0,
		speedobdkmh float default 0,
		torqueftlb float default 0,
		tripaveragekplkpl float default 0,
		tripaveragelitres100kml100km float default 0,
		tripaveragempgmpg float default 0,
		tripdistancekm float default 0,
		tripdistancestoredinvehicleprofilekm float default 0,
		triptimesincejourneystarts float default 0,
		triptimewhilstmovings float default 0,
		triptimewhilststationarys float default 0,
		turboboostvacuumgaugebar float default 0,
		voltageobdadapterv float default 0,
		volumetricefficiencycalculated float default 0,
		enginekwatthewheelskw float default 0,
		averagetripspeedwhilstmovingonlykmh float default 0,
		costpermilekminstantkm float default 0,
		costpermilekmtripkm float default 0,
		intakeairtemperaturef float default 0,
		intakemanifoldpressurekpa float default 0
	);
""",
'createdata': """
		create table if not exists torqdata (
		id int primary key not null,
		tripid int,
		tripdate datetime default 0,
		distance double default 0,
		fuelused double default 0,
		fuelcost double default 0,
		time double default 0,
		distancewhilstconnectedtoobd double default 0,
		minlongitude double default 0,
		maxlongitude double default 0,
		avglongitude double default 0,
		minlatitude double default 0,
		maxlatitude double default 0,
		avglatitude double default 0,
		mingpsspeedkmh double default 0,
		maxgpsspeedkmh double default 0,
		avggpsspeedkmh double default 0,
		minhorizontaldilutionofprecision double default 0,
		maxhorizontaldilutionofprecision double default 0,
		avghorizontaldilutionofprecision double default 0,
		minaltitudem double default 0,
		maxaltitudem double default 0,
		avgaltitudem double default 0,
		minbearing double default 0,
		maxbearing double default 0,
		avgbearing double default 0,
		mingravityxg double default 0,
		maxgravityxg double default 0,
		avggravityxg double default 0,
		mingravityyg double default 0,
		maxgravityyg double default 0,
		avggravityyg double default 0,
		mingravityzg double default 0,
		maxgravityzg double default 0,
		avggravityzg double default 0,
		minaccelerationsensortotalg double default 0,
		maxaccelerationsensortotalg double default 0,
		avgaccelerationsensortotalg double default 0,
		minaccelerationsensorxaxisg double default 0,
		maxaccelerationsensorxaxisg double default 0,
		avgaccelerationsensorxaxisg double default 0,
		minaccelerationsensoryaxisg double default 0,
		maxaccelerationsensoryaxisg double default 0,
		avgaccelerationsensoryaxisg double default 0,
		minaccelerationsensorzaxisg double default 0,
		maxaccelerationsensorzaxisg double default 0,
		avgaccelerationsensorzaxisg double default 0,
		minactualenginetorque double default 0,
		maxactualenginetorque double default 0,
		avgactualenginetorque double default 0,
		minandroiddevicebatterylevel double default 0,
		maxandroiddevicebatterylevel double default 0,
		avgandroiddevicebatterylevel double default 0,
		minaveragetripspeedwhilststoppedormovingkmh double default 0,
		maxaveragetripspeedwhilststoppedormovingkmh double default 0,
		avgaveragetripspeedwhilststoppedormovingkmh double default 0,
		mincoingkmaveragegkm double default 0,
		maxcoingkmaveragegkm double default 0,
		avgcoingkmaveragegkm double default 0,
		mincoingkminstantaneousgkm double default 0,
		maxcoingkminstantaneousgkm double default 0,
		avgcoingkminstantaneousgkm double default 0,
		mindistancetoemptyestimatedkm double default 0,
		maxdistancetoemptyestimatedkm double default 0,
		avgdistancetoemptyestimatedkm double default 0,
		mindistancetravelledwithmilcellitkm double default 0,
		maxdistancetravelledwithmilcellitkm double default 0,
		avgdistancetravelledwithmilcellitkm double default 0,
		minenginecoolanttemperaturef double default 0,
		maxenginecoolanttemperaturef double default 0,
		avgenginecoolanttemperaturef double default 0,
		minengineload double default 0,
		maxengineload double default 0,
		avgengineload double default 0,
		minenginerpmrpm double default 0,
		maxenginerpmrpm double default 0,
		avgenginerpmrpm double default 0,
		minfuelcosttripcost double default 0,
		maxfuelcosttripcost double default 0,
		avgfuelcosttripcost double default 0,
		minfuelflowratehourlhr double default 0,
		maxfuelflowratehourlhr double default 0,
		avgfuelflowratehourlhr double default 0,
		minfuelflowrateminuteccmin double default 0,
		maxfuelflowrateminuteccmin double default 0,
		avgfuelflowrateminuteccmin double default 0,
		minfuelrailpressurekpa double default 0,
		maxfuelrailpressurekpa double default 0,
		avgfuelrailpressurekpa double default 0,
		minfuelremainingcalculatedfromvehicleprofile double default 0,
		maxfuelremainingcalculatedfromvehicleprofile double default 0,
		avgfuelremainingcalculatedfromvehicleprofile double default 0,
		minfuelusedtripl double default 0,
		maxfuelusedtripl double default 0,
		avgfuelusedtripl double default 0,
		mingpsaccuracym double default 0,
		maxgpsaccuracym double default 0,
		avggpsaccuracym double default 0,
		mingpsaltitudem double default 0,
		maxgpsaltitudem double default 0,
		avggpsaltitudem double default 0,
		mingpsbearing double default 0,
		maxgpsbearing double default 0,
		avggpsbearing double default 0,
		mingpslatitude double default 0,
		maxgpslatitude double default 0,
		avggpslatitude double default 0,
		mingpslongitude double default 0,
		maxgpslongitude double default 0,
		avggpslongitude double default 0,
		mingpssatellites double default 0,
		maxgpssatellites double default 0,
		avggpssatellites double default 0,
		mingpsvsobdspeeddifferencekmh double default 0,
		maxgpsvsobdspeeddifferencekmh double default 0,
		avggpsvsobdspeeddifferencekmh double default 0,
		minhorsepoweratthewheelshp double default 0,
		maxhorsepoweratthewheelshp double default 0,
		avghorsepoweratthewheelshp double default 0,
		minkilometersperlitrelongtermaveragekpl double default 0,
		maxkilometersperlitrelongtermaveragekpl double default 0,
		avgkilometersperlitrelongtermaveragekpl double default 0,
		minlitresper100kilometerinstantl100km double default 0,
		maxlitresper100kilometerinstantl100km double default 0,
		avglitresper100kilometerinstantl100km double default 0,
		minlitresper100kilometerlongtermaveragel100km double default 0,
		maxlitresper100kilometerlongtermaveragel100km double default 0,
		avglitresper100kilometerlongtermaveragel100km double default 0,
		minmassairflowrategs double default 0,
		maxmassairflowrategs double default 0,
		avgmassairflowrategs double default 0,
		minmilespergalloninstantmpg double default 0,
		maxmilespergalloninstantmpg double default 0,
		avgmilespergalloninstantmpg double default 0,
		minmilespergallonlongtermaveragempg double default 0,
		maxmilespergallonlongtermaveragempg double default 0,
		avgmilespergallonlongtermaveragempg double default 0,
		maxspeedgpskmh double default 0,
		avgspeedgpskmh double default 0,
		maxspeedobdkmh double default 0,
		avgspeedobdkmh double default 0,
		mintorqueftlb double default 0,
		maxtorqueftlb double default 0,
		avgtorqueftlb double default 0,
		mintripaveragekplkpl double default 0,
		maxtripaveragekplkpl double default 0,
		avgtripaveragekplkpl double default 0,
		mintripaveragelitres100kml100km double default 0,
		maxtripaveragelitres100kml100km double default 0,
		avgtripaveragelitres100kml100km double default 0,
		mintripaveragempgmpg double default 0,
		maxtripaveragempgmpg double default 0,
		avgtripaveragempgmpg double default 0,
		mintripdistancekm double default 0,
		maxtripdistancekm double default 0,
		avgtripdistancekm double default 0,
		mintripdistancestoredinvehicleprofilekm double default 0,
		maxtripdistancestoredinvehicleprofilekm double default 0,
		avgtripdistancestoredinvehicleprofilekm double default 0,
		mintriptimesincejourneystarts double default 0,
		maxtriptimesincejourneystarts double default 0,
		avgtriptimesincejourneystarts double default 0,
		mintriptimewhilstmovings double default 0,
		maxtriptimewhilstmovings double default 0,
		avgtriptimewhilstmovings double default 0,
		mintriptimewhilststationarys double default 0,
		maxtriptimewhilststationarys double default 0,
		avgtriptimewhilststationarys double default 0,
		minturboboostvacuumgaugebar double default 0,
		maxturboboostvacuumgaugebar double default 0,
		avgturboboostvacuumgaugebar double default 0,
		minvoltageobdadapterv double default 0,
		maxvoltageobdadapterv double default 0,
		avgvoltageobdadapterv double default 0,
		minvolumetricefficiencycalculated double default 0,
		maxvolumetricefficiencycalculated double default 0,
		avgvolumetricefficiencycalculated double default 0,
		minenginekwatthewheelskw double default 0,
		maxenginekwatthewheelskw double default 0,
		avgenginekwatthewheelskw double default 0,
		minaveragetripspeedwhilstmovingonlykmh double default 0,
		maxaveragetripspeedwhilstmovingonlykmh double default 0,
		avgaveragetripspeedwhilstmovingonlykmh double default 0,
		mincostpermilekminstantkm double default 0,
		maxcostpermilekminstantkm double default 0,
		avgcostpermilekminstantkm double default 0,
		mincostpermilekmtripkm double default 0,
		maxcostpermilekmtripkm double default 0,
		avgcostpermilekmtripkm double default 0,
		minintakeairtemperaturef double default 0,
		maxintakeairtemperaturef double default 0,
		avgintakeairtemperaturef double default 0,
		minintakemanifoldpressurekpa double default 0,
		maxintakemanifoldpressurekpa double default 0,
		avgintakemanifoldpressurekpa double default 0,
		key torqdata_fk (tripid),
		constraint torqdata_fk foreign key (tripid) references torqtrips (id)
		)
	  """}


postgresql_cmds= {
	'createfiles' : """
 create table if not exists torqfiles
(
id int generated always as identity PRIMARY KEY,
csvfile  varchar(255),
csvhash varchar(255)

);
""",
'createlogs' : """
create table if not exists torqlogs
(
			id int generated always as identity PRIMARY KEY,
			tripid int not null,
			csvhash varchar(255) default 0,
			gpstime timestamp default current_timestamp,
			devicetime timestamp default current_timestamp,
			longitude double precision,
			latitude double precision,
			gpsspeedkmh double precision,
			horizontaldilutionofprecision double precision,
			altitudem double precision,
			bearing double precision,
			gravityxg double precision,
			gravityyg double precision,
			gravityzg double precision,
			accelerationsensortotalg double precision,
			accelerationsensorxaxisg double precision,
			accelerationsensoryaxisg double precision,
			accelerationsensorzaxisg double precision,
			actualenginetorque double precision,
			androiddevicebatterylevel                double precision,
			averagetripspeedwhilststoppedormovingkmh double precision,
			coingkmaveragegkm double precision,
			coingkminstantaneousgkm                  double precision,
			distancetoemptyestimatedkm               double precision,
			distancetravelledwithmilcellitkm double precision,
			enginecoolanttemperaturef double precision,
			engineload double precision,
			enginerpmrpm double precision,
			fuelcosttripcost        double precision,
			fuelflowratehourlhr     double precision,
			fuelflowrateminuteccmin double precision,
			fuelrailpressurekpa double precision,
			fuelremainingcalculatedfromvehicleprofile double precision,
			fuelusedtripl double precision,
			fuelpressurekpa double precision,
			gpsaccuracym double precision,
			gpsaltitudem double precision,
			gpsbearing double precision,
			gpslatitude double precision,
			gpslongitude double precision,
			gpssatellites double precision,
			gpsvsobdspeeddifferencekmh                double precision,
			horsepoweratthewheelshp double precision,
			intakeairtemperaturef double precision,
			intakemanifoldpressurekpa double precision,
			kilometersperlitreinstantkpl               double precision,
			kilometersperlitrelongtermaveragekpl       double precision,
			litresper100kilometerinstantl100km         double precision,
			litresper100kilometerlongtermaveragel100km double precision,
			massairflowrategs double precision,
			milespergalloninstantmpg         double precision,
			milespergallonlongtermaveragempg double precision,
			speedgpskmh double precision,
			speedobdkmh double precision,
			torqueftlb double precision,
			tripaveragekplkpl double precision,
			tripaveragelitres100kml100km         double precision,
			tripaveragempgmpg double precision,
			tripdistancekm double precision,
			tripdistancestoredinvehicleprofilekm double precision,
			triptimesincejourneystarts           double precision,
			triptimewhilstmovings                double precision,
			triptimewhilststationarys            double precision,
			turboboostvacuumgaugebar double precision,
			voltageobdadapterv                  double precision,
			volumetricefficiencycalculated      double precision,
			enginekwatthewheelskw               double precision,
			averagetripspeedwhilstmovingonlykmh double precision,
			costpermilekminstantkm double precision,
			costpermilekmtripkm double precision,
			foreign key(tripid) references torqfiles(id)
);
""", # 	foreign key (id) references torqlogs (tripid)
'createtrips': """
create table if not exists torqtrips
(
	id int generated always as identity PRIMARY KEY,
	csvfile varchar(255),
	csvhash varchar(255),
	distance double precision,
	fuelcost double precision,
	fuelused double precision,
	distancewhilstconnectedtoobd double precision,
	tripdate varchar(255),
	profile  varchar(255),
	time double precision,
	foreign key (id) references torqfiles (id)
);
""",
'createdata': """
create table if not exists torqdata (
id int generated always as identity PRIMARY KEY,
tripdate timestamp,
distance double precision,
fuelused double precision,
fuelcost double precision,
time double precision,
distancewhilstconnectedtoobd double precision,
minlongitude double precision,
maxlongitude double precision,
avglongitude double precision,
minlatitude double precision,
maxlatitude double precision,
avglatitude double precision,
mingpsspeedkmh double precision,
maxgpsspeedkmh double precision,
avggpsspeedkmh double precision,
minhorizontaldilutionofprecision double precision,
maxhorizontaldilutionofprecision double precision,
avghorizontaldilutionofprecision double precision,
minaltitudem double precision,
maxaltitudem double precision,
avgaltitudem double precision,
minbearing double precision,
maxbearing double precision,
avgbearing double precision,
mingravityxg double precision,
maxgravityxg double precision,
avggravityxg double precision,
mingravityyg double precision,
maxgravityyg double precision,
avggravityyg double precision,
mingravityzg double precision,
maxgravityzg double precision,
avggravityzg double precision,
minaccelerationsensortotalg double precision,
maxaccelerationsensortotalg double precision,
avgaccelerationsensortotalg double precision,
minaccelerationsensorxaxisg double precision,
maxaccelerationsensorxaxisg double precision,
avgaccelerationsensorxaxisg double precision,
minaccelerationsensoryaxisg double precision,
maxaccelerationsensoryaxisg double precision,
avgaccelerationsensoryaxisg double precision,
minaccelerationsensorzaxisg double precision,
maxaccelerationsensorzaxisg double precision,
avgaccelerationsensorzaxisg double precision,
minactualenginetorque double precision,
maxactualenginetorque double precision,
avgactualenginetorque double precision,
minandroiddevicebatterylevel double precision,
maxandroiddevicebatterylevel double precision,
avgandroiddevicebatterylevel double precision,
minaveragetripspeedwhilststoppedormovingkmh double precision,
maxaveragetripspeedwhilststoppedormovingkmh double precision,
avgaveragetripspeedwhilststoppedormovingkmh double precision,
mincoingkmaveragegkm double precision,
maxcoingkmaveragegkm double precision,
avgcoingkmaveragegkm double precision,
mincoingkminstantaneousgkm double precision,
maxcoingkminstantaneousgkm double precision,
avgcoingkminstantaneousgkm double precision,
mindistancetoemptyestimatedkm double precision,
maxdistancetoemptyestimatedkm double precision,
avgdistancetoemptyestimatedkm double precision,
mindistancetravelledwithmilcellitkm double precision,
maxdistancetravelledwithmilcellitkm double precision,
avgdistancetravelledwithmilcellitkm double precision,
minenginecoolanttemperaturef double precision,
maxenginecoolanttemperaturef double precision,
avgenginecoolanttemperaturef double precision,
minengineload double precision,
maxengineload double precision,
avgengineload double precision,
minenginerpmrpm double precision,
maxenginerpmrpm double precision,
avgenginerpmrpm double precision,
minfuelcosttripcost double precision,
maxfuelcosttripcost double precision,
avgfuelcosttripcost double precision,
minfuelflowratehourlhr double precision,
maxfuelflowratehourlhr double precision,
avgfuelflowratehourlhr double precision,
minfuelflowrateminuteccmin double precision,
maxfuelflowrateminuteccmin double precision,
avgfuelflowrateminuteccmin double precision,
minfuelrailpressurekpa double precision,
maxfuelrailpressurekpa double precision,
avgfuelrailpressurekpa double precision,
minfuelremainingcalculatedfromvehicleprofile double precision,
maxfuelremainingcalculatedfromvehicleprofile double precision,
avgfuelremainingcalculatedfromvehicleprofile double precision,
minfuelusedtripl double precision,
maxfuelusedtripl double precision,
avgfuelusedtripl double precision,
mingpsaccuracym double precision,
maxgpsaccuracym double precision,
avggpsaccuracym double precision,
mingpsaltitudem double precision,
maxgpsaltitudem double precision,
avggpsaltitudem double precision,
mingpsbearing double precision,
maxgpsbearing double precision,
avggpsbearing double precision,
mingpslatitude double precision,
maxgpslatitude double precision,
avggpslatitude double precision,
mingpslongitude double precision,
maxgpslongitude double precision,
avggpslongitude double precision,
mingpssatellites double precision,
maxgpssatellites double precision,
avggpssatellites double precision,
mingpsvsobdspeeddifferencekmh double precision,
maxgpsvsobdspeeddifferencekmh double precision,
avggpsvsobdspeeddifferencekmh double precision,
minhorsepoweratthewheelshp double precision,
maxhorsepoweratthewheelshp double precision,
avghorsepoweratthewheelshp double precision,
minkilometersperlitrelongtermaveragekpl double precision,
maxkilometersperlitrelongtermaveragekpl double precision,
avgkilometersperlitrelongtermaveragekpl double precision,
minlitresper100kilometerinstantl100km double precision,
maxlitresper100kilometerinstantl100km double precision,
avglitresper100kilometerinstantl100km double precision,
minlitresper100kilometerlongtermaveragel100km double precision,
maxlitresper100kilometerlongtermaveragel100km double precision,
avglitresper100kilometerlongtermaveragel100km double precision,
minmassairflowrategs double precision,
maxmassairflowrategs double precision,
avgmassairflowrategs double precision,
minmilespergalloninstantmpg double precision,
maxmilespergalloninstantmpg double precision,
avgmilespergalloninstantmpg double precision,
minmilespergallonlongtermaveragempg double precision,
maxmilespergallonlongtermaveragempg double precision,
avgmilespergallonlongtermaveragempg double precision,
maxspeedgpskmh double precision,
avgspeedgpskmh double precision,
maxspeedobdkmh double precision,
avgspeedobdkmh double precision,
mintorqueftlb double precision,
maxtorqueftlb double precision,
avgtorqueftlb double precision,
mintripaveragekplkpl double precision,
maxtripaveragekplkpl double precision,
avgtripaveragekplkpl double precision,
mintripaveragelitres100kml100km double precision,
maxtripaveragelitres100kml100km double precision,
avgtripaveragelitres100kml100km double precision,
mintripaveragempgmpg double precision,
maxtripaveragempgmpg double precision,
avgtripaveragempgmpg double precision,
mintripdistancekm double precision,
maxtripdistancekm double precision,
avgtripdistancekm double precision,
mintripdistancestoredinvehicleprofilekm double precision,
maxtripdistancestoredinvehicleprofilekm double precision,
avgtripdistancestoredinvehicleprofilekm double precision,
mintriptimesincejourneystarts double precision,
maxtriptimesincejourneystarts double precision,
avgtriptimesincejourneystarts double precision,
mintriptimewhilstmovings double precision,
maxtriptimewhilstmovings double precision,
avgtriptimewhilstmovings double precision,
mintriptimewhilststationarys double precision,
maxtriptimewhilststationarys double precision,
avgtriptimewhilststationarys double precision,
minturboboostvacuumgaugebar double precision,
maxturboboostvacuumgaugebar double precision,
avgturboboostvacuumgaugebar double precision,
minvoltageobdadapterv double precision,
maxvoltageobdadapterv double precision,
avgvoltageobdadapterv double precision,
minvolumetricefficiencycalculated double precision,
maxvolumetricefficiencycalculated double precision,
avgvolumetricefficiencycalculated double precision,
minenginekwatthewheelskw double precision default 0,
maxenginekwatthewheelskw double precision default 0,
avgenginekwatthewheelskw double precision default 0,
minaveragetripspeedwhilstmovingonlykmh double precision default 0,
maxaveragetripspeedwhilstmovingonlykmh double precision default 0,
avgaveragetripspeedwhilstmovingonlykmh double precision default 0,
mincostpermilekminstantkm double precision default 0,
maxcostpermilekminstantkm double precision default 0,
avgcostpermilekminstantkm double precision default 0,
mincostpermilekmtripkm double precision default 0,
maxcostpermilekmtripkm double precision default 0,
avgcostpermilekmtripkm double precision default 0,
minintakeairtemperaturef double precision,
maxintakeairtemperaturef double precision,
avgintakeairtemperaturef double precision,
minintakemanifoldpressurekpa double precision,
maxintakemanifoldpressurekpa double precision,
avgintakemanifoldpressurekpa double precision,
foreign key (id) references torqfiles (id));

	  """
}

sqlcmds = {
	'mysql': mysql_cmds,
	'sqlite' : sqlite_cmds,
	'postgresql' : postgresql_cmds
	}

torqdatasql="""
	SELECT     torqtrips.distance,
					 torqtrips.fuelused,
					 torqtrips.fuelcost,
					 torqtrips.time,
					 torqtrips.distancewhilstconnectedtoobd,
					 Min(longitude)                                  AS minlongitude,
					 Max(longitude)                                  AS maxlongitude,
					 Avg(longitude)                                  AS avglongitude,
					 Min(latitude)                                   AS minlatitude,
					 Max(latitude)                                   AS maxlatitude,
					 Avg(latitude)                                   AS avglatitude,
					 Min(gpsspeedkmh)                                AS mingpsspeedkmh,
					 Max(gpsspeedkmh)                                AS maxgpsspeedkmh,
					 Avg(gpsspeedkmh)                                AS avggpsspeedkmh,
					 Min(horizontaldilutionofprecision)              AS minhorizontaldilutionofprecision,
					 Max(horizontaldilutionofprecision)              AS maxhorizontaldilutionofprecision,
					 Avg(horizontaldilutionofprecision)              AS avghorizontaldilutionofprecision,
					 Min(altitudem)                                  AS minaltitudem,
					 Max(altitudem)                                  AS maxaltitudem,
					 Avg(altitudem)                                  AS avgaltitudem,
					 Min(bearing)                                    AS minbearing,
					 Max(bearing)                                    AS maxbearing,
					 Avg(bearing)                                    AS avgbearing,
					 Min(gravityxg)                                  AS mingravityxg,
					 Max(gravityxg)                                  AS maxgravityxg,
					 Avg(gravityxg)                                  AS avggravityxg,
					 Min(gravityyg)                                  AS mingravityyg,
					 Max(gravityyg)                                  AS maxgravityyg,
					 Avg(gravityyg)                                  AS avggravityyg,
					 Min(gravityzg)                                  AS mingravityzg,
					 Max(gravityzg)                                  AS maxgravityzg,
					 Avg(gravityzg)                                  AS avggravityzg,
					 Min(accelerationsensortotalg)                   AS minaccelerationsensortotalg,
					 Max(accelerationsensortotalg)                   AS maxaccelerationsensortotalg,
					 Avg(accelerationsensortotalg)                   AS avgaccelerationsensortotalg,
					 Min(accelerationsensorxaxisg)                   AS minaccelerationsensorxaxisg,
					 Max(accelerationsensorxaxisg)                   AS maxaccelerationsensorxaxisg,
					 Avg(accelerationsensorxaxisg)                   AS avgaccelerationsensorxaxisg,
					 Min(accelerationsensoryaxisg)                   AS minaccelerationsensoryaxisg,
					 Max(accelerationsensoryaxisg)                   AS maxaccelerationsensoryaxisg,
					 Avg(accelerationsensoryaxisg)                   AS avgaccelerationsensoryaxisg,
					 Min(accelerationsensorzaxisg)                   AS minaccelerationsensorzaxisg,
					 Max(accelerationsensorzaxisg)                   AS maxaccelerationsensorzaxisg,
					 Avg(accelerationsensorzaxisg)                   AS avgaccelerationsensorzaxisg,
					 Min(actualenginetorque)                         AS minactualenginetorque,
					 Max(actualenginetorque)                         AS maxactualenginetorque,
					 Avg(actualenginetorque)                         AS avgactualenginetorque,
					 Min(androiddevicebatterylevel)                  AS minandroiddevicebatterylevel,
					 Max(androiddevicebatterylevel)                  AS maxandroiddevicebatterylevel,
					 Avg(androiddevicebatterylevel)                  AS avgandroiddevicebatterylevel,
					 Min(averagetripspeedwhilststoppedormovingkmh)   AS minaveragetripspeedwhilststoppedormovingkmh,
					 Max(averagetripspeedwhilststoppedormovingkmh)   AS maxaveragetripspeedwhilststoppedormovingkmh,
					 Avg(averagetripspeedwhilststoppedormovingkmh)   AS avgaveragetripspeedwhilststoppedormovingkmh,
					 Min(coingkmaveragegkm)                          AS mincoingkmaveragegkm,
					 Max(coingkmaveragegkm)                          AS maxcoingkmaveragegkm,
					 Avg(coingkmaveragegkm)                          AS avgcoingkmaveragegkm,
					 Min(coingkminstantaneousgkm)                    AS mincoingkminstantaneousgkm,
					 Max(coingkminstantaneousgkm)                    AS maxcoingkminstantaneousgkm,
					 Avg(coingkminstantaneousgkm)                    AS avgcoingkminstantaneousgkm,
					 Min(distancetoemptyestimatedkm)                 AS mindistancetoemptyestimatedkm,
					 Max(distancetoemptyestimatedkm)                 AS maxdistancetoemptyestimatedkm,
					 Avg(distancetoemptyestimatedkm)                 AS avgdistancetoemptyestimatedkm,
					 Min(distancetravelledwithmilcellitkm)           AS mindistancetravelledwithmilcellitkm,
					 Max(distancetravelledwithmilcellitkm)           AS maxdistancetravelledwithmilcellitkm,
					 Avg(distancetravelledwithmilcellitkm)           AS avgdistancetravelledwithmilcellitkm,
					 Min(enginecoolanttemperaturef)                  AS minenginecoolanttemperaturef,
					 Max(enginecoolanttemperaturef)                  AS maxenginecoolanttemperaturef,
					 Avg(enginecoolanttemperaturef)                  AS avgenginecoolanttemperaturef,
					 Min(engineload)                                 AS minengineload,
					 Max(engineload)                                 AS maxengineload,
					 Avg(engineload)                                 AS avgengineload,
					 Min(enginerpmrpm)                               AS minenginerpmrpm,
					 Max(enginerpmrpm)                               AS maxenginerpmrpm,
					 Avg(enginerpmrpm)                               AS avgenginerpmrpm,
					 Min(fuelcosttripcost)                           AS minfuelcosttripcost,
					 Max(fuelcosttripcost)                           AS maxfuelcosttripcost,
					 Avg(fuelcosttripcost)                           AS avgfuelcosttripcost,
					 Min(fuelflowratehourlhr)                        AS minfuelflowratehourlhr,
					 Max(fuelflowratehourlhr)                        AS maxfuelflowratehourlhr,
					 Avg(fuelflowratehourlhr)                        AS avgfuelflowratehourlhr,
					 Min(fuelflowrateminuteccmin)                    AS minfuelflowrateminuteccmin,
					 Max(fuelflowrateminuteccmin)                    AS maxfuelflowrateminuteccmin,
					 Avg(fuelflowrateminuteccmin)                    AS avgfuelflowrateminuteccmin,
					 Min(fuelrailpressurekpa)                        AS minfuelrailpressurekpa,
					 Max(fuelrailpressurekpa)                        AS maxfuelrailpressurekpa,
					 Avg(fuelrailpressurekpa)                        AS avgfuelrailpressurekpa,
					 Min(fuelremainingcalculatedfromvehicleprofile)  AS minfuelremainingcalculatedfromvehicleprofile,
					 Max(fuelremainingcalculatedfromvehicleprofile)  AS maxfuelremainingcalculatedfromvehicleprofile,
					 Avg(fuelremainingcalculatedfromvehicleprofile)  AS avgfuelremainingcalculatedfromvehicleprofile,
					 Min(fuelusedtripl)                              AS minfuelusedtripl,
					 Max(fuelusedtripl)                              AS maxfuelusedtripl,
					 Avg(fuelusedtripl)                              AS avgfuelusedtripl,
					 Min(gpsaccuracym)                               AS mingpsaccuracym,
					 Max(gpsaccuracym)                               AS maxgpsaccuracym,
					 Avg(gpsaccuracym)                               AS avggpsaccuracym,
					 Min(gpsaltitudem)                               AS mingpsaltitudem,
					 Max(gpsaltitudem)                               AS maxgpsaltitudem,
					 Avg(gpsaltitudem)                               AS avggpsaltitudem,
					 Min(gpsbearing)                                 AS mingpsbearing,
					 Max(gpsbearing)                                 AS maxgpsbearing,
					 Avg(gpsbearing)                                 AS avggpsbearing,
					 Min(gpslatitude)                                AS mingpslatitude,
					 Max(gpslatitude)                                AS maxgpslatitude,
					 Avg(gpslatitude)                                AS avggpslatitude,
					 Min(gpslongitude)                               AS mingpslongitude,
					 Max(gpslongitude)                               AS maxgpslongitude,
					 Avg(gpslongitude)                               AS avggpslongitude,
					 Min(gpssatellites)                              AS mingpssatellites,
					 Max(gpssatellites)                              AS maxgpssatellites,
					 Avg(gpssatellites)                              AS avggpssatellites,
					 Min(gpsvsobdspeeddifferencekmh)                 AS mingpsvsobdspeeddifferencekmh,
					 Max(gpsvsobdspeeddifferencekmh)                 AS maxgpsvsobdspeeddifferencekmh,
					 Avg(gpsvsobdspeeddifferencekmh)                 AS avggpsvsobdspeeddifferencekmh,
					 Min(horsepoweratthewheelshp)                    AS minhorsepoweratthewheelshp,
					 Max(horsepoweratthewheelshp)                    AS maxhorsepoweratthewheelshp,
					 Avg(horsepoweratthewheelshp)                    AS avghorsepoweratthewheelshp,
					 Min(kilometersperlitrelongtermaveragekpl)       AS minkilometersperlitrelongtermaveragekpl,
					 Max(kilometersperlitrelongtermaveragekpl)       AS maxkilometersperlitrelongtermaveragekpl,
					 Avg(kilometersperlitrelongtermaveragekpl)       AS avgkilometersperlitrelongtermaveragekpl,
					 Min(litresper100kilometerinstantl100km)         AS minlitresper100kilometerinstantl100km,
					 Max(litresper100kilometerinstantl100km)         AS maxlitresper100kilometerinstantl100km,
					 Avg(litresper100kilometerinstantl100km)         AS avglitresper100kilometerinstantl100km,
					 Min(litresper100kilometerlongtermaveragel100km) AS minlitresper100kilometerlongtermaveragel100km,
					 Max(litresper100kilometerlongtermaveragel100km) AS maxlitresper100kilometerlongtermaveragel100km,
					 Avg(litresper100kilometerlongtermaveragel100km) AS avglitresper100kilometerlongtermaveragel100km,
					 Min(massairflowrategs)                          AS minmassairflowrategs,
					 Max(massairflowrategs)                          AS maxmassairflowrategs,
					 Avg(massairflowrategs)                          AS avgmassairflowrategs,
					 Min(milespergalloninstantmpg)                   AS minmilespergalloninstantmpg,
					 Max(milespergalloninstantmpg)                   AS maxmilespergalloninstantmpg,
					 Avg(milespergalloninstantmpg)                   AS avgmilespergalloninstantmpg,
					 Min(milespergallonlongtermaveragempg)           AS minmilespergallonlongtermaveragempg,
					 Max(milespergallonlongtermaveragempg)           AS maxmilespergallonlongtermaveragempg,
					 Avg(milespergallonlongtermaveragempg)           AS avgmilespergallonlongtermaveragempg,
					 Max(speedgpskmh)                                AS maxspeedgpskmh,
					 Avg(speedgpskmh)                                AS avgspeedgpskmh ,
					 Max(speedobdkmh)                                AS maxspeedobdkmh,
					 Avg(speedobdkmh)                                AS avgspeedobdkmh,
					 Min(torqueftlb)                                 AS mintorqueftlb,
					 Max(torqueftlb)                                 AS maxtorqueftlb,
					 Avg(torqueftlb)                                 AS avgtorqueftlb,
					 Min(tripaveragekplkpl)                          AS mintripaveragekplkpl,
					 Max(tripaveragekplkpl)                          AS maxtripaveragekplkpl,
					 Avg(tripaveragekplkpl)                          AS avgtripaveragekplkpl,
					 Min(tripaveragelitres100kml100km)               AS mintripaveragelitres100kml100km,
					 Max(tripaveragelitres100kml100km)               AS maxtripaveragelitres100kml100km,
					 Avg(tripaveragelitres100kml100km)               AS avgtripaveragelitres100kml100km,
					 Min(tripaveragempgmpg)                          AS mintripaveragempgmpg,
					 Max(tripaveragempgmpg)                          AS maxtripaveragempgmpg,
					 Avg(tripaveragempgmpg)                          AS avgtripaveragempgmpg,
					 Min(tripdistancekm)                             AS mintripdistancekm,
					 Max(tripdistancekm)                             AS maxtripdistancekm,
					 Avg(tripdistancekm)                             AS avgtripdistancekm,
					 Min(tripdistancestoredinvehicleprofilekm)       AS mintripdistancestoredinvehicleprofilekm,
					 Max(tripdistancestoredinvehicleprofilekm)       AS maxtripdistancestoredinvehicleprofilekm,
					 Avg(tripdistancestoredinvehicleprofilekm)       AS avgtripdistancestoredinvehicleprofilekm,
					 Min(triptimesincejourneystarts)                 AS mintriptimesincejourneystarts,
					 Max(triptimesincejourneystarts)                 AS maxtriptimesincejourneystarts,
					 Avg(triptimesincejourneystarts)                 AS avgtriptimesincejourneystarts,
					 Min(triptimewhilstmovings)                      AS mintriptimewhilstmovings,
					 Max(triptimewhilstmovings)                      AS maxtriptimewhilstmovings,
					 Avg(triptimewhilstmovings)                      AS avgtriptimewhilstmovings,
					 Min(triptimewhilststationarys)                  AS mintriptimewhilststationarys,
					 Max(triptimewhilststationarys)                  AS maxtriptimewhilststationarys,
					 Avg(triptimewhilststationarys)                  AS avgtriptimewhilststationarys,
					 Min(turboboostvacuumgaugebar)                   AS minturboboostvacuumgaugebar,
					 Max(turboboostvacuumgaugebar)                   AS maxturboboostvacuumgaugebar,
					 Avg(turboboostvacuumgaugebar)                   AS avgturboboostvacuumgaugebar,
					 Min(voltageobdadapterv)                         AS minvoltageobdadapterv,
					 Max(voltageobdadapterv)                         AS maxvoltageobdadapterv,
					 Avg(voltageobdadapterv)                         AS avgvoltageobdadapterv,
					 Min(volumetricefficiencycalculated)             AS minvolumetricefficiencycalculated,
					 Max(volumetricefficiencycalculated)             AS maxvolumetricefficiencycalculated,
					 Avg(volumetricefficiencycalculated)             AS avgvolumetricefficiencycalculated,
					 Min(enginekwatthewheelskw)                      AS minenginekwatthewheelskw,
					 Max(enginekwatthewheelskw)                      AS maxenginekwatthewheelskw,
					 Avg(enginekwatthewheelskw)                      AS avgenginekwatthewheelskw,
					 Min(averagetripspeedwhilstmovingonlykmh)        AS minaveragetripspeedwhilstmovingonlykmh,
					 Max(averagetripspeedwhilstmovingonlykmh)        AS maxaveragetripspeedwhilstmovingonlykmh,
					 Avg(averagetripspeedwhilstmovingonlykmh)        AS avgaveragetripspeedwhilstmovingonlykmh,
					 Min(costpermilekminstantkm)                     AS mincostpermilekminstantkm,
					 Max(costpermilekminstantkm)                     AS maxcostpermilekminstantkm,
					 Avg(costpermilekminstantkm)                     AS avgcostpermilekminstantkm,
					 Min(costpermilekmtripkm)                        AS mincostpermilekmtripkm,
					 Max(costpermilekmtripkm)                        AS maxcostpermilekmtripkm,
					 Avg(costpermilekmtripkm)                        AS avgcostpermilekmtripkm,
					 Min(intakeairtemperaturef)                      AS minintakeairtemperaturef,
					 Max(intakeairtemperaturef)                      AS maxintakeairtemperaturef,
					 Avg(intakeairtemperaturef)                      AS avgintakeairtemperaturef,
					 Min(intakemanifoldpressurekpa)                  AS minintakemanifoldpressurekpa,
					 Max(intakemanifoldpressurekpa)                  AS maxintakemanifoldpressurekpa,
					 Avg(intakemanifoldpressurekpa)                  AS avgintakemanifoldpressurekpa
FROM       torqlogs
INNER JOIN torqtrips
ON         torqtrips.id = torqlogs.tripid
WHERE      torqlogs.tripid="""

torqdatasql_psql="""
select
	torqtrips.distance,
	torqtrips.fuelused,
	torqtrips.fuelcost,
	torqtrips.time,
	torqtrips.distancewhilstconnectedtoobd,
	min(longitude) as minlongitude,
	max(longitude) as maxlongitude,
	avg(longitude) as avglongitude,
	min(latitude) as minlatitude,
	max(latitude) as maxlatitude,
	avg(latitude) as avglatitude,
	min(gpsspeedkmh) as mingpsspeedkmh,
	max(gpsspeedkmh) as maxgpsspeedkmh,
	avg(gpsspeedkmh) as avggpsspeedkmh,
	min(horizontaldilutionofprecision) as minhorizontaldilutionofprecision,
	max(horizontaldilutionofprecision) as maxhorizontaldilutionofprecision,
	avg(horizontaldilutionofprecision) as avghorizontaldilutionofprecision,
	min(altitudem) as minaltitudem,
	max(altitudem) as maxaltitudem,
	avg(altitudem) as avgaltitudem,
	min(bearing) as minbearing,
	max(bearing) as maxbearing,
	avg(bearing) as avgbearing,
	min(gravityxg) as mingravityxg,
	max(gravityxg) as maxgravityxg,
	avg(gravityxg) as avggravityxg,
	min(gravityyg) as mingravityyg,
	max(gravityyg) as maxgravityyg,
	avg(gravityyg) as avggravityyg,
	min(gravityzg) as mingravityzg,
	max(gravityzg) as maxgravityzg,
	avg(gravityzg) as avggravityzg,
	min(accelerationsensortotalg) as minaccelerationsensortotalg,
	max(accelerationsensortotalg) as maxaccelerationsensortotalg,
	avg(accelerationsensortotalg) as avgaccelerationsensortotalg,
	min(accelerationsensorxaxisg) as minaccelerationsensorxaxisg,
	max(accelerationsensorxaxisg) as maxaccelerationsensorxaxisg,
	avg(accelerationsensorxaxisg) as avgaccelerationsensorxaxisg,
	min(accelerationsensoryaxisg) as minaccelerationsensoryaxisg,
	max(accelerationsensoryaxisg) as maxaccelerationsensoryaxisg,
	avg(accelerationsensoryaxisg) as avgaccelerationsensoryaxisg,
	min(accelerationsensorzaxisg) as minaccelerationsensorzaxisg,
	max(accelerationsensorzaxisg) as maxaccelerationsensorzaxisg,
	avg(accelerationsensorzaxisg) as avgaccelerationsensorzaxisg,
	min(actualenginetorque) as minactualenginetorque,
	max(actualenginetorque) as maxactualenginetorque,
	avg(actualenginetorque) as avgactualenginetorque,
	min(androiddevicebatterylevel) as minandroiddevicebatterylevel,
	max(androiddevicebatterylevel) as maxandroiddevicebatterylevel,
	avg(androiddevicebatterylevel) as avgandroiddevicebatterylevel,
	min(averagetripspeedwhilststoppedormovingkmh) as minaveragetripspeedwhilststoppedormovingkmh,
	max(averagetripspeedwhilststoppedormovingkmh) as maxaveragetripspeedwhilststoppedormovingkmh,
	avg(averagetripspeedwhilststoppedormovingkmh) as avgaveragetripspeedwhilststoppedormovingkmh,
	min(coingkmaveragegkm) as mincoingkmaveragegkm,
	max(coingkmaveragegkm) as maxcoingkmaveragegkm,
	avg(coingkmaveragegkm) as avgcoingkmaveragegkm,
	min(coingkminstantaneousgkm) as mincoingkminstantaneousgkm,
	max(coingkminstantaneousgkm) as maxcoingkminstantaneousgkm,
	avg(coingkminstantaneousgkm) as avgcoingkminstantaneousgkm,
	min(distancetoemptyestimatedkm) as mindistancetoemptyestimatedkm,
	max(distancetoemptyestimatedkm) as maxdistancetoemptyestimatedkm,
	avg(distancetoemptyestimatedkm) as avgdistancetoemptyestimatedkm,
	min(distancetravelledwithmilcellitkm) as mindistancetravelledwithmilcellitkm,
	max(distancetravelledwithmilcellitkm) as maxdistancetravelledwithmilcellitkm,
	avg(distancetravelledwithmilcellitkm) as avgdistancetravelledwithmilcellitkm,
	min(enginecoolanttemperaturef) as minenginecoolanttemperaturef,
	max(enginecoolanttemperaturef) as maxenginecoolanttemperaturef,
	avg(enginecoolanttemperaturef) as avgenginecoolanttemperaturef,
	min(engineload) as minengineload,
	max(engineload) as maxengineload,
	avg(engineload) as avgengineload,
	min(enginerpmrpm) as minenginerpmrpm,
	max(enginerpmrpm) as maxenginerpmrpm,
	avg(enginerpmrpm) as avgenginerpmrpm,
	min(fuelcosttripcost) as minfuelcosttripcost,
	max(fuelcosttripcost) as maxfuelcosttripcost,
	avg(fuelcosttripcost) as avgfuelcosttripcost,
	min(fuelflowratehourlhr) as minfuelflowratehourlhr,
	max(fuelflowratehourlhr) as maxfuelflowratehourlhr,
	avg(fuelflowratehourlhr) as avgfuelflowratehourlhr,
	min(fuelflowrateminuteccmin) as minfuelflowrateminuteccmin,
	max(fuelflowrateminuteccmin) as maxfuelflowrateminuteccmin,
	avg(fuelflowrateminuteccmin) as avgfuelflowrateminuteccmin,
	min(fuelrailpressurekpa) as minfuelrailpressurekpa,
	max(fuelrailpressurekpa) as maxfuelrailpressurekpa,
	avg(fuelrailpressurekpa) as avgfuelrailpressurekpa,
	min(fuelremainingcalculatedfromvehicleprofile) as minfuelremainingcalculatedfromvehicleprofile,
	max(fuelremainingcalculatedfromvehicleprofile) as maxfuelremainingcalculatedfromvehicleprofile,
	avg(fuelremainingcalculatedfromvehicleprofile) as avgfuelremainingcalculatedfromvehicleprofile,
	min(fuelusedtripl) as minfuelusedtripl,
	max(fuelusedtripl) as maxfuelusedtripl,
	avg(fuelusedtripl) as avgfuelusedtripl,
	min(gpsaccuracym) as mingpsaccuracym,
	max(gpsaccuracym) as maxgpsaccuracym,
	avg(gpsaccuracym) as avggpsaccuracym,
	min(gpsaltitudem) as mingpsaltitudem,
	max(gpsaltitudem) as maxgpsaltitudem,
	avg(gpsaltitudem) as avggpsaltitudem,
	min(gpsbearing) as mingpsbearing,
	max(gpsbearing) as maxgpsbearing,
	avg(gpsbearing) as avggpsbearing,
	min(gpslatitude) as mingpslatitude,
	max(gpslatitude) as maxgpslatitude,
	avg(gpslatitude) as avggpslatitude,
	min(gpslongitude) as mingpslongitude,
	max(gpslongitude) as maxgpslongitude,
	avg(gpslongitude) as avggpslongitude,
	min(gpssatellites) as mingpssatellites,
	max(gpssatellites) as maxgpssatellites,
	avg(gpssatellites) as avggpssatellites,
	min(gpsvsobdspeeddifferencekmh) as mingpsvsobdspeeddifferencekmh,
	max(gpsvsobdspeeddifferencekmh) as maxgpsvsobdspeeddifferencekmh,
	avg(gpsvsobdspeeddifferencekmh) as avggpsvsobdspeeddifferencekmh,
	min(horsepoweratthewheelshp) as minhorsepoweratthewheelshp,
	max(horsepoweratthewheelshp) as maxhorsepoweratthewheelshp,
	avg(horsepoweratthewheelshp) as avghorsepoweratthewheelshp,
	min(kilometersperlitrelongtermaveragekpl) as minkilometersperlitrelongtermaveragekpl,
	max(kilometersperlitrelongtermaveragekpl) as maxkilometersperlitrelongtermaveragekpl,
	avg(kilometersperlitrelongtermaveragekpl) as avgkilometersperlitrelongtermaveragekpl,
	min(litresper100kilometerinstantl100km) as minlitresper100kilometerinstantl100km,
	max(litresper100kilometerinstantl100km) as maxlitresper100kilometerinstantl100km,
	avg(litresper100kilometerinstantl100km) as avglitresper100kilometerinstantl100km,
	min(litresper100kilometerlongtermaveragel100km) as minlitresper100kilometerlongtermaveragel100km,
	max(litresper100kilometerlongtermaveragel100km) as maxlitresper100kilometerlongtermaveragel100km,
	avg(litresper100kilometerlongtermaveragel100km) as avglitresper100kilometerlongtermaveragel100km,
	min(massairflowrategs) as minmassairflowrategs,
	max(massairflowrategs) as maxmassairflowrategs,
	avg(massairflowrategs) as avgmassairflowrategs,
	min(milespergalloninstantmpg) as minmilespergalloninstantmpg,
	max(milespergalloninstantmpg) as maxmilespergalloninstantmpg,
	avg(milespergalloninstantmpg) as avgmilespergalloninstantmpg,
	min(milespergallonlongtermaveragempg) as minmilespergallonlongtermaveragempg,
	max(milespergallonlongtermaveragempg) as maxmilespergallonlongtermaveragempg,
	avg(milespergallonlongtermaveragempg) as avgmilespergallonlongtermaveragempg,
	max(speedgpskmh) as maxspeedgpskmh,
	avg(speedgpskmh) as avgspeedgpskmh,
	max(speedobdkmh) as maxspeedobdkmh,
	avg(speedobdkmh) as avgspeedobdkmh,
	min(torqueftlb) as mintorqueftlb,
	max(torqueftlb) as maxtorqueftlb,
	avg(torqueftlb) as avgtorqueftlb,
	min(tripaveragekplkpl) as mintripaveragekplkpl,
	max(tripaveragekplkpl) as maxtripaveragekplkpl,
	avg(tripaveragekplkpl) as avgtripaveragekplkpl,
	min(tripaveragelitres100kml100km) as mintripaveragelitres100kml100km,
	max(tripaveragelitres100kml100km) as maxtripaveragelitres100kml100km,
	avg(tripaveragelitres100kml100km) as avgtripaveragelitres100kml100km,
	min(tripaveragempgmpg) as mintripaveragempgmpg,
	max(tripaveragempgmpg) as maxtripaveragempgmpg,
	avg(tripaveragempgmpg) as avgtripaveragempgmpg,
	min(tripdistancekm) as mintripdistancekm,
	max(tripdistancekm) as maxtripdistancekm,
	avg(tripdistancekm) as avgtripdistancekm,
	min(tripdistancestoredinvehicleprofilekm) as mintripdistancestoredinvehicleprofilekm,
	max(tripdistancestoredinvehicleprofilekm) as maxtripdistancestoredinvehicleprofilekm,
	avg(tripdistancestoredinvehicleprofilekm) as avgtripdistancestoredinvehicleprofilekm,
	min(triptimesincejourneystarts) as mintriptimesincejourneystarts,
	max(triptimesincejourneystarts) as maxtriptimesincejourneystarts,
	avg(triptimesincejourneystarts) as avgtriptimesincejourneystarts,
	min(triptimewhilstmovings) as mintriptimewhilstmovings,
	max(triptimewhilstmovings) as maxtriptimewhilstmovings,
	avg(triptimewhilstmovings) as avgtriptimewhilstmovings,
	min(triptimewhilststationarys) as mintriptimewhilststationarys,
	max(triptimewhilststationarys) as maxtriptimewhilststationarys,
	avg(triptimewhilststationarys) as avgtriptimewhilststationarys,
	min(turboboostvacuumgaugebar) as minturboboostvacuumgaugebar,
	max(turboboostvacuumgaugebar) as maxturboboostvacuumgaugebar,
	avg(turboboostvacuumgaugebar) as avgturboboostvacuumgaugebar,
	min(voltageobdadapterv) as minvoltageobdadapterv,
	max(voltageobdadapterv) as maxvoltageobdadapterv,
	avg(voltageobdadapterv) as avgvoltageobdadapterv,
	min(volumetricefficiencycalculated) as minvolumetricefficiencycalculated,
	max(volumetricefficiencycalculated) as maxvolumetricefficiencycalculated,
	avg(volumetricefficiencycalculated) as avgvolumetricefficiencycalculated,
	min(enginekwatthewheelskw) as minenginekwatthewheelskw,
	max(enginekwatthewheelskw) as maxenginekwatthewheelskw,
	avg(enginekwatthewheelskw) as avgenginekwatthewheelskw,
	min(averagetripspeedwhilstmovingonlykmh) as minaveragetripspeedwhilstmovingonlykmh,
	max(averagetripspeedwhilstmovingonlykmh) as maxaveragetripspeedwhilstmovingonlykmh,
	avg(averagetripspeedwhilstmovingonlykmh) as avgaveragetripspeedwhilstmovingonlykmh,
	min(costpermilekminstantkm) as mincostpermilekminstantkm,
	max(costpermilekminstantkm) as maxcostpermilekminstantkm,
	avg(costpermilekminstantkm) as avgcostpermilekminstantkm,
	min(costpermilekmtripkm) as mincostpermilekmtripkm,
	max(costpermilekmtripkm) as maxcostpermilekmtripkm,
	avg(costpermilekmtripkm) as avgcostpermilekmtripkm,
	min(intakeairtemperaturef) as minintakeairtemperaturef,
	max(intakeairtemperaturef) as maxintakeairtemperaturef,
	avg(intakeairtemperaturef) as avgintakeairtemperaturef,
	min(intakemanifoldpressurekpa) as minintakemanifoldpressurekpa,
	max(intakemanifoldpressurekpa) as maxintakemanifoldpressurekpa,
	avg(intakemanifoldpressurekpa) as avgintakemanifoldpressurekpa
from
	torqlogs
	join torqtrips on torqtrips.id = tripid
where
	tripid =
"""



def create_tripdata(engine, session, newfilelist):
	#torqtrips = pd.read_sql(f'select id from torqtrips', session)
	for newtrip in newfilelist:
		# logger.debug(f'[updatetrip] id={trip}')
		trip = newtrip.id
		if engine.name == 'postgresql':
			sqlmagic = f'{torqdatasql_psql}{trip} group by torqtrips.distance,torqtrips.fuelused,torqtrips.fuelcost,torqtrips.time,torqtrips.distancewhilstconnectedtoobd '
		elif engine.name == 'mysql':
			#sqlmagic = f'{torqdatasql}{trip} group by tripid '
			sqlmagic = f'{torqdatasql}{trip}'
		elif engine.name == 'sqlite':
			#sqlmagic = f'{torqdatasql}{trip} group by tripid '
			sqlmagic = f'{torqdatasql}{trip}'
		#res = pd.read_sql(f'SELECT tripid, MIN({c}) as min{c}, MAX({c}) as max{c}, AVG({c}) as avg{c} FROM torqlogs WHERE tripid = "{trip}"', engine)
		#res = pd.read_sql(sqlmagic, session)
		try:
			res = [k for k in session.execute(text(sqlmagic)).all()]
		except OperationalError as e:
			logger.error(f'[createtripdata] OperationalError code={e} args={e.args[0]}  tripid={trip} newtrip={newtrip} ')
			logger.error(e)
			logger.error(f'[e] {type(e)}')
			continue
		sql_tripdate = text(f'select tripdate from torqtrips where id={trip}')
		tripdate = [k._asdict() for k in session.execute(sql_tripdate).fetchall()]
		# logger.debug(f'[createtripdata] res={len(res)} {type(res)} res0={type(res[0])} tripdate={tripdate}')
		# res.insert(1, "tripdate", tripdate)
		if engine.name == 'mysql' or engine.name == 'sqlite':
			try:
				pl.DataFrame(res).to_pandas().to_sql('torqdata', engine, if_exists='append',  index=False)
				#_ = [pl.DataFrame(r).to_sql('torqdata', session, if_exists='append',  index=False) for r in res]
			except AttributeError as e:
				logger.error(f'[createtripdata] {e}')
			except IntegrityError as e:
				logger.error(f'[IntegrityError] trip:{trip} {e}')
				# emsg1 = e.args[0].split(') (')[0][1:]
				# emsg2 = e.args[0].split(') (')[1][0:4]
				# emsg3 = e.args[0].split(') (')[1][7:]
				# logger.error(f'[IntegrityError] trip:{trip} {e.code} {emsg1} {emsg2} {emsg3} tripdate={tripdate}')
			except ValueError as e:
				logger.error(f'[createtripdata] {e} {type(e)} trip:{trip} tripdate={tripdate}')
		elif engine.name == 'postgresql':
			try:
				pl.DataFrame(res).to_pandas().to_sql('torqdata', engine, if_exists='append', index=False)
			except Exception as e:
				logger.error(f'[createtripdata] {e} {type(e)} trip:{trip} tripdate={tripdate}')

def send_torqdata(tfid, dburl, debug=False):
	engine = create_engine(dburl, echo=False)
	insp = inspect(engine)
	Session = sessionmaker(bind=engine)
	session = Session()

	#Session = sessionmaker(bind=engine)
	#session = Session()
	try:
		tf = session.query(TorqFile).filter(TorqFile.id == tfid).first()
		if debug:
			logger.debug(f'{tfid=} {tf=}')
	except OperationalError as e:
		logger.error(f'[sendtd] OperationalError code={e} args={e.args[0]} tripid={tfid}')
		return None
	except ProgrammingError as e:
		logger.error(f'[sendtd] ProgrammingError {e} tripid={tfid}')
		return None
	if engine.name == 'postgresql':
		sqlmagic = f'{torqdatasql_psql}{tf.id} group by torqtrips.distance,torqtrips.fuelused,torqtrips.fuelcost,torqtrips.time,torqtrips.distancewhilstconnectedtoobd '
	elif engine.name == 'mysql':
		sqlmagic = f'{torqdatasql}{tf.id}'
	elif engine.name == 'sqlite':
		sqlmagic = f'{torqdatasql}{tf.id}'
	try:
		# p = pd.DataFrame([f._mapping for f in foo])
		# get columns
		# insp.get_columns('torqfiles')
		# res = pl.DataFrame([k for k in session.execute(text(sqlmagic)).all()])
		# SELECT * FROM information_schema.columns WHERE TABLE_NAME = ''
		res = pl.DataFrame(pd.DataFrame([k for k in session.execute(text(sqlmagic)).all()]))
	except OperationalError as e:
		logger.error(f'[sendtd] OperationalError code={e} args={e.args[0]} tripid={tf.tripid} newtrip={tf} ')
		return None
	if len(res) == 0:
		logger.warning(f'[sendtd] no data for tripid={tf.tripid} newtrip={tf}\nres:{res}')
		return None
	sql_tripdate = text(f'select tripdate from torqtrips where id={tf.tripid}')
	tripdate_ = session.execute(sql_tripdate).one()._asdict().get('tripdate')
	if isinstance(tripdate_, str):
		try:
			tripdate = datetime.strptime(tripdate_[0][:-7],'%Y-%m-%d %H:%M:%S')
		except TypeError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} td={tripdate_} {type(tripdate_)}')
		except ValueError as e:
			tripdate = datetime.strptime(tripdate_[:-7],'%Y-%m-%d %H:%M:%S')
			#logger.warning(f'[sendtd] error:{type(e)} {e} tripdate trip:{tf} tripdate_={tripdate_} tripdate={tripdate}')
	else:
		tripdate = tripdate_
	try:
		tripdateseries = pl.Series(name="tripdate", values=[tripdate for k in range(len(res))])
		res.insert_at_idx(1, tripdateseries)
		#res.insert(1, "tripdate", [tripdate for k in range(len(res))])
	except IndexError as e:
		logger.error(f'[sendtd] resinsert error:{e} insert tripdate trip:{tf} tripdate={tripdate} res={type(res)} {len(res)}')
	except ValueError as e:
		logger.error(f'[sendtd] resinsert error:{type(e)} {e} insert tripdate trip:{tf} tripdate={tripdate} res={type(res)} {len(res)}')

	# tripdate = datetime.strptime(pdata_date ,'%a %b %d %H:%M:%S %Z%z %Y')
	# tripdate = datetime.strptime(tripdict['tripdate'],'%Y-%m-%d %H:%M:%S')
	# get colum names
	# session.execute(text(sqlmagic)).keys()
	# insp.get_columns('torqfiles')
	if engine.name == 'mysql' or engine.name == 'sqlite':
		try:
			#pl.DataFrame(res).to_pandas().to_sql('torqdata', engine, if_exists='append',  index=False)
			res.to_pandas().to_sql('torqdata', engine, if_exists='append',  index=False)
		except OperationalError as e:
			logger.error(f'[sendtd] code={e} args={e.args[0]} trip:{tf} tripdate={tripdate}')
		except AttributeError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
		except IntegrityError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
		except ValueError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
	elif engine.name == 'postgresql':
		try:
			res.to_pandas().to_sql('torqdata', engine, if_exists='append', index=False)
		except ValueError as e:
			logger.error(f'[!] {e} res:{type(res)} ')
		except Exception as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
	if debug:
		logger.info(f'[torqdata] tfid={tf.id} tripdate={tripdate}  tf={tf}')
	engine.dispose()




def send_torqdata_ppe(tfid, session, debug=False):
	tf = session.query(TorqFile).filter(TorqFile.id == tfid).first()
	if debug:
		logger.debug(f'{tfid=} {tf=}')
	# if dbmode == 'postgresql':
	# 	sqlmagic = f'{torqdatasql_psql}{tf.id} group by torqtrips.distance,torqtrips.fuelused,torqtrips.fuelcost,torqtrips.time,torqtrips.distancewhilstconnectedtoobd '
	# elif dbmode == 'mysql':
	# 	sqlmagic = f'{torqdatasql}{tf.id}'
	# elif engine.name == 'sqlite':
	# 	sqlmagic = f'{torqdatasql}{tf.id}'
	sqlmagic = f'{torqdatasql}{tf.id}'
	if debug:
		print(f'\nsqlmagic={sqlmagic}\n')
	try:
		res = pl.DataFrame(pd.DataFrame([k for k in session.execute(text(sqlmagic)).all()]))
	except OperationalError as e:
		logger.error(f'[sendtd] OperationalError code={e} args={e.args[0]} tripid={tf.tripid} newtrip={tf} ')
		return None
	if len(res) == 0:
		logger.warning(f'[sendtd] no data for tripid={tf.tripid} newtrip={tf}\nres:{res}')
		return None
	sql_tripdate = text(f'select tripdate from torqtrips where id={tf.tripid}')
	try:
		tripdate_ = session.execute(sql_tripdate).one()._asdict().get('tripdate')
	except OperationalError as e:
		logger.error(f'{e} {tf=}')
		raise e
	if isinstance(tripdate_, str):
		try:
			tripdate = datetime.strptime(tripdate_[0][:-7],'%Y-%m-%d %H:%M:%S')
		except TypeError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} td={tripdate_} {type(tripdate_)}')
		except ValueError as e:
			tripdate = datetime.strptime(tripdate_[:-7],'%Y-%m-%d %H:%M:%S')
			#logger.warning(f'[sendtd] error:{type(e)} {e} tripdate trip:{tf} tripdate_={tripdate_} tripdate={tripdate}')
	else:
		tripdate = tripdate_
	try:
		tripdateseries = pl.Series(name="tripdate", values=[tripdate for k in range(len(res))])
		res.insert_at_idx(1, tripdateseries)
		#res.insert(1, "tripdate", [tripdate for k in range(len(res))])
	except IndexError as e:
		logger.error(f'[sendtd] resinsert error:{e} insert tripdate trip:{tf} tripdate={tripdate} res={type(res)} {len(res)}')
	except ValueError as e:
		logger.error(f'[sendtd] resinsert error:{type(e)} {e} insert tripdate trip:{tf} tripdate={tripdate} res={type(res)} {len(res)}')

	if session.connection().dialect.name == 'mysql' or session.connection().dialect.name == 'sqlite':
		try:
			#pl.DataFrame(res).to_pandas().to_sql('torqdata', engine, if_exists='append',  index=False)
			res.to_pandas().to_sql('torqdata', session.get_bind(), if_exists='append',  index=False)
		except OperationalError as e:
			logger.error(f'[sendtd] code={e} args={e.args[0]} trip:{tf} tripdate={tripdate}')
		except AttributeError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
		except IntegrityError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
		except ValueError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
	# elif dbmode == 'postgresql':
	# 	try:
	# 		res.to_pandas().to_sql('torqdata', session.get_bind(), if_exists='append', index=False)
	# 	except ValueError as e:
	# 		logger.error(f'[!] {e} res:{type(res)} ')
	# 	except Exception as e:
	# 		logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
	# if debug:
	# 	logger.info(f'[torqdata] tfid={tf.id} tripdate={tripdate}  tf={tf}')
