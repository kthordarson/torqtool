from hashlib import md5
from threading import Thread, active_count

import pandas as pd
from loguru import logger
from pandas import DataFrame
from sqlalchemy import (BIGINT, BigInteger, Column, DateTime, Float, Integer,
                        MetaData, Numeric, String, Table, create_engine,
                        inspect, select, text)
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions

def create_tripdata(engine=None):
	selectfoo=""" 
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
WHERE      torqlogs.tripid= """

	selectfoo_psql="""
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
	toptrips = pd.read_sql(f'select * from torqtrips', engine)
	for trip in toptrips.id:
		# logger.debug(f'[updatetrip] id={trip}')
		try:
			if engine.name == 'postgresql':
				sqlmagic = f'{selectfoo_psql}{trip} group by torqtrips.distance,torqtrips.fuelused,torqtrips.fuelcost,torqtrips.time,torqtrips.distancewhilstconnectedtoobd '
			else:
				#sqlmagic = f'{selectfoo}{trip} group by tripid '
				sqlmagic = f'{selectfoo}{trip}'
			#res = pd.read_sql(f'SELECT tripid, MIN({c}) as min{c}, MAX({c}) as max{c}, AVG({c}) as avg{c} FROM torqlogs WHERE tripid = "{trip}"', engine)
			res = pd.read_sql(sqlmagic, engine)
			tripdate = pd.read_sql(f'select tripdate from torqtrips where id={trip} ', engine)
			# torqtrip = pd.read_sql(f'select * from torqtrips where id={trip} ', engine)
			tripdate = pd.to_datetime(tripdate.values[0][0])
			res.insert(1, "tripdate", [tripdate for k in range(len(res))])
			if engine.name == 'mysql':
				try:
					res.to_sql('torqdata', engine, if_exists='append', index_label='id')
				except IntegrityError as e:
					logger.error(f'[E] {e} trip:{trip}')
			if engine.name == 'postgresql':
				res.to_sql('torqdata', engine, if_exists='append', index=False)
			# alltripsdata.append({'tripid':trip, 'data':res})
			#print(f'[r] {res}')
		except OperationalError as e:
			logger.error(f'err {e}')
