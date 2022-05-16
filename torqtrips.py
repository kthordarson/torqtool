from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, DataError

from hashlib import md5
from utils import get_csv_files
from threading import Thread, active_count
import psycopg2
from datamodels import TorqTrip, TorqFile, TorqEntry

Base = declarative_base()

from loguru import logger


param_dic = {
	'dialect': 'mysql',
	'driver': 'pymysql',
	'host' : 'elitedesk',
	'database' : 'torq9',
	'user' : 'torq',
	# 'password' : 'foobar9999',
	'password' : 'dzt3f5jCvMlbUvRG',
	'pool_size': 200,
	'max_overflow':0,
	'port': 3306
}

connect = "%s+%s://%s:%s@%s:%s/%s" % (
param_dic['dialect'],
param_dic['driver'],
param_dic['user'],
param_dic['password'],
param_dic['host'],
param_dic['port'],
param_dic['database'])
engine = create_engine(connect)
tasks = []
if engine.driver == 'mysql':
	conn = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()

hashres = session.execute(select(TorqFile)).fetchall()
hashlist = [k[0].hash for k in hashres]
max_results = 10
sql = f"SELECT * FROM torqtrips ORDER BY distance DESC LIMIT {max_results}"
mycursor = session.execute(sql)
profiles = mycursor.fetchall()
sql = f"select * from information_schema.columns where table_schema = 'torq9'"
colnames = session.execute(sql).fetchall()
columns = [k[3] for k in colnames if k[2] == 'torqlogs']
[print(f'[profiledata] top {max_results} tripid:{k.tripid} dist:{k.distance} trip:{k.tripid}') for k in profiles]
db_err = []
info2 = []
for trip in profiles:
	# print(f'[t] checking {trip.filename} dberr:{len(db_err)} i2:{len(info2)}')
	#sql = f"SELECT * FROM torqlogs WHERE tripid = {trip.tripid}"
	# print(f'[s] {sql}')
	#cursor = session.execute(sql)
	#results = cursor.fetchall()
	#num_fields = len(cursor.columns) 'SpeedGPSkmh'
	chk_cols = ['SpeedGPSkmh', 'SpeedOBDkmh', 'Averagetripspeedwhilststoppedormovingkmh','DistancetravelledwithMILCELlitkm', 'KilometersPerLitreLongTermAveragekpl', 'Averagetripspeedwhilstmovingonlykmh', 'LitresPer100KilometerLongTermAveragel100km', 'Tripdistancestoredinvehicleprofilekm', 'TripDistancekm', 'TripTimeSincejourneystarts', 'CostpermilekmTripkm', 'TripaverageLitres100KMl100km', 'EnginekWAtthewheelskW',  'Fuelpressurekpa', 'CostpermilekmInstantkm', 'TripaverageMPGmpg', 'TripaverageKPLkpl', 'Triptimewhilstmovings', 'Actualenginetorque', 'Triptimewhilststationarys']
	for c in chk_cols:
		res = session.execute(f'SELECT MIN({c}), MAX({c}), AVG({c}) FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()[0]
		if None not in res:
			print(f'[tt] trip:{trip.tripid} c:{c} min:{res[0]} max:{res[1]} avg:{res[2]} res:{res} type:{type(res)}')
		else:
			pass
			#print(f'[tt]N trip:{trip.tripid} c:{c} min:{res[0]} max:{res[1]} avg:{res[2]} res:{res} type:{type(res)}')
#	SpeedGPSkmh = session.execute(f'SELECT MIN(SpeedGPSkmh), MAX(SpeedGPSkmh), AVG(SpeedGPSkmh) FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()[0]
#	SpeedOBDkmh = session.execute(f'SELECT MIN(SpeedOBDkmh), MAX(SpeedOBDkmh), AVG(SpeedOBDkmh) FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()[0]
#	print(f'[tt] {trip.tripid} gps:{SpeedGPSkmh[0]}/{SpeedGPSkmh[1]}/{SpeedGPSkmh[2]} odb:{SpeedOBDkmh[0]}/{SpeedOBDkmh[1]}/{SpeedOBDkmh[2]}')
	# for c in columns:
	# 	info = str(session.execute(f'SELECT MIN({c}),MAX({c}),AVG({c}) FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()[0])
	# 	if 'None' in info:
	# 		info2.append(str(session.execute(f'SELECT {c} FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()))
	# 		db_err.append(f'{trip.filename}|{c}|{info}')
#			print(f'[c] tid:{trip.tripid} tf:{trip.filename} {c} {info} i2:{len(info2)}')# {type(min)} {type(max)}')
col=set([k.split('|')[1] for k in db_err if k.split('|')[1]])
print(col)
#for k in info2:
#	print(k)
#for err in db_err:
#	print(f'[e] {err}')
#[print(f'[t] {k.id}') for k in profiles]

# 	hashlist = [k for k in conn.execute('select hash from torqfiles')]
