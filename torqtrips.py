from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, DataError
import pandas as pd
from hashlib import md5
from utils import get_csv_files
from threading import Thread, active_count

# from datamodels import TorqFile

TORQDBHOST = 'localhost'
TORQDBUSER = 'torq'
TORQDBPASS = 'dzt3f5jCvMlbUvRG'

dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/torq?charset=utf8mb4"
engine = create_engine(dburl)

max_results = 10
toptrips = pd.read_sql(f'select * from torqtrips order by distance desc limit {max_results}', engine)

for trip in toptrips.id:
	chk_cols = ['speedgpskmh', 'speedobdkmh', 'averagetripspeedwhilststoppedormovingkmh', 'distancetravelledwithmilcellitkm', 'kilometersperlitrelongtermaveragekpl', 'averagetripspeedwhilstmovingonlykmh',
	            'litresper100kilometerlongtermaveragel100km', 'tripdistancestoredinvehicleprofilekm', 'tripdistancekm', 'triptimesincejourneystarts', 'costpermilekmtripkm', 'tripaveragelitres100kml100km', 'enginekwatthewheelskw',
	            'costpermilekminstantkm', 'tripaveragempgmpg', 'tripaveragekplkpl', 'triptimewhilstmovings', 'actualenginetorque', 'triptimewhilststationarys']
	print(f'[trip] {trip}')
	for c in chk_cols[0:4]:
		print(f'[c] c:{c} t:{trip}')
		res = pd.read_sql(f'SELECT MIN({c}), MAX({c}), AVG({c}) FROM torqlogs WHERE tripid = "{trip}"', engine)
		if None not in res:
			print(f'[r] {res}')
		else:
			pass
		# print(f'[tt]N trip:{trip.tripid} c:{c} min:{res[0]} max:{res[1]} avg:{res[2]} res:{res} type:{type(res)}')
#	SpeedGPSkmh = session.execute(f'SELECT MIN(SpeedGPSkmh), MAX(SpeedGPSkmh), AVG(SpeedGPSkmh) FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()[0]
#	SpeedOBDkmh = session.execute(f'SELECT MIN(SpeedOBDkmh), MAX(SpeedOBDkmh), AVG(SpeedOBDkmh) FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()[0]
#	print(f'[tt] {trip.tripid} gps:{SpeedGPSkmh[0]}/{SpeedGPSkmh[1]}/{SpeedGPSkmh[2]} odb:{SpeedOBDkmh[0]}/{SpeedOBDkmh[1]}/{SpeedOBDkmh[2]}')
# for c in columns:
# 	info = str(session.execute(f'SELECT MIN({c}),MAX({c}),AVG({c}) FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()[0])
# 	if 'None' in info:
# 		info2.append(str(session.execute(f'SELECT {c} FROM torqlogs WHERE tripid = {trip.tripid}').fetchall()))
# 		db_err.append(f'{trip.filename}|{c}|{info}')
#			print(f'[c] tid:{trip.tripid} tf:{trip.filename} {c} {info} i2:{len(info2)}')# {type(min)} {type(max)}')
# col=set([k.split('|')[1] for k in db_err if k.split('|')[1]])
# print(col)
# for k in info2:
#	print(k)
# for err in db_err:
#	print(f'[e] {err}')
# [print(f'[t] {k.id}') for k in profiles]

# 	hashlist = [k for k in conn.execute('select hash from torqfiles')]
# print(f'[t] checking {trip.filename} dberr:{len(db_err)} i2:{len(info2)}')
# sql = f"SELECT * FROM torqlogs WHERE tripid = {trip.tripid}"
# print(f'[s] {sql}')
# cursor = session.execute(sql)
# results = cursor.fetchall()
# num_fields = len(cursor.columns) 'SpeedGPSkmh'
