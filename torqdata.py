from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.exc import OperationalError, DataError
import pandas as pd
from loguru import logger
from torqcols import cols
#from datamodels import TorqFile


def get_trip_data(trip):
	resdata = []
	for c in cols:
		# print(f'[c] c:{c} t:{trip}')
		res = None
		try:
			res = pd.read_sql(f'SELECT tripid, MIN({c}) as min{c}, MAX({c}) as max{c}, AVG({c}) as avg{c} FROM torqlogs WHERE tripid = "{trip}"', engine)
			resdata.append(res)
		except OperationalError as e:
			logger.warning(f'err {c}')
			res = None
	print(f'[{trip}] {len(resdata)}')
	return resdata

if __name__ == '__main__':
	TORQDBHOST = 'localhost'
	TORQDBUSER = 'torq'
	TORQDBPASS = 'dzt3f5jCvMlbUvRG'

	dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/torq?charset=utf8mb4"
	engine=create_engine(dburl)

	max_results = 10
	toptrips = pd.read_sql(f'select * from torqtrips order by distance desc limit {max_results}', engine)
	for trip in toptrips.id:
		td = get_trip_data(trip)
		print(f'td {len(td)} {type(td)}')

