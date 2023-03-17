# todo fix [IntegrityError] trip:63 gkpj pymysql.err.IntegrityError 1452 Cannot add or update a child row: a foreign key constraint fails (`torq`.`torqdata`, CONSTRAINT `torqdata_ibfk_1` FOREIGN KEY (`id`) REFERENCES `torqlogs` (`tripid`))') tripdate=2022-12-20 18:03:04
# todo fix only create tripdata for new trips
from psycopg2.errors import InvalidTextRepresentation

from hashlib import md5
from threading import Thread, active_count
from datetime import datetime
import pandas as pd
from loguru import logger
from pandas import DataFrame
from sqlalchemy import (BIGINT, BigInteger, Column, DateTime, Float, Integer,
                        MetaData, Numeric, String, Table, create_engine,
                        inspect, select, text)
from sqlalchemy.exc import (ArgumentError, CompileError, DataError, IntegrityError, OperationalError, ProgrammingError)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from torqsqlstrings import torqdatasql_psql, torqdatasql
from datamodels import TorqFile
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
			logger.error(f'[createtripdata] OperationalError tripid={trip} newtrip={newtrip} ')
			logger.error(e)
			logger.error(f'[e] {type(e)}')
			continue
		sql_tripdate = text(f'select tripdate from torqtrips where id={trip}')
		tripdate = [k._asdict() for k in session.execute(sql_tripdate).fetchall()]
		# logger.debug(f'[createtripdata] res={len(res)} {type(res)} res0={type(res[0])} tripdate={tripdate}')
		# res.insert(1, "tripdate", tripdate)
		if engine.name == 'mysql' or engine.name == 'sqlite':
			try:
				pd.DataFrame(res).to_sql('torqdata', engine, if_exists='append',  index=False)
				#_ = [pd.DataFrame(r).to_sql('torqdata', session, if_exists='append',  index=False) for r in res]
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
				pd.DataFrame(res).to_sql('torqdata', engine, if_exists='append', index=False)
			except InvalidTextRepresentation as e:
				logger.error(f'[createtripdata] {e} {type(e)} trip:{trip} tripdate={tripdate}')

def send_torqdata(tfid, dburl):
	engine = create_engine(dburl, echo=False)
	Session = sessionmaker(bind=engine)
	session = Session()

	#Session = sessionmaker(bind=engine)
	#session = Session()
	try:
		tf = session.query(TorqFile).filter(TorqFile.id == tfid).first()
	except OperationalError as e:
		logger.error(f'[sendtd] OperationalError {e} tripid={tfid}')
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
		res = pd.DataFrame([k for k in session.execute(text(sqlmagic)).all()])
	except OperationalError as e:
		logger.error(f'[sendtd] OperationalError tripid={tf.tripid} newtrip={tf} ')
		logger.error(e)
		logger.error(f'[e] {type(e)}')
		return None
	if len(res) == 0:
		logger.warning(f'[sendtd] no data for tripid={tf.tripid} newtrip={tf} ')
		return None
	sql_tripdate = text(f'select tripdate from torqtrips where id={tf.tripid}')
	tripdate_ = session.execute(sql_tripdate).one()._asdict().get('tripdate')
	if isinstance(tripdate_, str):
		try:
			tripdate = datetime.strptime(tripdate_[0][:-7],'%Y-%m-%d %H:%M:%S')
		except TypeError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} td={tripdate_} {type(tripdate_)}')
	else:
		tripdate = tripdate_
	try:
		res.insert(1, "tripdate", [tripdate for k in range(len(res))])
	except IndexError as e:
		logger.error(f'[sendtd] resinsert error:{e} insert tripdate trip:{tf} tripdate={tripdate} res={type(res)} {len(res)}')

	# tripdate = datetime.strptime(pdata_date ,'%a %b %d %H:%M:%S %Z%z %Y')
	# tripdate = datetime.strptime(tripdict['tripdate'],'%Y-%m-%d %H:%M:%S')
	if engine.name == 'mysql' or engine.name == 'sqlite':
		try:
			pd.DataFrame(res).to_sql('torqdata', engine, if_exists='append',  index=False)
		except OperationalError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
		except AttributeError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
		except IntegrityError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
		except ValueError as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
	elif engine.name == 'postgresql':
		try:
			pd.DataFrame(res).to_sql('torqdata', engine, if_exists='append', index=False)
		except InvalidTextRepresentation as e:
			logger.error(f'[sendtd] {e} {type(e)} trip:{tf} tripdate={tripdate}')
	# logger.info(f'[torqdata] tfid={tf.id} tripdate={tripdate}  tf={tf}')
	engine.dispose()
