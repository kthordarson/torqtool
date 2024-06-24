# todo fix only create tripdata for new trips

from datetime import datetime

import pandas as pd
import polars as pl
from loguru import logger
from sqlalchemy import (
	create_engine,
	inspect,
	text,
)
from sqlalchemy.exc import (
	IntegrityError,
	OperationalError,
	ProgrammingError,
)
from sqlalchemy.orm import sessionmaker

from datamodels import Torqdata, TorqFile, Torqlogs
from converter import get_parser
from utils import get_engine_session
from schemas import schema_datatypes

def create_tripdata(engine, session, newfilelist):
	#torqtrips = pd.read_sql(f'select id from torqtrips', session)
	for newtrip in newfilelist:
		# logger.debug(f'[updatetrip] id={trip}')
		trip = newtrip.id
		if engine.name == 'postgresql':
			sqlmagic = ''
		elif engine.name == 'mysql':
			sqlmagic = ''
		elif engine.name == 'sqlite':
			sqlmagic = ''
		try:
			res = [k for k in session.execute(text(sqlmagic)).all()]
		except OperationalError as e:
			logger.error(f'[createtripdata] OperationalError code={e} args={e.args[0]}  newtrip={newtrip} ')
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
	logger.warning('not implemented')
	return None
def xsend_torqdata(tfid, dburl, debug=False):
	engine = create_engine(dburl, echo=False)
	insp = inspect(engine)
	Session = sessionmaker(bind=engine)
	session = Session()

	#Session = sessionmaker(bind=engine)
	#session = Session()
	try:
		tf = session.query(TorqFile).filter(TorqFile.fileid == tfid).first()
		if debug:
			logger.debug(f'{tfid=} {tf=}')
	except OperationalError as e:
		logger.error(f'[sendtd] OperationalError code={e} args={e.args[0]} ')
		return None
	except ProgrammingError as e:
		logger.error(f'[sendtd] ProgrammingError {e} ')
		return None
	if engine.name == 'postgresql':
		sqlmagic = ''
	elif engine.name == 'mysql':
		sqlmagic = ''
	elif engine.name == 'sqlite':
		sqlmagic = ''
	try:
		# p = pd.DataFrame([f._mapping for f in foo])
		# get columns
		# insp.get_columns('torqfiles')
		# res = pl.DataFrame([k for k in session.execute(text(sqlmagic)).all()])
		# SELECT * FROM information_schema.columns WHERE TABLE_NAME = ''
		res = pl.DataFrame(pd.DataFrame([k for k in session.execute(text(sqlmagic)).all()]))
	except OperationalError as e:
		logger.error(f'[sendtd] OperationalError code={e} args={e.args[0]}  newtrip={tf} ')
		return None
	if len(res) == 0:
		logger.warning(f'[sendtd] no data for newtrip={tf}\nres:{res}')
		return None
	sql_tripdate = text(f'select tripdate from torqtrips where id={tf.fileid}')
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
		logger.info(f'[torqdata] tfid={tf.fileid} tripdate={tripdate}  tf={tf}')
	engine.dispose()

def collect_db_filestats(args):
	engine, session = get_engine_session(args)
	session.execute(text('drop table if exists filestats'))
	if args.dbmode == 'sqlite':
		session.execute(text('PRAGMA journal_mode=WAL;'))
		session.execute(text('pragma synchronous = normal;'))
		session.execute(text('pragma temp_store = memory;'))
		session.execute(text('pragma mmap_size = 30000000000;'))
		#session.execute(text('pragma journal_mode = memory;'))
	if not args.db_limit:
		file_ids = pd.DataFrame(session.execute(text('select fileid from torqfiles ')))
	else:
		file_ids = pd.DataFrame(session.execute(text(f'select fileid from torqfiles limit {args.db_limit}')))
	logger.debug(f'fileids={len(file_ids)} ')
	results = []
	for fileidx, file in enumerate(file_ids.itertuples()):
		if args.extradebug:
			logger.debug(f'[{fileidx}/{len(file_ids)}] working on fileid {file.fileid} ')
		#results[file.fileid] = []
		total_rows = pd.DataFrame(session.execute(text(f'select count(id) from torqlogs where id>0 and fileid={file.fileid}'))).values[0][0]
		if total_rows == 0:
			logger.warning(f'no rows for {file.fileid}')
			continue
		else:
			logger.info(f'total_rows={total_rows} for {file.fileid}')
		for idx,column in enumerate(schema_datatypes):
			if args.extradebug:
				pass # logger.debug(f'[{fileidx}/{len(file_ids)}] fileid {file.fileid}  col: {column} ')
			nulls = pd.DataFrame(session.execute(text(f'select count(id) as count from torqlogs where id>0 and fileid={file.fileid} and {column} is null ')).all()).values[0][0]
			notnulls = total_rows - nulls
			#dfval = df.values[0][0]
			if args.extradebug and nulls>0:
				logger.debug(f'[{fileidx}/{len(file_ids)}/{idx}/{len(schema_datatypes)}] {file.fileid} - {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} ratio: {notnulls/total_rows}')

			results.append( {'fileid': file.fileid, 'column':column, 'nulls':nulls, 'nullratio':nulls/total_rows})
		logger.info(f'[{fileidx}/{len(file_ids)}] {file.fileid} ')
	df = pd.DataFrame([k for k in results])
	logger.debug(f'sending {len(df)} filestats to db...')
	try:
		df.to_sql(con=engine, name='filestats',if_exists='append')
	except Exception as e:
		logger.error(f'{type(e)} {e} for\n{df=}\n {results=}\n')
	return df
	
def collect_db_columnstats(args):
	engine, session = get_engine_session(args)
	results = {}
	session.execute(text('drop table if exists columnstats'))
	total_rows = pd.DataFrame(session.execute(text('select count(id) from torqlogs'))).values[0][0]
	logger.info(f'{total_rows} in db')
	for idx,column in enumerate(schema_datatypes):
		try:
			nulls = pd.DataFrame(session.execute(text(f'select count(id) as count from torqlogs where {column} is null')).all()).values[0][0]
			notnulls = total_rows - nulls
			#dfval = df.values[0][0]
			logger.debug(f'[{idx}/{len(schema_datatypes)}-{len(results)}] {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} ratio: {notnulls/total_rows}')
			results[column] = {'column':column, 'nulls':nulls, 'nullratio':nulls/total_rows}
		except (OperationalError,) as e:
			logger.warning(f'{type(e)} {e} for {column}')
		except Exception as e:
			logger.error(f'{type(e)} {e} for {column}')
	df = pd.DataFrame([results[k] for k in results])
	try:
		df.to_sql(con=engine, name='columnstats',if_exists='replace')
	except Exception as e:
		logger.error(f'{type(e)} {e} for {df=} {results=}')
	return results

def collect_db_speeds(args):
	engine, session = get_engine_session(args)
	session.execute(text('drop table if exists speeds'))
	#res = session.execute(text('drop table speeds'))
	#print(res)
	if not args.db_limit:
		df = pd.DataFrame(session.execute(text('select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid')).all()).fillna(0)
	else:
		df = pd.DataFrame(session.execute(text(f'select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid limit {args.db_limit}')).all()).fillna(0)
	logger.info(f'dbspeeds:{df.describe()}')
	# res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))
	logger.info(f"dbspeeds: dfres {df.to_sql(name='speeds', con=engine, if_exists='replace')}")
	return 0
	# create table startstops as select fileid as fileid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs group by fileid;

def collect_db_startends(args):
	engine, session = get_engine_session(args)
	session.execute(text('drop table if exists startends'))
	if not args.db_limit:
		df = pd.DataFrame(session.execute(text('select fileid as fileid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs group by fileid;')).all()).fillna(0)
	else:
		df = pd.DataFrame(session.execute(text(f'select fileid as fileid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs group by fileid limit {args.db_limit};')).all()).fillna(0)
	logger.info(f"dbstartends: {df.describe()}")
	# res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))
	logger.info(f"dbstartends dfres: {df.to_sql(name='startends', con=engine, if_exists='replace')}")
	return 0

def main(args):
	engine, session = get_engine_session(args)
	if args.dbmode == 'sqlite':
		session.execute(text('PRAGMA journal_mode=WAL;'))
		session.execute(text('pragma synchronous = normal;'))
		session.execute(text('pragma temp_store = memory;'))
		session.execute(text('pragma mmap_size = 30000000000;'))
	print(args)
	if args.db_filestats:
		return collect_db_filestats(args)
	elif args.db_columnstats:
		return collect_db_columnstats(args)
	elif args.db_startends:
		return collect_db_startends(args)
	elif args.db_speed:
		return collect_db_speeds(args)
	elif args.db_allstats:
		logger.debug(f'starting all stats')
		dbspeed = collect_db_speeds(args)
		logger.debug(f'all stats dbspeed done')
		dbstartends = collect_db_startends(args)
		logger.debug(f'all stats dbstartends done')
		dbcolumstats = collect_db_columnstats(args)
		logger.debug(f'all stats dbcolumstats done')
		dbfilestats = collect_db_filestats(args)
		logger.debug(f'all stats dbfilestats done')
		return {'dbspeed':dbspeed, 'dbstartends':dbstartends, 'dbcolumstats':dbcolumstats, 'dbfilestats':dbfilestats}
	else:
		logger.warning(f'missing args')
if __name__ == '__main__':
	parser = get_parser('dataupdate')
	parser.add_argument('--db_speed', default=False, help="db_speed", action="store_true", dest='db_speed')
	parser.add_argument('--db_startends', default=False, help="db_startends", action="store_true", dest='db_startends')
	parser.add_argument('--db_columnstats', default=False, help="db_columnstats", action="store_true", dest='db_columnstats')
	parser.add_argument('--db_filestats', default=False, help="db_filestats", action="store_true", dest='db_filestats')
	parser.add_argument('--db_allstats', default=False, help="db_allstats", action="store_true", dest='db_allstats')
	
	args = parser.parse_args()
	try:
		r = main(args)
		print(f'[main] got {type(r)}')
	except Exception as e:
		logger.error(f'unhandled {type(e)} {e}')

