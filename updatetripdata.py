#!/usr/bin/python3
# todo fix only create tripdata for new trips
import pandas as pd
from datetime import datetime
from loguru import logger
import sys
from sqlalchemy import (text)
from sqlalchemy.exc import (OperationalError,)
from utils import get_parser
from utils import get_engine_session
from schemas import schema_datatypes


def send_torqdata(tfid, dburl, debug=False):
	logger.warning("not implemented")
	return None


def collect_db_filestats(args, todatabase=True, droptable=True):
	# todo fix this is very slow
	engine, session = get_engine_session(args)
	if droptable:
		session.execute(text("drop table if exists filestats"))
	if args.dbmode == "sqlite":
		session.execute(text("PRAGMA journal_mode=WAL;"))
		session.execute(text("pragma synchronous = normal;"))
		session.execute(text("pragma temp_store = memory;"))
		session.execute(text("pragma mmap_size = 30000000000;"))
		# session.execute(text('pragma journal_mode = memory;'))
	q = "select fileid from torqfiles where error_flag=0"
	if args.db_limit:
		q += f" limit {args.db_limit}"
	file_ids = pd.DataFrame(session.execute(text(q)))
	logger.debug(f"fileids={len(file_ids)} ")
	results = []
	for fileidx, file in enumerate(file_ids.itertuples()):
		if args.extradebug:
			logger.debug(f"[{fileidx}/{len(file_ids)}] working on fileid {file.fileid} ")
		# results[file.fileid] = []
		total_rows = pd.DataFrame(session.execute(text(f"select count(*) from torqlogs where  fileid={file.fileid}"))
		).values[0][0]  # id>0 and
		if total_rows == 0:
			logger.warning(f"no rows for {file.fileid}")
			continue
		else:
			logger.info(f"total_rows={total_rows} for {file.fileid}")
		for idx, column in enumerate(schema_datatypes):
			if args.extradebug:
				logger.debug(f"[{fileidx}/{len(file_ids)}] fileid {file.fileid}  col: {column} ")
			nulls = pd.DataFrame(session.execute(text(f"select count(*) as count from torqlogs where  fileid={file.fileid} and {column} is null ")).all()).values[0][0]  # id>0 and
			notnulls = total_rows - nulls
			# dfval = df.values[0][0]
			if args.extradebug and nulls > 0:
				logger.debug(f"[{fileidx}/{len(file_ids)}/{idx}/{len(schema_datatypes)}] {file.fileid} - {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} ratio: {notnulls/total_rows}")

			results.append({
					"fileid": file.fileid,
					"column": column,
					"nulls": nulls,
					"nullratio": nulls / total_rows,
				}
			)
		logger.info(f"[{fileidx}/{len(file_ids)}] {file.fileid} ")
	df = pd.DataFrame([k for k in results])
	try:
		if todatabase:
			df.to_sql(con=engine, name="filestats", if_exists="append")
			logger.debug(f"Sent filestats for {file.fileid} to db...")
		else:
			# logger.debug(f'returning {len(df)} filestats ...')
			return df
	except Exception as e:
		logger.error(f"{type(e)} {e} for\n{df=}\n {results=}\n")
		return None


def create_db_filestats(data: pd.DataFrame, fileid: int, args=None):
	# todo fix this is very slow
	t0 = datetime.now()
	engine, session = get_engine_session(args)
	results = []
	nulls = 0
	notnulls = 0
	# file = pd.DataFrame(session.execute(text(f'select * from torqfiles where fileid={fileid}'))).values[0][0]
	# results[file.fileid] = []
	total_rows = pd.DataFrame(session.execute(text(f"select count(*) from torqlogs where fileid={fileid}"))).values[0][0]  # where id>0 and
	if total_rows == 0:
		logger.warning(f"no rows for {fileid}")
		return None
	if args.extradebug:
		logger.info(f"create_db_filestats for {fileid=} {total_rows=}")
	for idx, column in enumerate(schema_datatypes):
		if args.extradebug:
			pass  # logger.debug(f"fileid {fileid}  col: {column} ")
		nulls = pd.DataFrame(session.execute(text(f"select count(*) as count from torqlogs where  fileid={fileid} and {column} is null ")).all()).values[0][0]  # id>0 and
		notnulls = total_rows - nulls
		# dfval = df.values[0][0]
		if args.extradebug and nulls > 0:
			pass  # logger.debug(f"[{idx}/{len(schema_datatypes)}] {fileid} - {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} ratio: {notnulls/total_rows}")
		results.append({"fileid": fileid, "column": column, "nulls": nulls, "nullratio": nulls / total_rows, })
	df = pd.DataFrame([k for k in results])
	# logger.debug(f"t: {(datetime.now()-t0).seconds} scanpath returned {len(results)} files")
	logger.info(f"total_rows={total_rows} for {fileid} df: {len(df)} t: {(datetime.now()-t0).seconds} nulls: {nulls}/{notnulls}")
	return df

def collect_db_columnstats(args):
	engine, session = get_engine_session(args)
	results = {}
	# session.execute(text("drop table if exists columnstats"))
	t0 = datetime.now()
	total_rows = pd.DataFrame(session.execute(text("select count(*) from torqlogs"))).values[0][0]
	logger.info(f"{total_rows} in db t0: {(datetime.now()-t0).seconds}")
	for idx, column in enumerate(schema_datatypes):
		t1 = datetime.now()
		try:
			nulls = pd.DataFrame(session.execute(text(f"select count(*) as count from torqlogs where {column} is null")).all()).values[0][0]
			notnulls = total_rows - nulls
			# dfval = df.values[0][0]
			if nulls / total_rows > 0.5:
				logger.warning(f"[{idx}/{len(schema_datatypes)}-{len(results)}] t0: {(datetime.now()-t0).seconds} t1: {(datetime.now()-t1).seconds} {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} nlr: {notnulls/total_rows}")
			results[column] = {"column": column, "nulls": nulls, "nullratio": nulls / total_rows,}
		except (OperationalError,) as e:
			logger.warning(f"{type(e)} {e} for {column}")
		except Exception as e:
			logger.error(f"{type(e)} {e} for {column}")
	df = pd.DataFrame([results[k] for k in results])
	try:
		df.to_sql(con=engine, name="columnstats", if_exists="replace")
	except Exception as e:
		logger.error(f"{type(e)} {e} for {df=} {results=}")
	logger.info(f"t0: {(datetime.now()-t0).seconds} columnstats: {len(df)}")
	return results


def collect_db_speeds(args):
	engine, session = get_engine_session(args)
	try:
		pass  # session.execute(text('drop table if exists speeds;'))
	except Exception as e:
		logger.error(f"{type(e)} {e}")
		session.rollback()
		return -1
	# res = session.execute(text('drop table speeds'))
	# print(res)
	# q = "select fileid,avg(gpsspeedkmh) as gpsspeedkmh, avg(speedobdkmh) as speedobdkmh, avg(speedgpskmh) as speedgpskmh, min(gpstime) as gpstime  from torqlogs where gpsspeedkmh is not null and gpsspeedkmh>0 and speedobdkmh is not null and speedobdkmh>0  and speedgpskmh is not null and speedgpskmh>0 group by fileid "
	q = 'select fileid,avg(gpsspeedkmh) as gpsspeedkmh, avg(speedobdkmh) as speedobdkmh, avg(speedgpskmh) as speedgpskmh, min(gpstime) as gpstime  from torqlogs group by fileid '
	# oldq = 'select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'
	if args.db_limit:
		q += f" limit {args.limit}"
	df = pd.DataFrame(session.execute(text(q)).all()).fillna(0)
	logger.info(f"dbspeeds:{df.describe()}")
	# res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))
	df = df.to_sql(name='speeds', con=engine, if_exists='replace')
	logger.info(f"dbspeeds: dfres {df}")
	return 0

def collect_db_startends(args):
	engine, session = get_engine_session(args)
	gpsoffset_1 = 0.00064
	gpsoffset_2 = gpsoffset_1 / 4
	gpsoffset_3 = gpsoffset_2 / 2
	gps_offsets = {'count1': gpsoffset_1, 'count2': gpsoffset_2, 'count3': gpsoffset_3}
	session.execute(text('DROP TABLE IF EXISTS startends'))
	session.commit()
	session.execute(text('CREATE TABLE startends (posid INT, latmin FLOAT, lonmin FLOAT, latmax FLOAT, lonmax FLOAT, count1 INT, count2 INT, count3 INT)'))
	session.commit()

	file_ids = pd.DataFrame(session.execute(text("SELECT fileid FROM torqfiles")).all()).fillna(0)
	cnt = 0

	for file in file_ids.itertuples():
		df = pd.DataFrame(columns=['latmin', 'lonmin', 'latmax', 'lonmax', 'count1', 'count2', 'count3'])
		query = f"""
			SELECT
				MIN(latitude) FILTER (WHERE gpstime = first_gpstime) AS latmin,
				MIN(longitude) FILTER (WHERE gpstime = first_gpstime) AS lonmin,
				MIN(latitude) FILTER (WHERE gpstime = last_gpstime) AS latmax,
				MIN(longitude) FILTER (WHERE gpstime = last_gpstime) AS lonmax
			FROM (
				SELECT
					latitude,
					longitude,
					gpstime,
					FIRST_VALUE(gpstime) OVER (PARTITION BY fileid ORDER BY gpstime ASC) AS first_gpstime,
					FIRST_VALUE(gpstime) OVER (PARTITION BY fileid ORDER BY gpstime DESC) AS last_gpstime
				FROM torqlogs
				WHERE latitude != 0 AND longitude != 0 AND fileid = {file.fileid}
			) subquery
			WHERE gpstime = first_gpstime OR gpstime = last_gpstime
		"""
		data = pd.DataFrame(session.execute(text(query)).all())

		try:
			df['latmin'] = data['latmin']
		except Exception as e:
			logger.error(f"{type(e)} {e}")
			logger.error(f"{data=}")
			raise e

		df['lonmin'] = data['lonmin']
		df['latmax'] = data['latmax']
		df['lonmax'] = data['lonmax']
		countvalue = None
		for kcolumn, value in gps_offsets.items():
			df[kcolumn] = 0
			try:
				df_count = pd.DataFrame(session.execute(text(f"""
					SELECT COUNT(*) AS {kcolumn}
					FROM (
						SELECT DISTINCT fileid
						FROM torqlogs
						WHERE latitude !=0 and latitude BETWEEN {data.latmin.values[0] - value} AND {data.latmin.values[0] + value}
						AND longitude !=0 and longitude BETWEEN {data.lonmax.values[0] - value} AND {data.lonmax.values[0] + value}
					) AS temp
				""")).all())
				countvalue = int(df_count.values[0][0])
				df[kcolumn] = countvalue
				logger.debug(f"latlons=[ {data.latmin.values[0]} {data.lonmin.values[0]} {data.latmax.values[0]} {data.lonmax.values[0]} ] count = {countvalue} [{kcolumn} {value}]")
			except TypeError as e:
				logger.error(f"{type(e)} {e} {data=}")
				# continue
		if sum(df.count1+df.count2+df.count3) > 0:
			logger.info(f'[{file.fileid}] {len(df)} {cnt}')
			cnt += 1
			if cnt >= 3:
				pass
			df['posid'] = cnt
			df.reset_index(drop=True, inplace=True)
			dfx = df.to_sql(name='startends', con=engine, if_exists='append', index=False)
			logger.info(f'tosqldone  {dfx}')
		else:
			logger.warning(f"sum {sum(df.count1+df.count2+df.count3)}")

	return 0

def oldcollect_db_startends(args):
	engine, session = get_engine_session(args)
	gpsoffset_1 = 0.00064
	gpsoffset_2 = gpsoffset_1/4
	gpsoffset_3 = gpsoffset_2/2
	gps_offsets = {'count1':gpsoffset_1, 'count2':gpsoffset_2, 'count3': gpsoffset_3}
	session.execute(text('drop table if exists startends'))
	session.commit()
	session.execute(text('create table startends (index int, posid int ,latmin float,lonmin float ,latmax float,lonmax float ,count1 int ,count2 int ,count3 int)'))
	session.commit()
	# session.execute(text("drop table if exists startends"))
	file_ids = pd.DataFrame(session.execute(text("select fileid from torqfiles ")).all()).fillna(0)
	# get start/end pos for each fileid
	cnt = 0
	for file in file_ids.itertuples():
		# df = pd.DataFrame(columns=['latmin', 'lonmin', 'latmax', 'lonmax', 'count1', 'count2', 'count3'])
		# df.index.name = 'posid'
		df = pd.DataFrame(columns=['latmin', 'lonmin', 'latmax', 'lonmax', 'count1', 'count2', 'count3'])
		# mindata = pd.DataFrame(session.execute(text(f'SELECT latitude as latmin,longitude as lonmin FROM torqlogs WHERE latitude != 0 and fileid={file.fileid} ORDER BY gpstime ASC LIMIT 1'))).all()
		mindata = pd.DataFrame(session.execute(text(f'SELECT latitude as latmin,longitude as lonmin FROM torqlogs WHERE latitude != 0 and fileid={file.fileid} ORDER BY gpstime ASC LIMIT 1')).all())
		maxdata = pd.DataFrame(session.execute(text(f'SELECT latitude as latmax,longitude as lonmax FROM torqlogs WHERE latitude != 0 and fileid={file.fileid} ORDER BY gpstime DESC LIMIT 1')).all())
		try:
			df['latmin'] = mindata['latmin']
		except Exception as e:
			logger.error(f"{type(e)} {e}")
			logger.error(f"{mindata=}")
			continue
		df['lonmin'] = mindata['lonmin']
		df['latmax'] = maxdata['latmax']
		df['lonmax'] = maxdata['lonmax']
		for kcolumn, value in gps_offsets.items():
			df_count = pd.DataFrame(session.execute(text(f'select count(*) as {kcolumn} from (select distinct fileid from  torqlogs where  latitude between {mindata.latmin.values[0]-value} and {mindata.latmin.values[0]+value} and longitude between {maxdata.lonmax.values[0]-value} and {maxdata.lonmax.values[0]+value} ) as temp')).all())
			countvalue = int(df_count.values[0][0])
			df[kcolumn] = countvalue
			logger.debug(f"latlons=[ {mindata.latmin.values[0]} {mindata.lonmin.values[0]} {maxdata.latmax.values[0]} {maxdata.lonmax.values[0]} ] count = {countvalue} [{kcolumn} {value}]")
		# df = pd.concat((df0, df), axis=0)
		# df['posid'] = cnt
		# fd = pd.concat((mindata,maxdata),axis=1)
		# fd = pd.DataFrame((k for k in fd.itertuples()),index=['posid'])
		# fd['posid'] = file.Index
		# df = pd.concat((df,fd),axis=0)
		logger.info(f'[{file.fileid}] {len(df)} {cnt}')
		cnt += 1
		if cnt >= 3:
			pass # break
		# lastq = f'SELECT latitude as latmax,longitude as lonmax FROM torqlogs WHERE latitude != 0 and fileid={file.fileid} ORDER BY gpstime DESC LIMIT 1'
		# total_rows = pd.DataFrame(session.execute(text(f"select count(*) from torqlogs where  fileid={file.fileid}")).all()).values[0][0]
		# df = df.reset_index(drop=True)
		logger.info(f'tosql {len(df)} {df}')
		# session.close()
		# engine, session = get_engine_session(args)
		dfx = df.to_sql(name='startends', con=engine, if_exists='append', index=True, index_label='posid')
		logger.info(f'tosqldone  {dfx}')
	# if not args.db_limit:
	# 	df = pd.DataFrame(session.execute(text("select fileid as posid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs where latitude != 0 group by fileid ")).all()).fillna(0)
	# else:
	# 	df = pd.DataFrame(session.execute(text(f"select fileid as posid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs where latitude != 0  group by fileid limit {args.db_limit};")).all()).fillna(0)
	# logger.info(f"dbstartends: {len(df)}")
	# res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))


def xxxcollect_db_startends(args):
	engine, session = get_engine_session(args)
	# session.execute(text("drop table if exists startends"))
	firstq = 'SELECT latitude as latmin,longitude as lonmin FROM torqlogs WHERE latitude != 0 ORDER BY gpstime ASC LIMIT 1'
	lastq = 'SELECT latitude as latmax,longitude as lonmax FROM torqlogs WHERE latitude != 0 ORDER BY gpstime DESC LIMIT 1'
	if not args.db_limit:
		df = pd.DataFrame(session.execute(text("select fileid as posid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs where latitude != 0 group by fileid ")).all()).fillna(0)
	else:
		df = pd.DataFrame(session.execute(text(f"select fileid as posid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs where latitude != 0  group by fileid limit {args.db_limit};")).all()).fillna(0)
	logger.info(f"dbstartends: {len(df)}")
	# res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))
	df = df.to_sql(name='startends', con=engine, if_exists='replace')
	logger.info(f"dbstartends dfres: {df}")
	try:
		session.execute(text('alter table startends add column if not exists count1 int;'))
		session.commit()
		session.execute(text('alter table startends add column if not exists count2 int;'))
		session.commit()
		session.execute(text('alter table startends add column if not exists count3 int;'))
		session.commit()
	except Exception as e:
		logger.error(f"{type(e)} {e}")
		session.rollback()
	df = pd.DataFrame(session.execute(text('select * from startends')).all())
	logger.info(f"dbstartends: {len(df)}")
	gpsoffset_1 = 0.00064
	gpsoffset_2 = gpsoffset_1/4
	gpsoffset_3 = gpsoffset_2/2
	gps_offsets = {'count1':gpsoffset_1, 'count2':gpsoffset_2, 'count3': gpsoffset_3}
	for d in df.iterrows():
		x = d[1]
		for kcolumn, value in gps_offsets.items():
			dfxx = pd.DataFrame(session.execute(text(f'select count(*) from (select distinct fileid from  torqlogs where  latitude between {x.latmin-value} and {x.latmin+value} and longitude between {x.lonmin-value} and {x.lonmax+value} ) as temp')).all())
			dfval = dfxx.values[0][0]
			xfid = int(x.posid)
			logger.debug(f"posid {xfid} latlons=[ {x.latmin} {x.lonmin} {x.latmax} {x.lonmax} ] count = {dfval} {kcolumn} {value}")
			session.execute(text(f'update startends set {kcolumn}={dfval} where posid={xfid}'))
			session.commit()
		# dfxx = pd.DataFrame(session.execute(text(f'select count(*) from (select distinct fileid from  torqlogs where  latitude between {x.latmin-gpsoffset} and {x.latmin+gpsoffset} and longitude between {x.lonmin-gpsoffset} and {x.lonmax+gpsoffset} ) as temp')).all())
		# dfval = dfxx.values[0][0]
		# xfid = int(x.fileid)
		# logger.debug(f"fileid {xfid} latlons=[ {x.latmin} {x.lonmin} {x.latmax} {x.lonmax} ] count = {dfval}")
		# session.execute(text(f'update startends set count={dfval} where fileid={xfid}'))
		# session.commit()
	return 0


def xcollect_db_startends(args):
	engine, session = get_engine_session(args)
	df = pd.DataFrame(session.execute(text('select * from startends')).all())
	logger.info(f"dbstartends: {len(df)}")
	gpsoffset = 0.001
	for d in df.iterrows():
		x = d[1]
		dfxx = pd.DataFrame(session.execute(text(f'select count(*) from (select distinct fileid from  torqlogs where  latitude between {x.latmin-gpsoffset} and {x.latmin+gpsoffset} and longitude between {x.lonmin-gpsoffset} and {x.lonmax+gpsoffset} ) as temp')).all())
		dfval = dfxx.values[0][0]
		xfid = int(x.fileid)
		logger.debug(f"fileid {xfid} latlons=[ {x.latmin} {x.lonmin} {x.latmax} {x.lonmax} ] count = {dfval}")
		q = text(f'update startends set count={dfval} where fileid={xfid}')
		session.execute(q)
	return 0


def main(args):
	engine, session = get_engine_session(args)
	if args.dbmode == "sqlite":
		session.execute(text("PRAGMA journal_mode=WAL;"))
		session.execute(text("pragma synchronous = normal;"))
		session.execute(text("pragma temp_store = memory;"))
		session.execute(text("pragma mmap_size = 30000000000;"))
	if args.db_filestats:
		return collect_db_filestats(args)
	elif args.db_columnstats:
		return collect_db_columnstats(args)
	elif args.db_startends:
		return collect_db_startends(args)
	elif args.db_speed:
		return collect_db_speeds(args)
	elif args.db_allstats:
		logger.debug("starting all stats")
		dbspeed = collect_db_speeds(args)
		logger.debug("all stats dbspeed done")
		dbstartends = collect_db_startends(args)
		logger.debug("all stats dbstartends done")
		dbcolumstats = collect_db_columnstats(args)
		logger.debug("all stats dbcolumstats done")
		dbfilestats = collect_db_filestats(args)
		logger.debug("all stats dbfilestats done")
		return {
			"dbspeed": dbspeed,
			"dbstartends": dbstartends,
			"dbcolumstats": dbcolumstats,
			"dbfilestats": dbfilestats,
		}
	else:
		logger.warning("missing args")


if __name__ == "__main__":
	parser = get_parser("dataupdate")
	parser.add_argument("--db_speed",
		default=False,
		help="db_speed",
		action="store_true",
		dest="db_speed",
	)
	parser.add_argument("--db_startends",
		default=False,
		help="db_startends",
		action="store_true",
		dest="db_startends",
	)
	parser.add_argument("--db_columnstats",
		default=False,
		help="db_columnstats",
		action="store_true",
		dest="db_columnstats",
	)
	parser.add_argument("--db_filestats",
		default=False,
		help="db_filestats",
		action="store_true",
		dest="db_filestats",
	)
	parser.add_argument("--db_allstats",
		default=False,
		help="db_allstats",
		action="store_true",
		dest="db_allstats",
	)

	args = parser.parse_args()
	try:
		r = main(args)
		print(f"[main] got {type(r)}")
	except Exception as e:
		logger.error(f"unhandled {type(e)} {e}")
		sys.exit(-1)
