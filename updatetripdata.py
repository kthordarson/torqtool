# todo fix only create tripdata for new trips
import pandas as pd
from datetime import datetime
from loguru import logger
from sqlalchemy import (text,
)
from sqlalchemy.exc import (OperationalError,
)

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
	session.execute(text("drop table if exists columnstats"))
	total_rows = pd.DataFrame(session.execute(text("select count(*) from torqlogs"))).values[0][0]
	logger.info(f"{total_rows} in db")
	for idx, column in enumerate(schema_datatypes):
		try:
			nulls = pd.DataFrame(session.execute(text(f"select count(*) as count from torqlogs where {column} is null")).all()).values[0][0]
			notnulls = total_rows - nulls
			# dfval = df.values[0][0]
			logger.debug(f"[{idx}/{len(schema_datatypes)}-{len(results)}] {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} ratio: {notnulls/total_rows}")
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
	q = "select fileid,avg(gpsspeedkmh) as gpsspeedkmh, avg(speedobdkmh) as speedobdkmh, avg(speedgpskmh) as speedgpskmh, min(gpstime) as gpstime  from torqlogs where gpsspeedkmh is not null and gpsspeedkmh>0 and speedobdkmh is not null and speedobdkmh>0  and speedgpskmh is not null and speedgpskmh>0 group by fileid "
	# oldq = 'select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'
	if args.db_limit:
		q += f" limit {args.limit}"
	df = pd.DataFrame(session.execute(text(q)).all()).fillna(0)
	logger.info(f"dbspeeds:{df.describe()}")
	# res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))
	logger.info(f"dbspeeds: dfres {df.to_sql(name='speeds', con=engine, if_exists='replace')}")
	return 0


def collect_db_startends(args):
	engine, session = get_engine_session(args)
	session.execute(text("drop table if exists startends"))
	if not args.db_limit:
		df = pd.DataFrame(session.execute(text("select fileid as fileid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs group by fileid;")).all()).fillna(0)
	else:
		df = pd.DataFrame(session.execute(text(f"select fileid as fileid,min(latitude) as latmin,min(longitude) as lonmin,max(latitude) as latmax,max(longitude) as lonmax from torqlogs group by fileid limit {args.db_limit};")).all()).fillna(0)
	logger.info(f"dbstartends: {df.describe()}")
	# res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))
	logger.info(f"dbstartends dfres: {df.to_sql(name='startends', con=engine, if_exists='replace')}")
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
