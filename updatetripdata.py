#!/usr/bin/python3
# todo fix only create tripdata for new trips
import pandas as pd
import argparse
from datetime import datetime
from loguru import logger
import sys
from sqlalchemy import (text)
from sqlalchemy.exc import (OperationalError, DuplicateColumnError)
from utils import get_parser, get_engine_session, convert_string_to_datetime
from schemas import schema_datatypes, dataschema
from datamodels import TorqFile, Startpos, Endpos

def send_torqdata(tfid, dburl, debug=False):
	logger.warning("not implemented")
	return None


def collect_db_filestats(args, todatabase=True, droptable=True):
	# todo fix this is very slow
	engine, session = get_engine_session(args)
	# if droptable:
	# 	session.execute(text("drop table if exists filestats"))
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
		total_rows = pd.DataFrame(session.execute(text(f"select count(*) from torqlogs where  fileid={file.fileid}"))).values[0][0]  # id>0 and
		if total_rows == 0:
			logger.warning(f"no rows for {file.fileid}")
			continue
		else:
			logger.info(f"total_rows={total_rows} for {file.fileid}")
		column_list = [(idx,k) for idx,k in enumerate(dataschema) if k not in ['gpstime','devicetime']]
		for idx, column in enumerate(column_list):
			if args.extradebug:
				logger.debug(f"[{fileidx}/{len(file_ids)}] fileid {file.fileid}  col: {column} ")
			nulls = pd.DataFrame(session.execute(text(f"select count(*) as count from torqlogs where  fileid={file.fileid} and {column} is null ")).all()).values[0][0]  # id>0 and
			notnulls = total_rows - nulls
			# dfval = df.values[0][0]
			if args.extradebug and nulls > 0:
				logger.debug(f"[{fileidx}/{len(file_ids)}/{idx}/{len(column_list)}] {file.fileid} - {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} ratio: {notnulls/total_rows}")

			result = ({
					"fileid": file.fileid,
					"column": column,
					"nulls": nulls,
					"nullratio": nulls / total_rows,
				}
			)
			results.append(result)
		logger.info(f"[{fileidx}/{len(file_ids)}] {file.fileid} ")

def send_db_filestats(args, todatabase=True, droptable=True, results=None):
	engine, session = get_engine_session(args)
	df = pd.DataFrame([k for k in results])
	try:
		if todatabase:
			df.to_sql(con=engine, name="replace", if_exists="append")
			logger.debug(f"Sent filestats for {file.fileid} to db...")
		else:
			# logger.debug(f'returning {len(df)} filestats ...')
			return df
	except Exception as e:
		logger.error(f"{type(e)} {e} for\n{df=}\n {results=}\n")
		return None


def oldspupdates(args: argparse.Namespace, fileinfo: dict):
	engine, session = get_engine_session(args)
	fileid = fileinfo.get("fileid", None)
	torqfile = session.query(TorqFile).filter(TorqFile.fileid == fileid).first()
	total_rows_db = int(pd.DataFrame(session.execute(text(f"select count(*) from torqlogs where fileid={torqfile.fileid}"))).values[0][0])  # where id>0 and
	datemin = pd.DataFrame(session.execute(text(f"select gpstime,latitude as latstart,longitude as lonstart from torqlogs where fileid={torqfile.fileid} order by gpstime asc limit 1 ")))
	datemax = pd.DataFrame(session.execute(text(f"select gpstime,latitude as latend, longitude as lonend from torqlogs where fileid={torqfile.fileid} order by gpstime desc limit 1 ")))
	#start_pos = session.execute(text(f'select fileid,latitude as latstart,longitude as lonstart from torqlogs where fileid={torqfile.fileid} order by gpstime asc limit 1')).one()
	#start_pos = datemin.values[0]
	# todo check if startpos exists before creating new
	start_pos = {'latstart': float(datemin.loc[0].latstart), 'lonstart': float(datemin.loc[0].lonstart)}
	end_pos = {'latend': float(datemax.loc[0].latend), 'lonend': float(datemax.loc[0].lonend)}
	sp_updates = session.query(Startpos).filter(Startpos.latstart == start_pos['latstart']).filter(Startpos.lonstart == start_pos['lonstart']).all()
	ep_updates = session.query(Endpos).filter(Endpos.latend == end_pos['latend']).filter(Endpos.lonend == end_pos['lonend']).all()
	if len(sp_updates) > 0:
		for s in sp_updates:
			s.count += 1
			torqfile.startid = s.startid
			logger.warning(f"startpos already exists for {torqfile.fileid} {torqfile.startid} {s.count=} {start_pos=}")
			session.add(s)
			session.commit()
		else:
			sp = Startpos(latstart=start_pos['latstart'], lonstart=start_pos['lonstart'],label=torqfile.csvfile)
			sp.count = 1
			session.add(sp)
			session.commit()
			torqfile.startid = sp.startid
	session.add(torqfile)
	session.commit()


def get_start_pos_info(args, fileinfo, gpsoffset=0.00004):
	# guess the start and end positions
	# returns startid and endid
	# gpsoffset = 0.00004
	latoffset = 0.0001010 + gpsoffset
	lonoffset = 0.0001421 + gpsoffset
	engine, session = get_engine_session(args)
	sp_updates = session.query(Startpos).filter(
		Startpos.latstart >= fileinfo['dlatstart']-latoffset).filter(
		Startpos.latstart <= fileinfo['dlatstart']+latoffset).filter(
		Startpos.lonstart >= fileinfo['dlonstart']-lonoffset).filter(
		Startpos.lonstart <= fileinfo['dlonstart']+lonoffset).all()
	session.close()
	return sp_updates

def get_sp_updates(args, latstart, lonstart, gpsoffset=0.00004):
	latoffset = 0.0000510 + gpsoffset
	lonoffset = 0.0001221 + gpsoffset
	engine, session = get_engine_session(args)
	sp_updates = session.query(Startpos).filter(
		Startpos.latstart >= latstart-latoffset).filter(
		Startpos.latstart <= latstart+latoffset).filter(
		Startpos.lonstart >= lonstart-lonoffset).filter(
		Startpos.lonstart <= lonstart+lonoffset).all()
	session.close()
	return sp_updates

def get_ep_updates(args, latend, lonend, gpsoffset=0.00004):
	latoffset = 0.0000510 + gpsoffset
	lonoffset = 0.0001221 + gpsoffset
	engine, session = get_engine_session(args)
	ep_updates = session.query(Endpos).filter(
		Endpos.latend >= latend-latoffset).filter(
		Endpos.latend <= latend+latoffset).filter(
		Endpos.lonend >= lonend-lonoffset).filter(
		Endpos.lonend <= lonend+lonoffset).all()
	session.close()
	return ep_updates

def get_start_end_info(args, fileinfo, gpsoffset=0.00002):
	# guess the start and end positions
	# returns startid and endid
	# gpsoffset = 0.00004
	# latoffset = 0.0000510 + gpsoffset
	# lonoffset = 0.0001221 + gpsoffset
	# engine, session = get_engine_session(args)
	sp_updates = get_sp_updates(args, fileinfo['dlatstart'], fileinfo['dlonstart'], gpsoffset)
	ep_updates = get_ep_updates(args, fileinfo['dlatend'], fileinfo['dlonend'], gpsoffset)
	# ep_updates = session.query(Endpos).filter(Endpos.latend > fileinfo['dlatend']-latoffset).filter(Endpos.latend < fileinfo['dlatend']+latoffset).filter(Endpos.lonend >= fileinfo['dlonend']-lonoffset).filter(Endpos.lonend <= fileinfo['dlonend']+lonoffset).all()
	# session.close()
	return sp_updates, ep_updates

def get_bounding_box(args, fileinfo, gpsoffset=0.00002):
	"""
	"""
	return 0

def calculate_bounding_box(coordinates):
# Example usage:
# coordinates = [(34.05, -118.25), (36.16, -115.15), (40.71, -74.01), (37.77, -122.42)]
# bounding_box = calculate_bounding_box(coordinates)
# print(bounding_box)  # Output: (34.05, -122.42, 40.71, -74.01)
	min_lat = float('inf')
	min_lon = float('inf')
	max_lat = float('-inf')
	max_lon = float('-inf')

	for lat, lon in coordinates:
		if lat < min_lat:
			min_lat = lat
		if lon < min_lon:
			min_lon = lon
		if lat > max_lat:
			max_lat = lat
		if lon > max_lon:
			max_lon = lon

	return (min_lat, min_lon, max_lat, max_lon)


def update_torqfile(args: argparse.Namespace, fileinfo: dict):
	# todo fix this is very slow
	engine, session = get_engine_session(args)
	fileid = fileinfo.get("fileid", None)
	torqfile = session.query(TorqFile).filter(TorqFile.fileid == fileid).first()
	trip_start = convert_string_to_datetime(fileinfo["dtripstart"])  # datetime.fromisoformat(str(datemin.values[0][0]))
	trip_end = convert_string_to_datetime(fileinfo["dtripend"])  # datetime.fromisoformat(str(datemax.values[0][0]))
	trip_duration = (trip_end - trip_start).total_seconds()
	torqfile.startlat = float(fileinfo["dlatstart"])
	torqfile.startlon = float(fileinfo["dlonstart"])
	torqfile.endlat = float(fileinfo["dlatend"])
	torqfile.endlon = float(fileinfo["dlonend"])
	torqfile.sent_rows = fileinfo["sent_rows"]  # total_rows_db
	torqfile.sendtime = fileinfo.get("sendtime", None)
	torqfile.readtime = fileinfo.get("readtime", None)
	torqfile.trip_start = trip_start
	torqfile.trip_end = trip_end
	torqfile.trip_duration = trip_duration
	session.close()
	engine, session = get_engine_session(args)
	sp_updates, ep_updates = get_start_end_info(args, fileinfo)
	if len(sp_updates) == 1:
		# found startpos
		sp = session.query(Startpos).filter(Startpos.startid == sp_updates[0].startid).one()
		torqfile.startid = sp.startid
		sp.count += 1
		session.add(sp)
		if sp.label is None:
			logger.warning(f'found startpos id: {sp.startid} label: {sp.label} count: {sp.count} missing label')
		else:
			logger.info(f'found startpos id: {sp.startid} label: {sp.label} count: {sp.count} ')
	elif len(sp_updates) > 1:
		# multiple startpos
		logger.warning(f'multiple startpos sp: {len(sp_updates)} {torqfile.csvfile}')
	elif len(sp_updates) == 0:
		# new startpos
		logger.debug(f'new startpos {fileinfo["dlatstart"]} {fileinfo["dlonstart"]} ')
		sp = Startpos(latstart=fileinfo["dlatstart"], lonstart=fileinfo["dlonstart"])
		sp.count = 1
		session.add(sp)
		# session.commit()

	if len(ep_updates) == 1:
		# found endpos
		ep = ep_updates[0]
		torqfile.endid = ep.endid
		ep.count += 1
		session.add(ep)
		if ep.label is None:
			logger.warning(f'found endpos id: {ep.endid} label: {ep.label} count: {ep.count} missing label')
		else:
			logger.info(f'found endpos id: {ep.endid} label: {ep.label} count: {ep.count} ')
	elif len(ep_updates) > 1:
		# multiple endpos
		logger.warning(f'# multiple endpos ep: {len(ep_updates)} ')
	elif len(ep_updates) == 0:
		# new endpos
		logger.debug(f'new endpos {fileinfo["dlatend"]} {fileinfo["dlonend"]} ')
		ep = Endpos(latend=fileinfo["dlatend"], lonend=fileinfo["dlonend"])
		ep.count = 1
		session.add(ep)
		# session.commit()

	session.add(torqfile)
	session.commit()
	logger.info(f"updatedone for fileid: {fileid} ")  # \n{fileinfo=}\n")
	return 0

def collect_db_columnstats(args):
	engine, session = get_engine_session(args)

	try:
		session.execute(text("drop table if exists columnstats;"))
	except Exception as e:
		logger.error(f'{type(e)} {e}')
		session.rollback()
		return 0
	t0 = datetime.now()
	total_rows = pd.DataFrame(session.execute(text("select count(*) from torqlogs"))).values[0][0]
	column_list = [(idx,k) for idx,k in enumerate(dataschema) if k not in ['gpstime','devicetime']]
	logger.info(f"{total_rows} in db t0: {(datetime.now()-t0).seconds} column_list: {len(column_list)}")
	results = pd.DataFrame()
	tempres = {}
	for idx, column in column_list:
		t1 = datetime.now()
		try:
			nulls = pd.DataFrame(session.execute(text(f"select count(*) as count from torqlogs where {column} is null")).all()).values[0][0]
			notnulls = total_rows - nulls
			# dfval = df.values[0][0]
			if nulls / total_rows > 0.9:
				logger.warning(f"[{idx}/{len(column_list)}]  {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} nlr: {notnulls/total_rows}")
			else:
				logger.info(f"[{idx}/{len(column_list)}] {column} nulls {nulls} ratio:  {nulls/total_rows} notnulls:{notnulls} nlr: {notnulls/total_rows}")
			# results = pd.concat([pd.DataFrame([{"column_name": column, "nulls": nulls, "nullratio": nulls / total_rows,}]),results])
			tempres[column] = {"column_name": column, "nulls": nulls, "nullratio": nulls / total_rows,}
		except (OperationalError,) as e:
			logger.warning(f"{type(e)} {e} for {column}")
			# session.rollback()
			# continue
		except Exception as e:
			logger.error(f"{type(e)} {e} for {column}")
			# session.rollback()
			# continue
	results = pd.DataFrame([tempres[k] for k in tempres])
	try:
		logger.info(f"sending {len(results)}")
		results.to_sql(con=engine, name="columnstats", if_exists="replace", index=True)
		logger.info(f"done sending {len(results)}")
	except Exception as e:
		logger.error(f"{type(e)} {e} for {results=} {results=}")
		session.rollback()

	return 1


def collect_db_speeds(args):
	engine, session = get_engine_session(args)
	try:
		session.execute(text('delete from speeds;'))  # pass  # session.execute(text('drop table if exists speeds;'))
		session.commit()
	except Exception as e:
		logger.error(f"{type(e)} {e}")
		session.rollback()
		return -1
	# res = session.execute(text('drop table speeds'))
	# print(res)
	# q = "select fileid,avg(gpsspeedkmh) as gpsspeedkmh, avg(speedobdkmh) as speedobdkmh, avg(speedgpskmh) as speedgpskmh, min(gpstime) as gpstime  from torqlogs where gpsspeedkmh is not null and gpsspeedkmh>0 and speedobdkmh is not null and speedobdkmh>0  and speedgpskmh is not null and speedgpskmh>0 group by fileid "
	q = 'select fileid,avg(gpsspeedkmh) as gpsspeedkmh, avg(speedobdkmh) as speedobdkmh, avg(speedgpskmh) as speedgpskmh, min(gpstime) as gpstime  from torqlogs group by fileid; '
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
	getstartendquery = """
SELECT
	fileid,
	MIN(latitude) FILTER (WHERE gpstime = first_gpstime) AS latmin,
	MIN(longitude) FILTER (WHERE gpstime = first_gpstime) AS lonmin,
	MIN(latitude) FILTER (WHERE gpstime = last_gpstime) AS latmax,
	MIN(longitude) FILTER (WHERE gpstime = last_gpstime) AS lonmax
FROM (
	SELECT
		fileid,
		latitude,
		longitude,
		gpstime,
		FIRST_VALUE(gpstime) OVER (PARTITION BY fileid ORDER BY gpstime ASC) AS first_gpstime,
		FIRST_VALUE(gpstime) OVER (PARTITION BY fileid ORDER BY gpstime DESC) AS last_gpstime
	FROM torqlogs
) subquery
WHERE gpstime = first_gpstime OR gpstime = last_gpstime group by fileid;
"""
	engine, session = get_engine_session(args)
	df = pd.DataFrame(session.execute(text(getstartendquery)).all())
	gpsoffset = 0.05
	for pos in df.itertuples():
		# start_pos = {'latstart': float(pos.loc[0].latstart), 'lonstart': float(pos.loc[0].lonstart)}
		sp_updates = session.execute(text(f"select * from startpos where latstart between {pos.latmin-gpsoffset} and {pos.latmin+gpsoffset} and lonstart between {pos.lonmin-gpsoffset} and {pos.lonmin+gpsoffset} ")).all()
		logger.debug(f'{pos=} {len(sp_updates)=}')
		if len(sp_updates) > 0:
			for x in sp_updates:
				sp = session.query(Startpos).filter(Startpos.startid == x[0]).one()
				sp.count += 1
				session.add(sp)
				session.commit()
				logger.warning(f"startpos already exists for {pos.fileid} {pos=} {len(sp_updates)} {sp.count=}")
		else:
			# create new startpos
			sp = Startpos(latstart=pos.latmin, lonstart=pos.lonmin)
			sp.count = 1
			logger.info(f"newstartpos {pos.fileid} {pos.latmin} {pos.lonmin} {sp.count}")
			session.add(sp)
			session.commit()

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
		# s = collect_db_start_pos(args)
		# logger.debug("all stats startpos done")
		# e = collect_db_end_pos(args)
		# logger.debug("all stats endpos done")
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
	parser.add_argument("--db_startpos",
		default=False,
		help="db_startpos",
		action="store_true",
		dest="db_startpos",
	)
	parser.add_argument("--db_endpos",
		default=False,
		help="db_endpos",
		action="store_true",
		dest="db_endpos",
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
