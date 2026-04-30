#!/usr/bin/python3
# todo fix only create tripdata for new trips
import pandas as pd
import argparse
from datetime import datetime
from loguru import logger
import sys
from sqlalchemy import (text)
from utils import get_parser, get_engine_session, convert_string_to_datetime
from schemas import dataschema  # schema_datatypes,
from datamodels import TorqFile, Startpos, Endpos
from numbers import Real


def _normalize_col_name(value: str) -> str:
	return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _get_torqlogs_columns(session) -> list[str]:
	rows = session.execute(text("PRAGMA table_info(torqlogs)")).all()
	return [row[1] for row in rows]


def _resolve_schema_columns(session, requested_columns: list[str]) -> dict[str, str]:
	actual_columns = _get_torqlogs_columns(session)
	normalized_actual = {_normalize_col_name(col): col for col in actual_columns}
	resolved: dict[str, str] = {}
	for requested in requested_columns:
		actual = normalized_actual.get(_normalize_col_name(requested))
		if actual:
			resolved[requested] = actual
	return resolved

def to_float(value: object) -> float | None:
    if isinstance(value, Real) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, (str, bytes, bytearray, memoryview)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return None

def collect_db_filestats(args, todatabase=True, droptable=True):
	# todo fix this is very slow
	session = get_engine_session(args)
	# if droptable:
	# 	session.execute(text("drop table if exists filestats"))
	if args.dbmode == "sqlite":
		session.execute(text("PRAGMA journal_mode=WAL;"))
		session.execute(text("pragma synchronous = normal;"))
		session.execute(text("pragma temp_store = memory;"))
		session.execute(text("pragma mmap_size = 30000000000;"))
		# session.execute(text('pragma journal_mode = memory;'))
	q = "select fileid from torqfiles;"
	if args.db_limit:
		q += f" limit {args.db_limit}"
	file_ids = pd.DataFrame(session.execute(text(q)))
	logger.debug(f"fileids={len(file_ids)} ")
	results = []
	requested_columns = [k for k in dataschema if k not in ['gpstime','devicetime']]
	resolved_columns = _resolve_schema_columns(session, requested_columns)
	missing_count = len(requested_columns) - len(resolved_columns)
	if missing_count:
		logger.warning(f"Skipping {missing_count} schema columns not present in torqlogs")

	# Keep order stable for predictable logging/results.
	column_pairs = [(req, resolved_columns[req]) for req in requested_columns if req in resolved_columns]
	if not column_pairs:
		logger.warning("No compatible columns found for file stats")
		return 0

	# Build one aggregate query and reuse for each fileid.
	select_parts = [
		"COUNT(*) AS total_rows",
	]
	for req, actual in column_pairs:
		alias = f"nulls_{req}"
		select_parts.append(f'SUM(CASE WHEN "{actual}" IS NULL THEN 1 ELSE 0 END) AS "{alias}"')
	agg_sql = text(f'SELECT {", ".join(select_parts)} FROM torqlogs WHERE fileid = :fileid')

	for fileidx, file in enumerate(file_ids.itertuples()):
		row = session.execute(agg_sql, {"fileid": file.fileid}).mappings().one()
		total_rows = int(row["total_rows"] or 0)
		if total_rows == 0:
			logger.warning(f"no rows for {file.fileid}")
			continue

		for idx, (requested_col, actual_col) in enumerate(column_pairs):
			alias = f"nulls_{requested_col}"
			nulls = int(row.get(alias, 0) or 0)
			# notnulls = total_rows - nulls
			# dfval = df.values[0][0]
			result = ({
					"fileid": file.fileid,
					"column": actual_col,
					"nulls": nulls,
					"nullratio": nulls / total_rows,
				}
			)
			results.append(result)
		logger.info(f"[{fileidx}/{len(file_ids)}] fileid: {file.fileid} results: {len(results)}")

	if todatabase and results:
		try:
			session.execute(text("DELETE FROM filestats"))
			session.commit()
			pd.DataFrame(results).to_sql(
				name='filestats',
				con=session.get_bind(),
				if_exists='append',
				index=False,
				method='multi',
				chunksize=args.sqlchunksize,
			)
			logger.info(f"saved {len(results)} filestats rows")
		except Exception as e:
			logger.error(f"{type(e)} {e} while writing filestats")
			session.rollback()
			return -1

	return len(results)

def get_sp_updates(args, latstart, lonstart, gpsoffset=0.00004):
	latoffset = 0.0000510 + gpsoffset
	lonoffset = 0.0001221 + gpsoffset
	session = get_engine_session(args)
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
	session = get_engine_session(args)
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

async def update_torqfile(args: argparse.Namespace, fileinfo: dict):
	# todo fix this is very slow
	session = get_engine_session(args)
	fileid = fileinfo.get("fileid", None)
	torqfile = session.query(TorqFile).filter(TorqFile.fileid == fileid).first()
	trip_start = convert_string_to_datetime(fileinfo["dtripstart"])  # datetime.fromisoformat(str(datemin.values[0][0]))
	trip_end = convert_string_to_datetime(fileinfo["dtripend"])  # datetime.fromisoformat(str(datemax.values[0][0]))
	trip_duration = (trip_end - trip_start).total_seconds()
	if isinstance(torqfile, TorqFile):
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
	session = get_engine_session(args)
	sp_updates, ep_updates = get_start_end_info(args, fileinfo)
	if len(sp_updates) == 1:
		# found startpos
		sp = session.query(Startpos).filter(Startpos.startid == sp_updates[0].startid).one()
		if isinstance(torqfile, TorqFile):
			torqfile.startid = sp.startid
		if isinstance(sp, Startpos):
			sp.count += 1
		session.add(sp)
		# if sp.label is None:
		# 	logger.warning(f'found startpos id: {sp.startid} label: {sp.label} count: {sp.count} missing label')
		# else:
		# 	logger.info(f'found startpos id: {sp.startid} label: {sp.label} count: {sp.count} ')
	elif len(sp_updates) > 1:
		# multiple startpos
		# logger.warning(f'multiple startpos sp: {len(sp_updates)} {torqfile.csvfile} ')
		# _ = [logger.warning(f'{k.startid} {k.label} {k.latstart} {k.lonstart}') for k in sp_updates]
		if len(set([k.label for k in sp_updates])) == 1:
			# todo create new merged startpos set by bounding box
			pass
	elif len(sp_updates) == 0:
		# new startpos
		# logger.debug(f'new startpos {fileinfo["dlatstart"]} {fileinfo["dlonstart"]} ')
		sp = Startpos(latstart=fileinfo["dlatstart"], lonstart=fileinfo["dlonstart"])
		sp.count = 1
		session.add(sp)
		# session.commit()

	if len(ep_updates) == 1:
		# found endpos
		ep = ep_updates[0]
		if isinstance(torqfile, TorqFile):
			torqfile.endid = ep.endid
		ep.count += 1
		session.add(ep)
		# if ep.label is None:
		# 	logger.warning(f'found endpos id: {ep.endid} label: {ep.label} count: {ep.count} missing label')
		# else:
		# 	logger.info(f'found endpos id: {ep.endid} label: {ep.label} count: {ep.count} ')
	elif len(ep_updates) > 1:
		# multiple endpos
		# logger.warning(f'# multiple endpos ep: {len(ep_updates)}')
		# _ = [logger.warning(f'{k.endid} {k.label} {k.latend} {k.lonend}') for k in ep_updates]
		if len(set([k.label for k in ep_updates])) == 1:
			# todo create new merged endpos set by bounding box
			pass
	elif len(ep_updates) == 0:
		# new endpos
		# logger.debug(f'new endpos {fileinfo["dlatend"]} {fileinfo["dlonend"]} ')
		ep = Endpos(latend=fileinfo["dlatend"], lonend=fileinfo["dlonend"])
		ep.count = 1
		session.add(ep)
		# session.commit()

	session.add(torqfile)
	session.commit()
	# logger.info(f"updatedone for fileid: {fileid} ")  # \n{fileinfo=}\n")
	return 0

def collect_db_columnstats(args):
	session = get_engine_session(args)

	try:
		session.execute(text("drop table if exists columnstats;"))
	except Exception as e:
		logger.error(f'{type(e)} {e}')
		session.rollback()
		return 0
	t0 = datetime.now()
	requested_columns = [k for k in dataschema if k not in ['gpstime','devicetime']]
	resolved_columns = _resolve_schema_columns(session, requested_columns)
	column_pairs = [(req, resolved_columns[req]) for req in requested_columns if req in resolved_columns]
	if not column_pairs:
		logger.warning("No compatible columns found for column stats")
		return 0

	select_parts = ["COUNT(*) AS total_rows"]
	for req, actual in column_pairs:
		alias = f"nulls_{req}"
		select_parts.append(f'SUM(CASE WHEN "{actual}" IS NULL THEN 1 ELSE 0 END) AS "{alias}"')

	agg_sql = text(f'SELECT {", ".join(select_parts)} FROM torqlogs')
	row = session.execute(agg_sql).mappings().one()
	total_rows = int(row.get("total_rows", 0) or 0)
	logger.info(f"{total_rows} in db t0: {(datetime.now()-t0).seconds} requested_columns: {len(requested_columns)} resolved_columns: {len(resolved_columns)}")
	if total_rows == 0:
		logger.warning("No rows in torqlogs")
		return 0

	tempres = {}
	for idx, (requested_col, actual_col) in enumerate(column_pairs):
		nulls = int(row.get(f"nulls_{requested_col}", 0) or 0)
		notnulls = total_rows - nulls
		nullratio = nulls / total_rows
		if nullratio > 0.9:
			logger.warning(f"[{idx}/{len(column_pairs)}]  {requested_col}->{actual_col} nulls {nulls} ratio:  {nullratio} notnulls:{notnulls} nlr: {notnulls/total_rows}")
		else:
			logger.info(f"[{idx}/{len(column_pairs)}] {requested_col}->{actual_col} nulls {nulls} ratio:  {nullratio} notnulls:{notnulls} nlr: {notnulls/total_rows}")
		tempres[actual_col] = {
			"column_name": actual_col,
			"nulls": nulls,
			"nullratio": nullratio,
		}

	results = pd.DataFrame([tempres[k] for k in tempres])
	try:
		logger.info(f"sending {len(results)}")
		# results.to_sql(con=engine, name="columnstats", if_exists="replace", index=True)
		results.to_sql(con=session.get_bind(), name="columnstats", if_exists="replace", index=True, method='multi', chunksize=args.sqlchunksize)
		logger.info(f"done sending {len(results)}")
	except Exception as e:
		logger.error(f"{type(e)} {e} for {results=} {results=}")
		session.rollback()

	return 1


def collect_db_speeds(args):
	session = get_engine_session(args)
	try:
		session.execute(text('delete from speeds;'))
		session.commit()
	except Exception as e:
		logger.error(f"{type(e)} {e}")
		session.rollback()
		return -1
	resolved = _resolve_schema_columns(session, ['gpstime', 'gpsspeedkmh', 'speedgpskmh', 'speedobdkmh'])
	time_col = resolved.get('gpstime')
	obd_col = resolved.get('speedobdkmh')
	gps_col = resolved.get('speedgpskmh') or resolved.get('gpsspeedkmh')
	if not (time_col and obd_col and gps_col):
		logger.error("Missing required columns for speed aggregation")
		return -1

	q = (
		f'select fileid, '
		f'avg("{gps_col}") as gpsspeedkmh, '
		f'avg("{obd_col}") as speedobdkmh, '
		f'avg("{gps_col}") as speedgpskmh, '
		f'min("{time_col}") as gpstime '
		f'from torqlogs group by fileid; '
	)
	if args.db_limit:
		q += f" limit {args.limit}"
	try:
		df = pd.DataFrame(session.execute(text(q)).all()).fillna(0)
		logger.info(f"dbspeeds:{df.describe()}")
		# res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))
		df = df.to_sql(name='speeds', con=session.get_bind(), if_exists='replace')
		logger.info(f"dbspeeds: dfres {df}")
	except Exception as e:
		logger.error(f"{type(e)} {e} for {q=}")
		session.rollback()
	return 0

def collect_db_startends(args):
	session = get_engine_session(args)
	resolved = _resolve_schema_columns(session, ['gpstime', 'latitude', 'longitude'])
	time_col = resolved.get('gpstime')
	lat_col = resolved.get('latitude')
	lon_col = resolved.get('longitude')
	if not (time_col and lat_col and lon_col):
		logger.error("Missing required torqlogs columns for start/end collection")
		return -1

	getstartendquery = f"""
SELECT
    fileid,
	MIN("{lat_col}") FILTER (WHERE "{time_col}" = first_gpstime) AS latmin,
	MIN("{lon_col}") FILTER (WHERE "{time_col}" = first_gpstime) AS lonmin,
	MIN("{lat_col}") FILTER (WHERE "{time_col}" = last_gpstime) AS latmax,
	MIN("{lon_col}") FILTER (WHERE "{time_col}" = last_gpstime) AS lonmax
FROM (
    SELECT
        fileid,
		"{lat_col}",
		"{lon_col}",
		"{time_col}",
		FIRST_VALUE("{time_col}") OVER (PARTITION BY fileid ORDER BY "{time_col}" ASC) AS first_gpstime,
		FIRST_VALUE("{time_col}") OVER (PARTITION BY fileid ORDER BY "{time_col}" DESC) AS last_gpstime
    FROM torqlogs
) subquery
WHERE "{time_col}" = first_gpstime OR "{time_col}" = last_gpstime
GROUP BY fileid;
"""
	gpsoffset = 0.05

	rows = session.execute(text(getstartendquery)).mappings().all()
	for pos in rows:
		latmin = to_float(pos.get("latmin"))
		lonmin = to_float(pos.get("lonmin"))
		if latmin is None or lonmin is None:
			continue

		min_lat, max_lat = latmin - gpsoffset, latmin + gpsoffset
		min_lon, max_lon = lonmin - gpsoffset, lonmin + gpsoffset

		sp_updates = (
			session.query(Startpos)
			.filter(
				Startpos.latstart.between(min_lat, max_lat),
				Startpos.lonstart.between(min_lon, max_lon),
			)
			.all()
		)

		if sp_updates:
			for sp in sp_updates:
				sp.count = int(sp.count or 0) + 1
			logger.warning(f"startpos already exists for fileid={pos.get('fileid')} count={len(sp_updates)}")
		else:
			sp = Startpos(latstart=latmin, lonstart=lonmin)
			sp.count = 1
			logger.info(f"newstartpos {pos.fileid} {pos.latmin} {pos.lonmin} {sp.count}")
			session.add(sp)

	session.commit()

def main(args):
	session = get_engine_session(args)
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
		logger.info(f"[main] got {type(r)}")
	except Exception as e:
		logger.error(f"unhandled {type(e)} {e}")
		sys.exit(-1)
