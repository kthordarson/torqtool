#!/usr/bin/python3
# todo fix only create tripdata for new trips
import pandas as pd
import argparse
from datetime import datetime
from pathlib import Path
from loguru import logger
import sys
from sqlalchemy import text, inspect
from utils import get_parser, get_engine_session, convert_string_to_datetime, haversine
from schemas import dataschema
from datamodels import TorqFile, Startpos, Endpos
from schemas import TRIP_METRIC_COLUMNS
from numbers import Real


def _normalize_col_name(value: str) -> str:
    return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _get_torqlogs_columns(session) -> list[str]:
    inspector = inspect(session.get_bind())
    return [str(col["name"]) for col in inspector.get_columns("torqlogs")]


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
        except (TypeError, ValueError) as e:
            logger.warning(f"Could not convert value to float: {value} ({e})")
            return None
    return None


def _write_start_end_backup(session, backup_file: Path) -> None:
    start_rows = (
        session.execute(
            text(
                "SELECT startid, latstart, lonstart, count, label FROM startpos ORDER BY startid"
            )
        )
        .mappings()
        .all()
    )
    end_rows = (
        session.execute(
            text(
                "SELECT endid, latend, lonend, count, label FROM endpos ORDER BY endid"
            )
        )
        .mappings()
        .all()
    )

    with backup_file.open("w", encoding="utf-8", errors="replace") as f:
        f.write(f"# backup_at={datetime.now().isoformat()}\n")
        f.write("# table=startpos\n")
        f.write("startid | latstart | lonstart | count | label\n")
        f.write("--------+----------+----------+-------+------\n")
        for row in start_rows:
            f.write(
                f"{row['startid']} | {row['latstart']} | {row['lonstart']} | {row['count']} | {row['label'] if row['label'] is not None else ''}\n"
            )

        f.write("\n# table=endpos\n")
        f.write("endid | latend | lonend | count | label\n")
        f.write("------+--------+--------+-------+------\n")
        for row in end_rows:
            f.write(
                f"{row['endid']} | {row['latend']} | {row['lonend']} | {row['count']} | {row['label'] if row['label'] is not None else ''}\n"
            )


def _parse_labeled_coords(
    backup_file: Path, default_section: str | None = None
) -> dict[str, list[tuple[float, float, str]]]:
    parsed: dict[str, list[tuple[float, float, str]]] = {"start": [], "end": []}
    section = default_section

    with backup_file.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.strip()
            lower = line.lower()
            if "table=startpos" in lower or lower.startswith("startid |"):
                section = "start"
                continue
            if "table=endpos" in lower or lower.startswith("endid |"):
                section = "end"
                continue

            if section not in ("start", "end") or "|" not in line:
                continue

            parts = [part.strip() for part in line.split("|")]
            if len(parts) < 5:
                continue

            try:
                int(parts[0])
                lat = float(parts[1])
                lon = float(parts[2])
            except (TypeError, ValueError) as e:
                logger.warning(
                    f"Could not parse line in backup file: {raw_line.strip()} ({e})"
                )
                continue

            label = parts[4]
            if not label:
                continue

            parsed[section].append((lat, lon, label))

    return parsed


def _restore_labels_from_coords(
    session,
    table_name: str,
    id_column: str,
    lat_column: str,
    lon_column: str,
    rows: list[tuple[float, float, str]],
    match_offset: float = 0.001,
) -> int:
    updated = 0
    select_stmt = text(f"""
        SELECT {id_column} AS row_id, {lat_column} AS lat, {lon_column} AS lon, label
        FROM {table_name}
        WHERE {lat_column} BETWEEN :lat_min AND :lat_max
            AND {lon_column} BETWEEN :lon_min AND :lon_max
        """)
    update_stmt = text(
        f"UPDATE {table_name} SET label = :label WHERE {id_column} = :row_id"
    )

    for lat, lon, label in rows:
        candidates = (
            session.execute(
                select_stmt,
                {
                    "lat_min": lat - match_offset,
                    "lat_max": lat + match_offset,
                    "lon_min": lon - match_offset,
                    "lon_max": lon + match_offset,
                },
            )
            .mappings()
            .all()
        )
        if not candidates:
            continue

        best = min(
            candidates,
            key=lambda r: haversine(lat, lon, float(r["lat"]), float(r["lon"])),
        )
        current_label = best.get("label")
        if current_label == label:
            continue

        res = session.execute(
            update_stmt, {"label": label, "row_id": int(best["row_id"])}
        )
        if res.rowcount and res.rowcount > 0:
            updated += int(res.rowcount)

    return updated


def collect_db_filestats(args, todatabase=True, droptable=False):
    # Incremental and batched filestats collection.
    session = get_engine_session(args)
    # if droptable:
    # 	session.execute(text("drop table if exists filestats"))
    if args.dbmode == "sqlite":
        session.execute(text("PRAGMA journal_mode=WAL;"))
        session.execute(text("pragma synchronous = normal;"))
        session.execute(text("pragma temp_store = memory;"))
        session.execute(text("pragma mmap_size = 30000000000;"))
        # session.execute(text('pragma journal_mode = memory;'))
    q = "select fileid from torqfiles"
    q += ";"
    fileid_rows = session.execute(text(q)).all()
    file_ids = [int(row[0]) for row in fileid_rows if row and row[0] is not None]
    logger.debug(f"candidate fileids={len(file_ids)}")
    results: list[dict[str, object]] = []
    requested_columns = [k for k in dataschema if k not in ["gpstime", "devicetime"]]
    resolved_columns = _resolve_schema_columns(session, requested_columns)
    missing_count = len(requested_columns) - len(resolved_columns)
    if missing_count:
        logger.warning(f"Skipping {missing_count} schema columns not present in torqlogs")

    # Keep order stable for predictable logging/results.
    column_pairs = [
        (req, resolved_columns[req])
        for req in requested_columns
        if req in resolved_columns
    ]
    if not column_pairs:
        logger.warning("No compatible columns found for file stats")
        return 0

    if todatabase and droptable:
        try:
            session.execute(text("DELETE FROM filestats"))
            session.commit()
        except Exception as e:
            logger.error(f"{type(e)} {e} while clearing filestats")
            session.rollback()
            return -1

    already_analyzed: set[int] = set()
    if todatabase and not droptable:
        try:
            existing_rows = session.execute(
                text("SELECT DISTINCT fileid FROM filestats")
            ).all()
            already_analyzed = {
                int(row[0]) for row in existing_rows if row and row[0] is not None
            }
        except Exception as e:
            logger.debug(
                f"filestats table may be empty/missing; continuing without skip set: {e} ({type(e)})"
            )

    pending_fileids = [fid for fid in file_ids if fid not in already_analyzed]
    skipped_count = len(file_ids) - len(pending_fileids)
    logger.info(
        f"Found {len(file_ids)} candidate files; processing {len(pending_fileids)}, "
        f"skipped already analyzed {skipped_count}"
    )
    if not pending_fileids:
        return 0

    # Aggregate null stats in batches to reduce SQL round-trips.
    select_parts = ["fileid", "COUNT(*) AS total_rows"]
    for idx, (_, actual) in enumerate(column_pairs):
        select_parts.append(
            f'SUM(CASE WHEN "{actual}" IS NULL THEN 1 ELSE 0 END) AS "n_{idx}"'
        )

    if args.dbmode == "sqlite":
        # Keep room for SELECT params and DB limits.
        batch_size = 300
    else:
        batch_size = 1000

    total_processed = 0
    for batch_start in range(0, len(pending_fileids), batch_size):
        batch = pending_fileids[batch_start:batch_start + batch_size]
        placeholders = ", ".join(f":fid{i}" for i in range(len(batch)))
        params = {f"fid{i}": int(fid) for i, fid in enumerate(batch)}
        agg_sql = text(
            f'SELECT {", ".join(select_parts)} '
            f"FROM torqlogs WHERE fileid IN ({placeholders}) GROUP BY fileid"
        )
        batch_rows = session.execute(agg_sql, params).mappings().all()
        for row in batch_rows:
            fileid = int(row.get("fileid", 0) or 0)
            total_rows = int(row.get("total_rows", 0) or 0)
            if fileid <= 0 or total_rows <= 0:
                continue
            for idx, (_, actual_col) in enumerate(column_pairs):
                nulls = int(row.get(f"n_{idx}", 0) or 0)
                results.append(
                    {
                        "fileid": fileid,
                        "column_name": actual_col,
                        "nulls": nulls,
                        "nullratio": nulls / total_rows,
                    }
                )
            total_processed += 1
        logger.info(
            f"processed batch {batch_start // batch_size + 1} "
            f"({min(batch_start + len(batch), len(pending_fileids))}/{len(pending_fileids)} files)"
        )

    if todatabase and results:
        try:
            pd.DataFrame(results).to_sql(
                name="filestats",
                con=session.get_bind(),
                if_exists="append",
                index=False,
                method="multi",
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
    sp_updates = (
        session.query(Startpos)
        .filter(Startpos.latstart >= latstart - latoffset)
        .filter(Startpos.latstart <= latstart + latoffset)
        .filter(Startpos.lonstart >= lonstart - lonoffset)
        .filter(Startpos.lonstart <= lonstart + lonoffset)
        .all()
    )
    session.close()
    return sp_updates


def get_ep_updates(args, latend, lonend, gpsoffset=0.00004):
    latoffset = 0.0000510 + gpsoffset
    lonoffset = 0.0001221 + gpsoffset
    session = get_engine_session(args)
    ep_updates = (
        session.query(Endpos)
        .filter(Endpos.latend >= latend - latoffset)
        .filter(Endpos.latend <= latend + latoffset)
        .filter(Endpos.lonend >= lonend - lonoffset)
        .filter(Endpos.lonend <= lonend + lonoffset)
        .all()
    )
    session.close()
    return ep_updates


def get_start_end_info(args, fileinfo, gpsoffset=0.00002):
    # guess the start and end positions
    # returns startid and endid
    # gpsoffset = 0.00004
    # latoffset = 0.0000510 + gpsoffset
    # lonoffset = 0.0001221 + gpsoffset
    # engine, session = get_engine_session(args)
    sp_updates = get_sp_updates(
        args, fileinfo["dlatstart"], fileinfo["dlonstart"], gpsoffset
    )
    ep_updates = get_ep_updates(
        args, fileinfo["dlatend"], fileinfo["dlonend"], gpsoffset
    )
    # ep_updates = session.query(Endpos).filter(Endpos.latend > fileinfo['dlatend']-latoffset).filter(Endpos.latend < fileinfo['dlatend']+latoffset).filter(Endpos.lonend >= fileinfo['dlonend']-lonoffset).filter(Endpos.lonend <= fileinfo['dlonend']+lonoffset).all()
    # session.close()
    return sp_updates, ep_updates


async def update_torqfile(args: argparse.Namespace, fileinfo: dict):
    # todo fix this is very slow
    session = get_engine_session(args)
    fileid = fileinfo.get("fileid", None)
    torqfile = session.query(TorqFile).filter(TorqFile.fileid == fileid).first()
    trip_start = convert_string_to_datetime(
        fileinfo["dtripstart"]
    )  # datetime.fromisoformat(str(datemin.values[0][0]))
    trip_end = convert_string_to_datetime(
        fileinfo["dtripend"]
    )  # datetime.fromisoformat(str(datemax.values[0][0]))
    if trip_start and trip_end:
        trip_duration = (trip_end - trip_start).total_seconds()
    else:
        trip_duration = 0.0
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
    if len(sp_updates) >= 1:
        # pick the closest existing startpos
        closest_sp = min(
            sp_updates,
            key=lambda s: haversine(
                fileinfo["dlatstart"], fileinfo["dlonstart"], s.latstart, s.lonstart
            ),
        )
        sp = (
            session.query(Startpos).filter(Startpos.startid == closest_sp.startid).one()
        )
        sp.count = int(sp.count or 0) + 1
        session.add(sp)
        if isinstance(torqfile, TorqFile):
            torqfile.startid = sp.startid
    elif len(sp_updates) == 0:
        # new startpos
        sp = Startpos(
            latstart=fileinfo["dlatstart"], lonstart=fileinfo["dlonstart"], count=1
        )
        session.add(sp)
        session.flush()
        if isinstance(torqfile, TorqFile):
            torqfile.startid = sp.startid

    if len(ep_updates) >= 1:
        # pick the closest existing endpos
        closest_ep = min(
            ep_updates,
            key=lambda e: haversine(
                fileinfo["dlatend"], fileinfo["dlonend"], e.latend, e.lonend
            ),
        )
        ep = session.query(Endpos).filter(Endpos.endid == closest_ep.endid).one()
        ep.count = int(ep.count or 0) + 1
        session.add(ep)
        if isinstance(torqfile, TorqFile):
            torqfile.endid = ep.endid
    elif len(ep_updates) == 0:
        # new endpos
        ep = Endpos(latend=fileinfo["dlatend"], lonend=fileinfo["dlonend"], count=1)
        session.add(ep)
        session.flush()
        if isinstance(torqfile, TorqFile):
            torqfile.endid = ep.endid

    session.add(torqfile)
    session.commit()
    # logger.info(f"updatedone for fileid: {fileid} ")  # \n{fileinfo=}\n")
    return 0


def collect_db_columnstats(args):
    session = get_engine_session(args)

    try:
        session.execute(text("drop table if exists columnstats;"))
        session.commit()
    except Exception as e:
        logger.error(f"{type(e)} {e}")
        session.rollback()
        return 0
    t0 = datetime.now()
    requested_columns = [k for k in dataschema if k not in ["gpstime", "devicetime"]]
    resolved_columns = _resolve_schema_columns(session, requested_columns)
    column_pairs = [
        (req, resolved_columns[req])
        for req in requested_columns
        if req in resolved_columns
    ]
    if not column_pairs:
        logger.warning("No compatible columns found for column stats")
        return 0

    select_parts = ["COUNT(*) AS total_rows"]
    for req, actual in column_pairs:
        alias = f"nulls_{req}"
        select_parts.append(
            f'SUM(CASE WHEN "{actual}" IS NULL THEN 1 ELSE 0 END) AS "{alias}"'
        )

    agg_sql = text(f'SELECT {", ".join(select_parts)} FROM torqlogs')
    row = session.execute(agg_sql).mappings().one()
    total_rows = int(row.get("total_rows", 0) or 0)
    logger.info(
        f"{total_rows} in db t0: {(datetime.now()-t0).seconds} requested_columns: {len(requested_columns)} resolved_columns: {len(resolved_columns)}"
    )
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
            # pass
            # logger.info(f"[{idx}/{len(column_pairs)}] {requested_col}->{actual_col} nulls {nulls} ratio:  {nullratio} notnulls:{notnulls} nlr: {notnulls/total_rows}")
            tempres[actual_col] = {"column_name": actual_col, "nulls": nulls, "nullratio": nullratio, }

    results = pd.DataFrame([tempres[k] for k in tempres])
    try:
        logger.info(f"sending {len(results)}")
        # Use the session-bound connection to avoid waiting on locks from a separate engine connection.
        results.to_sql(
            con=session.connection(),
            name="columnstats",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=args.sqlchunksize,
        )
        session.commit()
        logger.info(f"done sending {len(results)}")
    except Exception as e:
        logger.error(f"{type(e)} {e} for {results=} {results=}")
        session.rollback()

    return 1


def collect_db_speeds(args):
    session = get_engine_session(args)
    try:
        session.execute(text("delete from speeds;"))
        session.commit()
    except Exception as e:
        logger.error(f"{type(e)} {e}")
        session.rollback()
        return -1
    resolved = _resolve_schema_columns(
        session, ["gpstime", "gpsspeedkmh", "speedgpskmh", "speedobdkmh"]
    )
    time_col = resolved.get("gpstime")
    obd_col = resolved.get("speedobdkmh")
    gps_col = resolved.get("speedgpskmh") or resolved.get("gpsspeedkmh")
    if not (time_col and obd_col and gps_col):
        logger.error("Missing required columns for speed aggregation")
        return -1

    q = (
        f"select fileid, "
        f'avg("{gps_col}") as gpsspeedkmh, '
        f'avg("{obd_col}") as speedobdkmh, '
        f'avg("{gps_col}") as speedgpskmh, '
        f'min("{time_col}") as gpstime '
        f"from torqlogs group by fileid; "
    )
    try:
        df = pd.DataFrame(session.execute(text(q)).all()).fillna(0)
        logger.info(f"dbspeeds:{df.describe()}")
        # res = session.execute(text('create table speeds as select fileid,avg(gpsspeedkmh) as speed,min(gpstime) as gpstime  from torqlogs group by fileid'))
        df = df.to_sql(name="speeds", con=session.get_bind(), if_exists="replace")
        logger.info(f"dbspeeds: dfres {df}")
    except Exception as e:
        logger.error(f"{type(e)} {e} for {q=}")
        session.rollback()
    return 0


def collect_db_startends(args, update_start=True, update_end=True, force_refresh=False):
    session = get_engine_session(args)
    label_restore_sources: list[Path] = []
    resolved = _resolve_schema_columns(session, ["gpstime", "latitude", "longitude"])
    time_col = resolved.get("gpstime")
    lat_col = resolved.get("latitude")
    lon_col = resolved.get("longitude")
    if not (time_col and lat_col and lon_col):
        logger.error("Missing required torqlogs columns for start/end collection")
        return -1

    if force_refresh:
        logger.info(
            f"Force refresh requested for start/end collection "
            f"(update_start={update_start}, update_end={update_end})"
        )
        try:
            backup_dir = Path(__file__).resolve().parent
            existing_refresh_backups = sorted(
                backup_dir.glob("start_endpos_backup_before_force_refresh_*.txt")
            )
            if existing_refresh_backups:
                label_restore_sources.append(existing_refresh_backups[-1])

            legacy_start_backup = backup_dir / "startpos_backup0.txt"
            legacy_end_backup = backup_dir / "endpos_backup0.txt"
            if legacy_start_backup.exists():
                label_restore_sources.append(legacy_start_backup)
            if legacy_end_backup.exists():
                label_restore_sources.append(legacy_end_backup)

            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            backup_file = (
                backup_dir / f"start_endpos_backup_before_force_refresh_{ts}.txt"
            )
            _write_start_end_backup(session, backup_file)
            logger.info(f"Backed up start/end tables to {backup_file}")

            if update_start:
                session.execute(text("UPDATE torqfiles SET startid = NULL"))
                session.execute(text("DELETE FROM startpos"))
            if update_end:
                session.execute(text("UPDATE torqfiles SET endid = NULL"))
                session.execute(text("DELETE FROM endpos"))
            session.commit()
        except Exception as e:
            logger.error(
                f"Failed forced reset for start/end collection: {e} ({type(e)})"
            )
            session.rollback()
            return -1

    if update_start and update_end:
        pending_q = (
            "SELECT fileid FROM torqfiles WHERE startid IS NULL OR endid IS NULL"
        )
    elif update_start:
        pending_q = "SELECT fileid FROM torqfiles WHERE startid IS NULL"
    elif update_end:
        pending_q = "SELECT fileid FROM torqfiles WHERE endid IS NULL"
    else:
        logger.info("collect_db_startends called with nothing to update")
        return 0

    pending_fileids_rows = session.execute(text(pending_q)).all()
    pending_fileids = [
        int(row[0]) for row in pending_fileids_rows if row and row[0] is not None
    ]
    if not pending_fileids:
        logger.info("No new/pending torqfiles for start/end processing")
        return 0
    logger.info(f"Processing start/end info for {len(pending_fileids)} pending files")

    getstartendquery_template = f"""
    SELECT fileid,
        MAX(CASE WHEN rn_asc = 1 THEN "{lat_col}" END) AS latstart,
        MAX(CASE WHEN rn_asc = 1 THEN "{lon_col}" END) AS lonstart,
        MAX(CASE WHEN rn_desc = 1 THEN "{lat_col}" END) AS latend,
        MAX(CASE WHEN rn_desc = 1 THEN "{lon_col}" END) AS lonend
    FROM (
        SELECT fileid, "{lat_col}", "{lon_col}",
            ROW_NUMBER() OVER (PARTITION BY fileid ORDER BY "{time_col}" ASC) AS rn_asc,
            ROW_NUMBER() OVER (PARTITION BY fileid ORDER BY "{time_col}" DESC) AS rn_desc
        FROM torqlogs
        WHERE "{lat_col}" IS NOT NULL AND "{lon_col}" IS NOT NULL
        AND fileid IN ({{placeholders}})
    ) sub
    WHERE rn_asc = 1 OR rn_desc = 1
    GROUP BY fileid;
    """
    gpsoffset = 0.001  # ~111 m clustering radius

    # Query start/end points in batches so we only process pending files.
    batch_size = 300 if args.dbmode == "sqlite" else 1000
    processed = 0
    for batch_start in range(0, len(pending_fileids), batch_size):
        batch = pending_fileids[batch_start:batch_start + batch_size]
        placeholders = ", ".join(f":fid{i}" for i in range(len(batch)))
        params = {f"fid{i}": int(fid) for i, fid in enumerate(batch)}
        getstartendquery = getstartendquery_template.format(placeholders=placeholders)
        rows = session.execute(text(getstartendquery), params).mappings().all()

        for pos in rows:
            fileid = pos.get("fileid")
            latstart = to_float(pos.get("latstart"))
            lonstart = to_float(pos.get("lonstart"))
            latend = to_float(pos.get("latend"))
            lonend = to_float(pos.get("lonend"))

            torqfile = session.query(TorqFile).filter(TorqFile.fileid == fileid).first()
            if not isinstance(torqfile, TorqFile):
                logger.warning(f"no TorqFile for fileid={fileid}")
                continue

            if (
                update_start
                and torqfile.startid is None
                and latstart is not None
                and lonstart is not None
            ):
                sp_matches = (
                    session.query(Startpos)
                    .filter(
                        Startpos.latstart.between(
                            latstart - gpsoffset, latstart + gpsoffset
                        ),
                        Startpos.lonstart.between(
                            lonstart - gpsoffset, lonstart + gpsoffset
                        ),
                    )
                    .all()
                )
                if sp_matches:
                    sp = min(
                        sp_matches,
                        key=lambda s: haversine(
                            latstart, lonstart, s.latstart, s.lonstart
                        ),
                    )
                    sp.count = int(sp.count or 0) + 1
                else:
                    sp = Startpos(latstart=latstart, lonstart=lonstart, count=1)
                    session.add(sp)
                    session.flush()
                torqfile.startid = sp.startid

            if (
                update_end
                and torqfile.endid is None
                and latend is not None
                and lonend is not None
            ):
                ep_matches = (
                    session.query(Endpos)
                    .filter(
                        Endpos.latend.between(latend - gpsoffset, latend + gpsoffset),
                        Endpos.lonend.between(lonend - gpsoffset, lonend + gpsoffset),
                    )
                    .all()
                )
                if ep_matches:
                    ep = min(
                        ep_matches,
                        key=lambda e: haversine(latend, lonend, e.latend, e.lonend),
                    )
                    ep.count = int(ep.count or 0) + 1
                else:
                    ep = Endpos(latend=latend, lonend=lonend, count=1)
                    session.add(ep)
                    session.flush()
                torqfile.endid = ep.endid
            processed += 1

        logger.info(
            f"start/end batch {batch_start // batch_size + 1}: "
            f"{min(batch_start + len(batch), len(pending_fileids))}/{len(pending_fileids)} files"
        )

    session.commit()

    if force_refresh and label_restore_sources:
        total_start_updates = 0
        total_end_updates = 0
        for src in label_restore_sources:
            try:
                if src.name.lower() == "startpos_backup0.txt":
                    parsed = _parse_labeled_coords(src, default_section="start")
                elif src.name.lower() == "endpos_backup0.txt":
                    parsed = _parse_labeled_coords(src, default_section="end")
                else:
                    parsed = _parse_labeled_coords(src)

                if update_start and parsed["start"]:
                    total_start_updates += _restore_labels_from_coords(
                        session,
                        table_name="startpos",
                        id_column="startid",
                        lat_column="latstart",
                        lon_column="lonstart",
                        rows=parsed["start"],
                    )

                if update_end and parsed["end"]:
                    total_end_updates += _restore_labels_from_coords(
                        session,
                        table_name="endpos",
                        id_column="endid",
                        lat_column="latend",
                        lon_column="lonend",
                        rows=parsed["end"],
                    )
                logger.info(f"Attempted label restore using backup source: {src}")
            except Exception as e:
                logger.warning(f"Could not restore labels from {src}: {e} ({type(e)})")

        session.commit()
        logger.info(
            f"Label restore updates after force refresh: "
            f"startpos={total_start_updates}, endpos={total_end_updates}"
        )

    logger.info(f"collect_db_startends completed for {processed} files")
    return 0


def collect_db_torqtrips(args):
    # populate torqtrips table with one row per trip, using start/end info from torqfiles and torqlogs
    session = get_engine_session(args)
    logger.info("Starting torqtrips aggregation")

    def _ensure_torqtrips_metric_columns(metric_names: list[str]) -> None:
        inspector = inspect(session.get_bind())
        existing = {
            str(col["name"]).lower() for col in inspector.get_columns("torqtrips")
        }
        numeric_sql_type = (
            "DOUBLE PRECISION"
            if args.dbmode in ("psql", "postgres", "postgresql")
            else "REAL"
        )
        for metric in metric_names:
            for suffix in ("min", "max", "avg", "stdev"):
                col_name = f"{metric}_{suffix}"
                if col_name.lower() in existing:
                    continue
                session.execute(text(f'ALTER TABLE torqtrips ADD COLUMN "{col_name}" {numeric_sql_type}'))
                existing.add(col_name.lower())
                # logger.debug(f"Added column {col_name} to torqtrips for metric {metric}")

    resolved = _resolve_schema_columns(session, ["gpstime", *TRIP_METRIC_COLUMNS])
    time_col = resolved.get("gpstime")
    if not time_col:
        logger.error("Missing required gpstime column for torqtrips aggregation")
        return -1

    resolved_metric_pairs = [
        (metric, resolved[metric])
        for metric in TRIP_METRIC_COLUMNS
        if metric in resolved
    ]
    if not resolved_metric_pairs:
        logger.warning(
            "No requested torqtrips metric columns found in torqlogs; updating base trip fields only"
        )
    _ensure_torqtrips_metric_columns([metric for metric, _ in resolved_metric_pairs])
    session.commit()

    q_fileids = "SELECT fileid FROM torqfiles"
    fileid_rows = session.execute(text(q_fileids)).all()
    fileids = [int(row[0]) for row in fileid_rows if row and row[0] is not None]
    if not fileids:
        logger.info("No torqfiles found for torqtrips aggregation")
        return 0

    batch_size = 300 if args.dbmode == "sqlite" else 1000
    inserted_rows = 0
    logger.debug(
        f"candidate fileids for torqtrips={len(fileids)}, processing in batches of {batch_size}"
    )
    for batch_start in range(0, len(fileids), batch_size):
        batch = fileids[batch_start:batch_start + batch_size]
        placeholders = ", ".join(f":fid{i}" for i in range(len(batch)))
        params = {f"fid{i}": int(fid) for i, fid in enumerate(batch)}

        try:
            # Remove prior rows for this batch so recalculation does not create duplicates.
            logger.debug(
                f"Clearing existing torqtrips rows for batch {batch_start // batch_size + 1} placeholders: {len(placeholders)}, params: {len(params)}"
            )
            session.execute(
                text(f"DELETE FROM torqtrips WHERE fileid IN ({placeholders})"), params
            )
            logger.info(
                f"Cleared existing torqtrips rows for batch {batch_start // batch_size + 1}"
            )

            metric_select_parts: list[str] = []
            for idx, (_, actual_col) in enumerate(resolved_metric_pairs):
                # Filter out float32 sentinel/overflow values (e.g. 3.4028235e+38 = FLT_MAX)
                # by NULLing any value whose absolute magnitude exceeds 1e30.
                valid = f'CASE WHEN ABS(tl."{actual_col}") < 1e30 THEN tl."{actual_col}" ELSE NULL END'
                metric_select_parts.extend(
                    [
                        f'MIN({valid}) AS "m_{idx}_min"',
                        f'MAX({valid}) AS "m_{idx}_max"',
                        f'AVG({valid}) AS "m_{idx}_avg"',
                        f'COUNT({valid}) AS "m_{idx}_count"',
                        f'AVG(CASE WHEN ABS(tl."{actual_col}") < 1e30 THEN tl."{actual_col}" * tl."{actual_col}" ELSE NULL END) AS "m_{idx}_avg_sq"',
                    ]
                )
            metric_sql = (
                (",\n\t\t" + ",\n\t\t".join(metric_select_parts))
                if metric_select_parts
                else ""
            )
            logger.debug(f"Constructed metric SQL for torqtrips {len(metric_sql)}")
            agg_sql = text(f"""
                SELECT
                    tl.fileid AS fileid,
                    MIN(tl."{time_col}") AS tripdate,
                    MAX(tl."{time_col}") AS trip_end,
                    MAX(tf.trip_distance) AS trip_distance
                    {metric_sql}
                FROM torqlogs tl
                LEFT JOIN torqfiles tf ON tf.fileid = tl.fileid
                WHERE tl.fileid IN ({placeholders})
                GROUP BY tl.fileid
                """)
            rows = session.execute(agg_sql, params).mappings().all()
            if not rows:
                session.commit()
                continue

            records: list[dict[str, object]] = []
            logger.debug(
                f"Processing {len(rows)} aggregated rows for torqtrips batch {batch_start // batch_size + 1}"
            )
            for row_idx, row in enumerate(rows):
                trip_start = row.get("tripdate")
                trip_end = row.get("trip_end")
                trip_duration = None
                if trip_start and trip_end:
                    try:
                        trip_start_dt = convert_string_to_datetime(str(trip_start))
                        trip_end_dt = convert_string_to_datetime(str(trip_end))
                        if trip_start_dt and trip_end_dt:
                            trip_duration = float(
                                (trip_end_dt - trip_start_dt).total_seconds()
                            )
                    except Exception as e:
                        logger.warning(
                            f"Could not compute trip duration for fileid {row.get('fileid')}: {e} ({type(e)})"
                        )

                rec: dict[str, object] = {
                    "fileid": int(row.get("fileid") or 0),
                    "tripdate": trip_start,
                    "time": trip_duration,
                    "trip_distance": row.get("trip_distance"),
                }

                for idx, (metric_name, _) in enumerate(resolved_metric_pairs):
                    min_val = row.get(f"m_{idx}_min")
                    max_val = row.get(f"m_{idx}_max")
                    avg_val = row.get(f"m_{idx}_avg")
                    count_val = int(row.get(f"m_{idx}_count") or 0)
                    avg_sq_val = row.get(f"m_{idx}_avg_sq")

                    rec[f"{metric_name}_min"] = (
                        float(min_val) if min_val is not None else None
                    )
                    rec[f"{metric_name}_max"] = (
                        float(max_val) if max_val is not None else None
                    )
                    rec[f"{metric_name}_avg"] = (
                        float(avg_val) if avg_val is not None else None
                    )
                    if count_val > 1 and avg_val is not None and avg_sq_val is not None:
                        variance = max(0.0, float(avg_sq_val) - (float(avg_val) ** 2))
                        rec[f"{metric_name}_stdev"] = variance**0.5
                    else:
                        rec[f"{metric_name}_stdev"] = None
                records.append(rec)
                # logger.debug(f"[{row_idx}/{len(rows)}] Prepared record for fileid {rec['fileid']} records: {len(records)}")

            if records:
                logger.debug(f"Writing {len(records)} torqtrips records for batch {batch_start // batch_size + 1} inserted_rows: {inserted_rows}")
                try:
                    # method="multi" builds one INSERT with all rows in the chunk, so the
                    # bound-parameter count is ncols * chunksize; cap chunksize to stay under
                    # each dialect's per-statement parameter limit (sqlite ~999, postgres 65535).
                    ncols = len(records[0])
                    max_params = 999 if args.dbmode == "sqlite" else 65535
                    safe_chunksize = max(1, min(args.sqlchunksize, max_params // ncols))
                    pd.DataFrame(records).to_sql(name="torqtrips", con=session.connection(), if_exists="append", index=False, method="multi", chunksize=safe_chunksize,)
                except Exception as e:
                    logger.error(f"Failed to write torqtrips records for batch {batch_start // batch_size + 1}: {e} ({type(e)})")
                    session.rollback()
                    return -1
                inserted_rows += len(records)

            session.commit()
            logger.info(f"torqtrips batch {batch_start // batch_size + 1}: {min(batch_start + len(batch), len(fileids))}/{len(fileids)} fileids")
        except Exception as e:
            session.rollback()
            logger.error(f"torqtrips batch  ({type(e)}) {batch_start // batch_size + 1} failed: {e}")
            return -1

    logger.info(f"collect_db_torqtrips completed, wrote {inserted_rows} rows")
    return inserted_rows

def update_indexes(args):
    session = get_engine_session(args)
    for column in dataschema:
        if column in ["gpstime", "devicetime"]:
            continue
        index_name = f"idx_torqlogs_{column}_fileid_notnulls"
        try:
            session.execute(
                text(
                    f'CREATE INDEX IF NOT EXISTS "{index_name}" ON torqlogs ("fileid") WHERE {column} IS NOT NULL;'
                )
            )
            # session.execute(text(f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{index_name}" ON torqlogs ("{column}") WHERE {column} IS NOT NULL;'))
            logger.info(f"Ensured index on torqlogs.{column} for non-null fileid")
            session.commit()
        except Exception as e:
            logger.error(f"Failed to create index {index_name}: {e} ({type(e)})")
            session.rollback()

def get_args():
    parser = get_parser("dataupdate")
    parser.add_argument(
        "--db_speed",
        default=False,
        help="db_speed",
        action="store_true",
        dest="db_speed",
    )
    parser.add_argument(
        "--db_startends",
        default=False,
        help="db_startends",
        action="store_true",
        dest="db_startends",
    )
    parser.add_argument(
        "--db_startpos",
        default=False,
        help="db_startpos",
        action="store_true",
        dest="db_startpos",
    )
    parser.add_argument(
        "--db_endpos",
        default=False,
        help="db_endpos",
        action="store_true",
        dest="db_endpos",
    )
    parser.add_argument(
        "--db_columnstats",
        default=False,
        help="db_columnstats",
        action="store_true",
        dest="db_columnstats",
    )
    parser.add_argument(
        "--db_filestats",
        default=False,
        help="db_filestats",
        action="store_true",
        dest="db_filestats",
    )
    parser.add_argument(
        "--db_torqtrips",
        default=False,
        help="db_torqtrips",
        action="store_true",
        dest="db_torqtrips",
    )
    parser.add_argument(
        "--db_allstats",
        default=False,
        help="db_allstats",
        action="store_true",
        dest="db_allstats",
    )
    parser.add_argument(
        "--force_refresh",
        default=False,
        help="force full recalculation for start/end stats",
        action="store_true",
        dest="force_refresh",
    )
    return parser.parse_args()


def main(args):

    if args.dbmode == "sqlite":
        session = get_engine_session(args)
        session.execute(text("PRAGMA journal_mode=WAL;"))
        session.execute(text("pragma synchronous = normal;"))
        session.execute(text("pragma temp_store = memory;"))
        session.execute(text("pragma mmap_size = 30000000000;"))
    if args.db_filestats:
        return collect_db_filestats(args)
    if args.db_torqtrips:
        logger.info("starting torqtrips")
        return collect_db_torqtrips(args)
    elif args.db_columnstats:
        return collect_db_columnstats(args)
    elif args.db_startpos:
        return collect_db_startends(
            args, update_start=True, update_end=False, force_refresh=args.force_refresh
        )
    elif args.db_endpos:
        return collect_db_startends(
            args, update_start=False, update_end=True, force_refresh=args.force_refresh
        )
    elif args.db_startends:
        return collect_db_startends(args, force_refresh=args.force_refresh)
    elif args.db_speed:
        return collect_db_speeds(args)
    elif args.db_allstats:
        logger.debug("starting all stats")
        dbspeed = collect_db_speeds(args)
        logger.debug("all stats dbspeed done")
        dbstartends = collect_db_startends(args, force_refresh=args.force_refresh)
        logger.debug("all stats dbstartends done")
        dbcolumstats = collect_db_columnstats(args)
        logger.debug("all stats dbcolumstats done")
        dbfilestats = collect_db_filestats(args)
        logger.debug("all stats dbfilestats done")
        dbtorqtrips = collect_db_torqtrips(args)
        logger.debug("all stats dbtorqtrips done")
        return {
            "dbspeed": dbspeed,
            "dbstartends": dbstartends,
            "dbcolumstats": dbcolumstats,
            "dbfilestats": dbfilestats,
            "dbtorqtrips": dbtorqtrips,
        }
    else:
        logger.warning("missing args")


if __name__ == "__main__":
    args = get_args()
    try:
        r = main(args)
        logger.info(f"[main] got {type(r)}")
    except Exception as e:
        logger.error(f"unhandled {type(e)} {e}")
        sys.exit(-1)
