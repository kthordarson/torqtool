#!/usr/bin/python3
import asyncio
import sys
from sqlalchemy.orm import sessionmaker
from collections.abc import AsyncIterable
from pathlib import Path
import pandas as pd
from loguru import logger
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
# sys.path.append('c:/apps/torqtool/torqtool')
from utils import get_parser
from datamodels import TorqFile, Torqlogs, database_dropall, send_torqfiles
from utils import generate_torqdata, get_csv_files, get_engine_session, send_torqtripdata

# june2024 rewrite: log files are stored diffrently from previous versions
# now the app stores the logs on the phone under /storage/emulated/0/Documents/torqueLogs
# one log file per trip, the log file is named with the start time of the trip
# newer versions do not create profile.properties files
# todo pull profile.properties info from log files
# todo handle reading from previous versions
# todo merge logs from previous versions
# check log files for errors and cleanup
# if a log files contains entries from more than 24h, check and split ???
# more ....

async def collect_info(engine) -> AsyncIterable[str]:
    with engine.connect() as conn:
        logcount = conn.execute(text("select count(*) from torqlogs")).all()
        yield logcount

async def collect(async_iterable):
    return [item async for item in async_iterable]


async def main(args):
    # t0 = datetime.now()
    session = get_engine_session(args)
    if args.database_dropall:
        try:
            database_dropall(session)
            sys.exit(0)
        except OperationalError as e:
            logger.error(f"[main] database_dropall {e}")
            sys.exit(2)
        except Exception as e:
            logger.error(f"[main] database_dropall {type(e)} {e}")
            sys.exit(2)
    if args.dbinfo:
        # info = collect_info()
        tasks = [
            asyncio.create_task(collect(collect_info(session.get_bind()))),
            # asyncio.create_task(collect(iterable())),
            # asyncio.create_task(collect(iterable()))
        ]
        results = await asyncio.gather(*tasks)
        logger.info(f"[dbinfo]  {results}")
        # files = session.query(Torqtrips).count()
        # trips = session.query(Torqtrips).count()
        # logs = session.query(Torqlogs).count()
        # logger.info(f'[main] {files=} {trips=} {logs=:,} {data=}')
        sys.exit(0)
    if args.create_trips:
        sess = sessionmaker(bind=session.get_bind())
        session = sess()
        # create trips data from database
        tf_ids = session.query(TorqFile.fileid).all()
        data = pd.DataFrame()
        for idx, tf in enumerate(tf_ids):
            # data = session.query(Torqlogs).filter(Torqlogs.fileid == tf.fileid).all()
            try:
                data = pd.read_sql(session.query(Torqlogs).filter(Torqlogs.fileid == tf.fileid).statement, con=session.get_bind(),)
            except OperationalError as e:
                logger.error(f"{idx} {e} {tf=}")
                continue
            if not data.empty:
                tripdata = None
                logger.info(f"[{idx}/{len(tf_ids)}] Generating tripdata for fileid {tf.fileid} ")
                try:
                    tripdata = generate_torqdata(data, session, args)
                except Exception as e:
                    logger.error(f"[!] unhandled {type(e)} {e} {tf=}")
                    sys.exit(1)
                if tripdata:
                    logger.debug(f"[{idx}/{len(tf_ids)}] Sending {len(tripdata)} tripdata for fileid {tf.fileid} ")
                    send_torqtripdata(tripdata, session, args.debug)
        sys.exit(0)

if __name__ == "__main__":
    parser = get_parser("torqtool")
    args = parser.parse_args()
    asyncio.run(main(args))
