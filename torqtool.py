# torqtool
import os, sys, struct
import functools
import asyncio
from multiprocessing import Value
from typing import List, Dict
from pathlib import Path
from pandas import read_csv
import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine
# from PyQt5.QtWidgets import QApplication

from utils import DataProcessor,get_torqlog_table,Torqfile, database_init, get_csv_files, check_db, init_db, dump_db, make_column_list, parse_csvfile
# from torqform import TorqForm
# from tweb import start_web

from threading import Thread, active_count

from loguru import logger
logger.add('tool.log')

MIN_READINGS = 3
MAX_THREADS = 7
CHUNK_SIZE = 3
SQLCHUNKSIZE = 1000
from typing import Callable, Any

def check_threads(threads):
    return True in [t.is_alive() for t in threads]

def stop_all_threads(threads):
    for t in threads:
        logger.debug(f'[stop_all_threads] stopping {t}')
        t.do_kill()
        try:
            t.join(timeout=1)
        except AssertionError as e:
            pass
        t.kill = True

def chunks(l, n):
    """Yield n number of sequential chunks from l."""
    d, r = divmod(len(l), n)
    for i in range(n):
        si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
        yield l[si:si + (d + 1 if i < r else d)]

class MainPath(Thread):
    def __init__(self, args=None, engine=None):
        Thread.__init__(self)
        self.args = args
        self.engine = engine
        self.column_list = []
        self.found_cols = []
        self.torqfiles = []
        self.hashlist = []
        self.csv_totalcount = -1
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.session.expire_on_commit = False
        self.conn = self.engine.connect()
        self.s_path = Path(self.args.path)
        self.kill = False
        self.torqthreads = []
        self.csv_file_list = get_csv_files(searchpath=self.s_path)
        self.csv_totalcount = len(self.csv_file_list)
        self.started = False
        if self.args.chunks:
            self._chunks = int(self.args.chunks)
        else:
            self._chunks = CHUNK_SIZE
        if self.args.max_workers:
            self.max_workers = int(self.args.max_workers) # int(int(self.args.max_workers) / self._chunks)
        else:
            self.max_workers = MAX_THREADS
        self.threads_started = False

    def do_kill(self):
        # logger.info(f'[mainpath] do_kill')
        self.kill = True
        for t in self.torqthreads:
            logger.info(f'[mainpath] do_kill t:{t}')
            t.do_kill()
        # logger.info(f'[mainpath] do_kill done')

    def start_senders(self):
        pass

    def run(self):
        time_start = datetime.now()
        logger.debug(f'[mainpath] thread started st:{len(self.torqthreads)}')
        asyncio.run(self.torq_readers())
        while True:
            if self.kill:
                logger.info(f'[mainpath] self.kill:{self.kill} ')
                for t in self.torqthreads:
                    logger.info(f'[mainpath] stopping {t} tk:{t.kill} ')
                    t.do_kill()
                    t.kill = True
                    try:
                        t.join(timeout=1)
                    except AssertionError:
                        pass
                return

    async def torq_readers(self):
        self.gather_csvfiles()
        logger.debug(f'[tr] t:{len(self.torqfiles)} c:{len(self.csv_file_list)}')
        loop = asyncio.get_event_loop()
        tasks = []
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            for torqfile in self.torqfiles:
                logger.debug(f'[p] {torqfile} {len(self.torqfiles)}')
                tasks.append(loop.run_in_executor(executor, functools.partial(torqfile.do_self_fix)))
                # tasks.append(loop.run_in_executor(executor, functools.partial(torqfile, None)))
        await asyncio.gather(*tasks)

            # buffer = read_csv(self.filename, delimiter=',', low_memory=False, encoding='cp1252', na_values=0)
            # torqfile.buffer = buffer

    def get_hashlist(self):
        hashlist = [k[0] for k in self.conn.execute('select hash from torqfiles')]
        return hashlist

    def gather_csvfiles(self):
        csv_counter = 0
        if self.args.sqlchunksize:
            sql_chunk = int(self.args.sqlchunksize)
        else:
            sql_chunk = SQLCHUNKSIZE
        for csvfile in self.csv_file_list: # get_csv_files(searchpath=self.s_path):
            tfile = Torqfile(filename=csvfile, engine=self.engine, sqlchunksize=sql_chunk)
            if tfile.hash in self.hashlist: # and tfile.hash in self.dbhashlist:  # already have file in database, skip
                logger.warning(f'[torqtool] {tfile.name} already exists in database, skipping ')
            else:
                csv_counter += 1
                self.torqfiles.append(tfile)
                self.found_cols = tfile.get_columns()
                self.column_list.append(self.found_cols)
                self.session.add(tfile)
                # logger.debug(f'[csvfile {csv_counter}/{self.csv_totalcount}] {csvfile.name} csvread: {parse_csvfile(csvfile)} cols: {len(self.found_cols)} / {len(self.column_list)} ')
        logger.debug(f'[csv] total: {self.csv_totalcount}')
        # self.session.commit()
        self.maincolumn_list = make_column_list(self.column_list)  # maincolum_list = master list of columns
        # logger.debug(f'[csv] file gathering done')

    def get_chunked_list(self):
        for torqfile in self.torqfiles:
            torqfile.maincolumn_list = self.maincolumn_list
        self.chunkedlist = [k for k in chunks(self.torqfiles, self._chunks)]
        return self.chunkedlist

    def get_sender_threads(self):
        return []

    def get_sender_threadsx(self):
        chunklist = self.get_chunked_list()
        for thread in range(len(chunklist)):
            t_thread = DataProcessor(thread_id=thread, torqfiles=self.chunkedlist[thread], max_workers=self.max_workers)
            self.torqthreads.append(t_thread)
        self.started = True
        logger.debug(f'[senders] total: {len(self.torqthreads)} cl: {len(chunklist)}')
        return self.torqthreads

