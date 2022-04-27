# torqtool
import os, sys, struct
from pathlib import Path
import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine
# from PyQt5.QtWidgets import QApplication

from utils import get_torqlog_table,Torqfile, database_init, get_csv_files, check_db, init_db, dump_db, DataSender, make_column_list, parse_csvfile
# from torqform import TorqForm
# from tweb import start_web

from threading import Thread, active_count

from loguru import logger
logger.add('tool.log')

MIN_READINGS = 3
MAX_THREADS = 7
CHUNK_SIZE = 3
SQLCHUNKSIZE = 1000

def check_threads(threads):
    return True in [t.is_alive() for t in threads]

def stop_all_threads(threads):
    for t in threads:
        logger.debug(f'[stop_all_threads] stopping {t}')
        t.do_kill()
        t.kill = True
        t.join(timeout=1)


def chunks(l, n):
    """Yield n number of sequential chunks from l."""
    d, r = divmod(len(l), n)
    for i in range(n):
        si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
        yield l[si:si + (d + 1 if i < r else d)]


class MainPath(Thread):
    def __init__(self, args, engine):
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
            self.max_workers = int(int(self.args.max_workers) / self._chunks)
        else:
            self.max_workers = MAX_THREADS
        

    def do_kill(self):
        # logger.info(f'[mainpath] do_kill')
        self.kill = True
        for t in self.torqthreads:
            logger.info(f'[mainpath] do_kill t:{t}')
            t.do_kill()
        # logger.info(f'[mainpath] do_kill done')

    def run(self):
        time_start = datetime.now()
        logger.debug(f'[mainpath] thread started')
        while True:
            if self.kill:
                logger.info(f'[mainpath] self.kill:{self.kill} ')
                for t in self.torqthreads:
                    logger.info(f'[mainpath] stopping {t} tk:{t.kill} ')
                    t.do_kill()
                    t.kill = True
                    t.join(timeout=1)
                return

    def process(self):
        self.hashlist = [k[0] for k in self.conn.execute('select hash from torqfiles')]

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
                tfile.name = str(tfile.name)
                tfile.engine = self.engine
                self.session.add(tfile)
                # logger.debug(f'[csvfile {csv_counter}/{self.csv_totalcount}] {csvfile.name} csvread: {parse_csvfile(csvfile)} cols: {len(self.found_cols)} / {len(self.column_list)} ')
        logger.debug(f'[csv] total: {self.csv_totalcount}')
        self.session.commit()
        self.maincolumn_list = make_column_list(self.column_list)  # maincolum_list = master list of columns
        # logger.debug(f'[csv] file gathering done')

    def get_chunked_list(self):
        for torqfile in self.torqfiles:
            torqfile.maincolumn_list = self.maincolumn_list
        self.chunkedlist = [k for k in chunks(self.torqfiles, self._chunks)]
        return self.chunkedlist


    def get_sender_threads(self):
        chunklist = self.get_chunked_list()
        for thread in range(len(chunklist)):
            t_thread = DataSender(thread_id=thread, torqfiles=self.chunkedlist[thread], max_workers=self.max_workers)
            self.torqthreads.append(t_thread)
        self.started = True
        logger.debug(f'[senders] total: {len(self.torqthreads)} cl: {len(chunklist)}')
        return self.torqthreads



if __name__ == '__main__':
    timestart = datetime.now()
    TORQDBHOST = 'elitedesk'  # os.getenv('TORQDBHOST')
    TORQDBUSER = 'torq'  # os.getenv('TORQDBUSER')
    TORQDBPASS = 'dzt3f5jCvMlbUvRG'  # os.getenv('TORQDBPASS')
    TORQDATABASE = 'torqdev'
    engine = create_engine(f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4")# , isolation_level='AUTOCOMMIT')
    parser = argparse.ArgumentParser(description="torqtool")
    parser.add_argument("--path", nargs="?", default=".", help="path to csv files", action="store")
    parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
    parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
    parser.add_argument("--check-db", default=False, help="check database", action="store_true", dest='check_db')
    parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')
    parser.add_argument("--fixcsv", default=False, help="repair csv", action="store_true", dest='fixcsv')
    parser.add_argument("--dump-db", nargs="?", default=None, help="dump database to file", action="store")
    parser.add_argument("--check-file", default=False, help="check database", action="store_true", dest='check_file')
    parser.add_argument("--webstart", default=False, help="start web listener", action="store_true", dest='web')
    parser.add_argument("--sqlchunksize", nargs="?", default="1000", help="sql chunk", action="store")
    parser.add_argument("--max_workers", nargs="?", default="4", help="max_workers", action="store")
    parser.add_argument("--chunks", nargs="?", default="4", help="chunks", action="store")
    args = parser.parse_args()
    threadlist = []
    sender_threads = []
    paththread = MainPath(args, engine)

    if args.init_db:
        logger.debug(f'[mainpath] Calling init_db ... ')
        init_db(engine)

    if len(args.path) > 1:
        paththread.daemon = True
        threadlist.append(paththread)
        paththread.start()
        paththread.process()
        paththread.gather_csvfiles()
        senders = paththread.get_sender_threads()
        for s in senders:
            sender_threads.append(s)
            s.daemon = True
            s.start()

    torqcount = 0
    while True:
        total_remaining = 0
        for t in sender_threads:
            total_remaining += t.get_remaining()
        if total_remaining == 0:
            logger.info(f'[main] total_remaining:{total_remaining} st:{len(sender_threads)} ')
            for st in sender_threads:
                logger.debug(f'[mainst] status {st.get_status()}')
            stop_all_threads(sender_threads)
            stop_all_threads(threadlist)
            break
    timeend = datetime.now() - timestart
    # torqcount = get_torqlog_table(engine)
    logger.info(f'[timeend] tc:{torqcount} path:{args.path} time:{timeend} sqlchunksize:{args.sqlchunksize} chunks:{args.chunks} max_workers:{args.max_workers}')




        # try:
        #     cmd = input(' > ')
        #     if cmd[:1] == 'q':
        #         for t in paththread.torqthreads:
        #             logger.info(f'[main] quit stopping thread {t} tk:{t.kill} tf:{t.finished}')
        #             t.kill = True
        #         stop_all_threads(threadlist)
        #         break
        #     if cmd[:1] == 't':
        #         torqcount = get_torqlog_table(engine)
        #         logger.info(f'[t] {torqcount}')
        #     if cmd[:1] == 'd':
        #         threadremains = 0
        #         logger.debug(f'[d] tc:{torqcount} paththr:{len(paththread.torqthreads)} torqfiles:{len(paththread.torqfiles)} hashlist: {len(paththread.hashlist)} ')
        #         for t in paththread.torqthreads:
        #             if not t.kill:
        #                 logger.debug(f'[dt] {t.get_status()}')
        #                 threadremains += t.get_remaining()
        #         logger.debug(f'[d] threadremains: {threadremains} total remaining: {total_remaining} elapsed: {datetime.now() - timestart}')
        # except KeyboardInterrupt:
        #     stop_all_threads(threadlist)
        #     break
        # except Exception as e:
        #     logger.error(f'E in main {e}')
        #     stop_all_threads(threadlist)
        #     break
