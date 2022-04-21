# torqtool
import os, sys, struct
from pathlib import Path
import argparse
from loguru import logger
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine
# from PyQt5.QtWidgets import QApplication

from utils import Torqfile, database_init, get_csv_files, check_db, init_db, dump_db, DataSender, make_column_list, parse_csvfile
# from torqform import TorqForm
# from tweb import start_web

from threading import Thread, active_count

MIN_READINGS = 3

def check_threads(threads):
    return True in [t.is_alive() for t in threads]

def stop_all_threads(threads):
    for t in threads:
        logger.debug(f'[stop_all_threads] stopping {t}')
        t.do_kill()
        t.kill = True
        t.join(timeout=3)


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
        self.dbhashlist = []
        self.csv_count = 0
        self.csv_totalcount = -1
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.session.expire_on_commit = False
        self.conn = self.engine.connect()
        self.initsession = False
        self.s_path = Path(self.args.path)
        self.kill = False
        self.torqthreads = []
        self.donethreads = 0
        self.csv_file_list = get_csv_files(searchpath=self.s_path)
        self.csv_totalcount = len(self.csv_file_list)
        self.started = False
        if self.args.init_db:
            logger.debug(f'[mainpath] Calling init_db ')
            init_db(self.engine)
            self.initsession = True
        logger.debug(f'[mainpath] thread init self.csv_totalcount: {self.csv_totalcount}')

    def do_kill(self):
        logger.info(f'[mainpath] do_kill')
        self.kill = True
        for t in self.torqthreads:
            logger.info(f'[mainpath] do_kill t:{t}')
            t.do_kill()
        logger.info(f'[mainpath] do_kill done')

    # self._stop()

    def run(self):
        while True:
            if self.kill:
                logger.info(f'[mainpath] kill signal ')
                for t in self.torqthreads:
                    logger.info(f'[mainpath] stopping threads {t}')
                    t.do_kill()
                    t.kill = True
                    t.join(timeout=1)
                return
            #while check_threads(self.torqthreads):
            #    if self.kill:
            #        return
                # dt_temp = 0
            self.donethreads = 0
            for t in self.torqthreads:
                if t.finished and self.started:
                    #dt_temp += 1
                    #logger.debug(f'[done] dt:{self.donethreads} {self.csv_totalcount} {self.csv_count}')
                    self.donethreads += 1 # dt_temp
            if self.donethreads == len(self.torqthreads) and self.started:
                logger.debug(f'[*done*] dt:{self.donethreads} totcnt:{self.csv_totalcount} csvcnt:{self.csv_count} thdrs:{len(self.torqthreads)}')
                return
            # logger.debug(f'[paththread] {len(self.torqthreads)}')

    def process(self):
        #self.started = True
        if not self.initsession:
            try:
                self.hashlist = [k[0] for k in self.conn.execute('select hash from torqfiles')]
                self.dbhashlist = [k[0] for k in self.conn.execute('select hash from torqlogs')]
                logger.debug(f'[mainpath] hashlist {len(self.hashlist)}')
            except ProgrammingError as e:
                logger.error(f'[torq] err {e.code} {e.orig}')
                database_init(self.engine)
                self.hashlist = []

    def gather_csvfiles(self):
        #self.started = True
        # self.csv_totalcount = len(self.csv_file_list) # len(get_csv_files(searchpath=self.s_path))
        # self.torqfiles = []
        for csvfile in self.csv_file_list: # get_csv_files(searchpath=self.s_path):
            # logger.debug(f'[mainpath] parsing {csvfile}')
            tfile = Torqfile(filename=csvfile, engine=self.engine)
            if tfile.hash in self.hashlist and tfile.hash in self.dbhashlist:  # already have file in database, skip
                logger.debug(f'[torqtool] {tfile.name} already exists in database, skipping')
            else:
                if tfile.num_lines > MIN_READINGS:
                    self.csv_count += 1
                    t1 = datetime.now()
                    self.torqfiles.append(tfile)
                    self.found_cols = tfile.get_columns()
                    self.column_list.append(self.found_cols)
                    tfile.name = str(tfile.name)
                    tfile.engine = self.engine
                    self.session.add(tfile)
                    logger.debug(f'[csvfile {self.csv_count}/{self.csv_totalcount}] [{(datetime.now() - t1).total_seconds()}] {csvfile.parent.name}/{csvfile.name} csvread: {parse_csvfile(csvfile)} cols: {len(self.found_cols)} / {len(self.column_list)} lines: {tfile.num_lines}')
                elif tfile.num_lines <= MIN_READINGS:
                    logger.error(f'[mainpath] invalid tfile {tfile} tfile.num_lines: {tfile.num_lines} MIN_READINGS: {MIN_READINGS} csvfile: {csvfile}')
        self.session.commit()
        self.maincolumn_list = make_column_list(self.column_list)  # maincolum_list = master list of columns
        logger.debug(f'[csv] file gathering done')

    def sendcsvdata(self):
        t1 = datetime.now()
        logger.debug(f'[mainpath] starting data send')

        chunk_size = 5
        for torqfile in self.torqfiles:
            torqfile.maincolumn_list = self.maincolumn_list
        self.chunkedlist = [k for k in chunks(self.torqfiles, chunk_size)]
        # logger.debug(f'[chunk] {len(self.chunkedlist)} {self.chunkedlist}')
        for thread in range(chunk_size):
            t_thread = DataSender(thread, self.chunkedlist[thread])
            self.torqthreads.append(t_thread)
            logger.debug(f'[mainpath] {thread} starting thread:{t_thread} torqthreads: {len(self.torqthreads)}')
        #for t in self.torqthreads:
            t_thread.daemon = True
            t_thread.start()
        self.started = True


#		for t in self.torqthreads:
#			t.join(timeout=3)

def check_file(args, engine):
    # check if file is accisble and contains valid data
    # logger.debug out info about file or existing database entry for file
    logger.debug(f'[check_file] Checking {args.file}')
    tfile = Torqfile(filename=args.file, engine=engine)
    logger.debug(f'[check_file] tfile {tfile}')
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        conn = engine.connect()
    except OperationalError as e:
        logger.debug(f'[check_file] err {e}')
        sys.exit(-1)
    hashlist = [k[0] for k in conn.execute('select hash from torqfiles')]
    if tfile.hash in hashlist:  # already have file in database, skip
        # todo logger.debug info about database entry for existing file
        res = [k for k in conn.execute(f'select DeviceTime from torqlogs where hash = "{tfile.hash}"')]
        logger.debug(f'[check_file] {tfile.name} already exists in database. date0: {res[0]} datex: {res[-1]}')
    else:
        # continue checking file
        tfile.buffer = tfile.read_csv_data()
        logger.info(f'[check_file] buffer {len(tfile.buffer)}')
    os._exit(1)


# def main_gui(args, engine):
#     logger.debug(f'[maingui] {args}')
#     app = QApplication([])
#     form = TorqForm(ui_file='untitled.ui', engine=engine, s_path=args.path)
#     sys.exit(app.exec_())


if __name__ == '__main__':

    TORQDBHOST = 'elitedesk'  # os.getenv('TORQDBHOST')
    TORQDBUSER = 'torq'  # os.getenv('TORQDBUSER')
    TORQDBPASS = 'dzt3f5jCvMlbUvRG'  # os.getenv('TORQDBPASS')
    if TORQDBHOST is None or TORQDBUSER is None or TORQDBPASS is None:
        logger.error(f'[err] check sql config')
        os._exit(-1)
    else:
        try:
            engine = create_engine(f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/torq?charset=utf8mb4", isolation_level='AUTOCOMMIT')
            # engine = create_engine(f"postgresql://postgres:foobar9999@elitedesk/torq")
        except OperationalError as e:
            logger.error(f'[sql] ERR {e}')
            os._exit(-1)
    parser = argparse.ArgumentParser(description="torqtool")
    parser.add_argument("--path", nargs="?", default=".", help="path to csv files", action="store")
    parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
    parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
    parser.add_argument("--check-db", default=False, help="check database", action="store_true", dest='check_db')
    parser.add_argument("--init-db", default=False, help="init database", action="store_true", dest='init_db')
    parser.add_argument("--dump-db", nargs="?", default=None, help="dump database to file", action="store")
    parser.add_argument("--check-file", default=False, help="check database", action="store_true", dest='check_file')
    parser.add_argument("--webstart", default=False, help="start web listener", action="store_true", dest='web')
    args = parser.parse_args()
    # maint = MainThread(name='main')
    threadlist = []
    paththread = MainPath(args, engine)
    # check_db(engine=engine)
    if len(args.path) > 1 and len(args.file) > 1:
        logger.debug(f'path or file, exit!')
        os._exit(-1)
    if args.dump_db:
        dump_db(args, engine)
    if args.web:
        pass
        # start_web(engine=engine)
    if args.check_db:
        logger.debug(f'[maincheck] start')
        try:
            check_db(engine=engine)
            logger.debug(f'[maincheck] done')
        except struct.error as e:
            logger.error(f'[checkdb] {e}')
            sys.exit(-1)
        logger.debug(f'[maincheck] done')
        paththread.conn.close()
        # engine = None
        # engine.dispose()
        os._exit(0)
    if args.check_file:
        check_file(args, engine)
    if args.gui:
        pass
        # main_gui(args, engine)
    if len(args.path) > 1:
        logger.debug(f'[path] {args.path} ')        
        paththread.daemon = True
        threadlist.append(paththread)
        paththread.start()
        paththread.process()
        paththread.gather_csvfiles()
        paththread.sendcsvdata()
    # mainpath(args, engine)
    if len(args.file) > 1:
        logger.debug(f'[file] {args.file}')
    # mainfile(args, engine)

    while check_threads(threadlist):
        try:
            cmd = input(': ')
            if cmd[:1] == 'q':
                for t in paththread.torqthreads:
                    logger.info(f'[main] stopping {t}')
                    t.kill = True
                stop_all_threads(threadlist)
            if cmd[:1] == 's':
                paththread.process()
                paththread.gather_csvfiles()
                paththread.sendcsvdata()
            if cmd[:1] == 'd':
                total_remaining = 0
                logger.debug(f'[d] donethr:{paththread.donethreads} paththr:{len(paththread.torqthreads)} torqfiles:{len(paththread.torqfiles)} hashlist: {len(paththread.hashlist)} dbhash: {len(paththread.dbhashlist)} c: {paththread.csv_count}/{paththread.csv_totalcount}')
                for t in paththread.torqthreads:
                    logger.debug(f'[debug] t:{t} {t.get_status()}')
                    total_remaining += t.get_remaining()
                logger.debug(f'[d] total remaining: {total_remaining}')
        except KeyboardInterrupt:
            stop_all_threads(threadlist)
        except Exception as e:
            logger.error(f'E in main {e}')
            stop_all_threads(threadlist)

# find and read all csv files, starting recursivly from '--path'
# check column names, remove extra spaces, save result
# make masterlist of columns
