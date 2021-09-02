# torqtool
import os, sys
from pathlib import Path
import argparse
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import create_engine
from PyQt5.QtWidgets import QApplication

from utils import Torqfile, database_init, get_csv_files
from utils import TorqForm
from utils import check_db
from tweb import start_web
TORQDBHOST = os.getenv('TORQDBHOST')
TORQDBUSER = os.getenv('TORQDBUSER')
TORQDBPASS = os.getenv('TORQDBPASS')

def parse_csvfile(csv_filename):
	if len(csv_filename.parent.name) == 13: # torq creates folders based on unix time with milliseconds
		dateguess = datetime.utcfromtimestamp(int(csv_filename.parent.name)/1000)
	else: # normal....
		dateguess = datetime.utcfromtimestamp(int(csv_filename.parent.name))
	# print(datetime.utcfromtimestamp(16294475123))
	return f'[parser] dateguess: {dateguess}'

def make_column_list(columnlist):
	templist = []
	for list  in columnlist:
		for col in list:
			if col in templist:
				pass
			else:
				templist.append(col)
				# print(f'[templist] {len(templist)} added {col}')
	templist = sorted(set(templist))
	with open('tempfields.txt', 'a') as f:
		f.writelines(templist)
	return templist

def main(args, engine):
	column_list = []
	torqfiles = []
	Session = sessionmaker(bind=engine)
	session = Session()
	conn = engine.connect()
	try:
		hashlist = [k[0] for k in conn.execute('select hash from torqfiles')]
	except ProgrammingError as e:
		print(f'[torq] err {e.code} {e.orig}')
		database_init(engine)
		hashlist = []
	s_path = Path(args.path)
	for csvfile in get_csv_files(searchpath=s_path):
		tfile = Torqfile(filename=csvfile)
		if tfile.hash in hashlist: # already have file in database, skip
			print(f'[torqtool] {tfile.name} already exists in database, skipping')
		else:
			torqfiles.append(tfile)
			found_cols = tfile.get_columns()			
			column_list.append(found_cols)		
			session.add(tfile)
			# tfile.send_data(engine=engine, cols=maincolumn_list)
			print(f'[csvfile] {csvfile.parent.name}/{csvfile.name} csvread: {parse_csvfile(csvfile)} cols: {len(found_cols)} / {len(column_list)} torqentries: {tfile.num_lines}')
	session.commit()
	maincolumn_list = make_column_list(column_list) # maincolum_list = master list of columns
	for torqfile in torqfiles:
		torqfile.update_columns(engine=engine, cols=maincolumn_list)
		torqfile.send_data(engine=engine, cols=maincolumn_list)


def main_gui(args, engine):
	print(f'[maingui] {args}')
	app = QApplication([])
	form = TorqForm(ui_file='untitled.ui', engine=engine, s_path=args.path)
	sys.exit(app.exec_())

if __name__ == '__main__':
	try:
		engine = create_engine(f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/torq?charset=utf8mb4")
	except OperationalError as e:
		print(f'[sql] ERR {e}')
		os.exit(-1)

	parser = argparse.ArgumentParser(description="torqtool")
	parser.add_argument("--path", nargs="?", default=".", help="path to csv files", action="store")
	parser.add_argument("--file", nargs="?", default=".", help="path to single csv file", action="store")
	parser.add_argument("--gui", default=False, help="Run gui", action="store_true", dest='gui')
	parser.add_argument("--check-db", default=False, help="check database", action="store_true", dest='check_db')
	parser.add_argument("--webstart", default=False, help="start web listener", action="store_true", dest='web')
	args = parser.parse_args()
	print(f'[torqtool] {args}')
	if args.web:
		start_web(engine=engine)
	if args.check_db:
		check_db(engine=engine)
	if args.gui:
		main_gui(args, engine)
	else:
		main(args, engine)

# find and read all csv files, starting recursivly from '--path'
# check column names, remove extra spaces, save result
# make masterlist of columns