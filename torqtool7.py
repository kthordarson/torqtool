from loguru import logger
import os
import re
import argparse
from pandas import read_csv, Series, to_datetime, DataFrame
from numpy import nan
from datetime import datetime
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError, InternalError, NoReferencedTableError, ResourceClosedError, PendingRollbackError, IntegrityError
from sqlalchemy import ForeignKey, create_engine, Table, MetaData, Column, Integer, String, inspect, select, Numeric, DateTime, text, BIGINT, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, DataError
from hashlib import md5
from utils import get_csv_files
from datamodels import TorqEntry, TorqFile, TorqTrip, TorqLogEntry

Base = declarative_base()

def read_torq_trip(filename):
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n') for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = to_datetime(pdata_date).to_pydatetime()
			# logger.info(f'[rs] pd:{pdata_date} {type(pdata_date)} td:{tripdate} {type(tripdate)}')
		except (OperationalError, Exception) as e:
			logger.error(f'[readsend] {e}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		torq_trip = TorqTrip()
		torq_trip.fuelCost = float(trip_profile['fuelCost'])
		torq_trip.fuelUsed = float(trip_profile['fuelUsed'])
		torq_trip.distanceWhilstConnectedToOBD = float(trip_profile['distanceWhilstConnectedToOBD'])
		torq_trip.distance = float(trip_profile['distance'])
		torq_trip.time = float(trip_profile['time'])
		torq_trip.filename = p_filename
		torq_trip.tripdate = tripdate
		torq_trip.profile = trip_profile['profile']
		# torq_trip.tripid = tripid
		# trip_profile = DataFrame([trip_profile])
		return torq_trip
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')

def fixbuffer(buffer=None): #, torqentryid=None):
	t0 = datetime.now()
	# buffer['id'] = Column('id', Integer, primary_key=True, autoincrement="auto", unique=True)
	# h:{csvhash} t:{tripid_} {type(tripid_)} p:{profile}')
	# logger.info(f'[fixb] b:{len(buffer)} id:{torqentryid}')
	# buffer['torqentryid'] = Series(data=[f'{torqentryid}' for k in range(len(buffer))])
	#buffer['torqfilename'] = Series(data=[f'{csvfile}' for k in range(len(buffer))])
	#buffer['hash'] = Series(data=[f'{csvhash}' for k in range(len(buffer))])
	#buffer['torqfileid'] = Series(data=[f'{fileid}' for k in range(len(buffer))])
	#buffer['tripid'] = Series(data=[f'{tripid_}' for k in range(len(buffer))])
	#buffer['profile'] = Series(data=[f'{profile}' for k in range(len(buffer))])
	badvals_str = ['9999999999', '-9999999999', '3.402823466385289e+38', '-3402823534620772000000000000000000000', '-3.402823534620772e+36', '-3.4028236187100775e+36', '-3.402823618710077e+36',
		'-3402823618710077500000000000000000000', '612508207723425200000000000000000000000', '612508207723425231880386882817669201920', '340282346638528860000000000000000000000']
	badvals = [9999999999, -9999999999, 3.402823466385289e+38, -3402823534620772000000000000000000000, -3.402823534620772e+36, -3.4028236187100775e+36, -3.402823618710077e+36, -
		3402823618710077500000000000000000000, 612508207723425200000000000000000000000, 612508207723425231880386882817669201920, 340282346638528860000000000000000000000]

	newbuff = DataFrame()
	col_list = []
	for col in buffer.columns:
		for b in badvals:
			buffer[col].replace(to_replace=b, value=0, regex=True, inplace=True)
	for col in buffer.columns:			
		for b in badvals_str:
			buffer[col].replace(to_replace=b, value=0,  regex=True, inplace=True)
	for col in buffer.columns:			
		newname_ = re.sub('\W', '', col)  # .encode('ascii', 'ignore')
		newname = newname_.encode('ascii', 'ignore').decode()
		col_list.append(newname)
	newbuff = buffer.set_axis(col_list, axis=1, inplace=False)
		#newbuff[newname] = buffer[col].rename(newname) # columns={col: newname})
		#logger.debug(f'[f] c:{col} n:{newname} nb:{newbuff[newname].name} bc:{len(buffer.columns)} nbc:{len(newbuff.columns)}')
		# tempbuffer = buffer[newname].replace(nan, 0)  # .transpose()
		# buffer[newname] = tempbuffer.transpose()
	logger.info(f'[fixb] done time: {(datetime.now() - t0).seconds} b:{len(buffer)} ')
	return newbuff

def database_init(engine):
	meta = MetaData(engine)
	t1 = datetime.now()
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
	tables = (TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__)
	try:
		Base.metadata.drop_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=True)
	except OperationalError as e:
		logger.error(f'drop {e}')
	try:
		Base.metadata.create_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=False)
	except OperationalError as e:
		logger.error(f'metacreateall {e}')
	logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} done')


def chunks(l, n):
	"""Yield n number of sequential chunks from l."""
	d, r = divmod(len(l), n)
	for i in range(n):
		si = (d + 1) * (i if i < r else r) + d * (0 if i < r else i - r)
		yield l[si:si + (d + 1 if i < r else d)]


def read_and_send(csvfile=None, engine=None, session=None, Session=None, csvhash=None, tablename=None, conn=None):

	meta = MetaData(engine)
	conn = engine.connect()
	session = Session()
	t0 = datetime.now()
	# tripid = str(csvfile.parts[-2])

	trip = read_torq_trip(csvfile)
	trip.hash = csvhash
	session.add(trip)
	session.commit()	
	tfile = TorqFile()
	tfile.hash = csvhash
	tfile.torqfilename = csvfile
	tfile.tripid = trip.tripid
	tfile.profile = trip.profile
	session.add(tfile)
	session.commit()
	torqlogentry = TorqLogEntry()
	torqlogentry.tripid = tfile.tripid
	torqlogentry.torqfileid = tfile.torqfileid
	session.add(torqlogentry)
	session.commit()
	# session.close()
	torqbuffer = read_csv(csvfile, delimiter=',', low_memory=False, skipinitialspace=True, thousands=',', keep_default_na=False, on_bad_lines='skip')
	buffer = fixbuffer(buffer=torqbuffer)# , torqentryid=torqlogentry.torqentryid)
	# tripdata = [f'{tfile.tripid}' for k in range(len(buffer))]
	tripbuffer = Series(data=[f'{tfile.tripid}' for k in range(len(buffer))])
	buffer['tripid'] = DataFrame(data=tripbuffer, columns=['tripid'])
	# tripdata = [f'{tfile.tripid}' for k in range(len(buffer))]

	tripbuffer = Series(data=[f'{tfile.torqfileid}' for k in range(len(buffer))])
	buffer['torqfileid'] = DataFrame(data=tripbuffer, columns=['torqfileid'])

	#buffer['torqfileid'] = Series(data=[f'{tfile.tripid}' for k in range(len(buffer))], index='torqfileid')
	# buffer.tripid =  Column('tripid', Integer, ForeignKey('torqtrips.tripid'))
	# buffer.torqfileid = Column('torqfileid', Integer, ForeignKey('torqfiles.torqfileid'))
	logger.debug(f'[rs] sending c:{csvfile} size:{len(buffer)} tlid:{torqlogentry.torqentryid}')
	t1 = datetime.now()
	try:
		logger.info(f'[tosql] csv:{csvfile} b:{len(buffer)} ')
		buffer.to_sql(con=engine, name='torqlogs', if_exists='append',  method='multi', chunksize=10000, index=False)
	except (OperationalError, IntegrityError) as e:
		logger.error(f'csv:{csvfile} {e.code} {e.args[0]} tripid:{trip.tripid} profid:{trip.tripid} tfileid:{tfile.torqfileid}')

	# for item in buffer:
	# 	try:
	# 		logger.info(f'[tosql] {item} buffer[item] {len(buffer[item])} {type(buffer[item])}')
	# 		buffer[item].to_sql(con=engine, name='torqlogs', if_exists='append',  method='multi', chunksize=10000, index_label='entry_id')
	# 	except (OperationalError, IntegrityError) as e:
	# 		logger.error(f'csv:{csvfile} {e.code} {e.args[0]} tripid:{trip.tripid} profid:{trip.tripid} tfileid:{tfile.torqfileid}')
	logger.debug(f'[rs] send done c:{csvfile} time: {(datetime.now() - t0).seconds} time: {(datetime.now() - t1).seconds} tripid:{trip.tripid} profid:{trip.tripid} tfileid:{tfile.torqfileid} ')# 
	return


def main(args):
	t0 = datetime.now()
	TORQDBHOST = 'elitedesk' # os.getenv('TORQDBHOST')
	TORQDBUSER = 'torq' # os.getenv('TORQDBUSER')
	TORQDBPASS = 'dzt3f5jCvMlbUvRG'
	TORQDATABASE = 'torq7'
	dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/{TORQDATABASE}?charset=utf8mb4" # &sessionVariables=sql_mode='NO_ENGINE_SUBSTITUTION'"
	engine = create_engine(dburl, pool_size=200, max_overflow=0)# , isolation_level='AUTOCOMMIT')

	Session = sessionmaker(bind=engine)
	session = Session()
	session.autoflush = True
	# sql = "SET sql_mode = 'NO_ENGINE_SUBSTITUTION'"
	# session.execute(sql)
	if not database_exists(dburl):
		create_database(dburl)
	if args.init_db:
		logger.debug(f'[mainpath] Calling init_db ... ')
		try:
			# sql = 'DROP TABLE IF EXISTS torqfiles;'
			sql = 'DROP TABLE IF EXISTS  torq7.torqfiles;'
			session.execute(sql)
		except IntegrityError as e:
			pass
			# logger.warning(f'[e] {e}')
		try:
			sql = 'DROP TABLE IF EXISTS  torq7.torqlogs;'
			session.execute(sql)
		except IntegrityError as e:
			pass
			# logger.warning(f'[e] {e}')
		try:
			sql = 'DROP TABLE IF EXISTS  torq7.torqtrips;'
			session.execute(sql)
		except IntegrityError as e:
			pass
			# logger.warning(f'[e] {e}')
		database_init(engine)

	hashres = session.execute(select(TorqFile)).fetchall()
	hashlist = [k[0].hash for k in hashres]
	filelist = []
	csv_file_list = get_csv_files(searchpath=args.path)
	for idx,csv in enumerate(csv_file_list):
		csvhash = md5(open(csv,'rb').read()).hexdigest()
		if csvhash in hashlist:
			logger.warning(f'[{csv}] already in database')
		else:
			filelist.append({'filename':csv, 'hash':csvhash})
	logger.debug(f'read start time: {(datetime.now() -t0).seconds} csv:{len(csv_file_list)} h:{len(hashlist)} fl:{len(filelist)}')
	for idx,csv in enumerate(filelist):
		read_and_send(csvfile=csv['filename'], Session=Session, csvhash=csv['hash'], engine=engine)
		logger.info(f'[Sent] {idx} of {len(csv_file_list)} r: {len(csv_file_list)-idx}')
	logger.debug(f'torqtask done time: {(datetime.now() -t0).seconds} ')

if __name__ == '__main__':
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
	t0 = datetime.now()
	main(args)
	logger.info(f'[main] done time: {datetime.now() - t0}')
