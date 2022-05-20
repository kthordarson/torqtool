# utils and db things here
import os
import sys
import re
from hashlib import md5
from pathlib import Path
from loguru import logger
from sqlalchemy import create_engine, MetaData, select, update
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError, DataError
from psycopg2.errors import DatatypeMismatch
from sqlalchemy.ext.declarative import declarative_base

from datetime import datetime

from datamodels import TorqTrip
logger.add('tool.log')
from pandas import to_datetime

from sqlalchemy.exc import OperationalError

MIN_FILESIZE = 4096
Base = declarative_base()
from datamodels import TorqEntry, TorqFile, TorqTrip, TorqLogEntry

def get_csv_files(searchpath:Path, recursive=True, dbmode=None):
	if not isinstance(searchpath, Path):
		searchpath = Path(searchpath)
	if not isinstance(searchpath, Path):
		logger.debug(f'[getcsv] err: searchpath {searchpath} is {type(searchpath)} need Path object')
		return []
	else: 
		# csvhash = md5(open(csv, 'rb').read()).hexdigest()
		torqcsvfiles = [({'csvfilename':k, 'size':os.stat(k).st_size, 'hash':md5(open(k, 'rb').read()).hexdigest(), 'dbmode':dbmode}) for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE]
		for idx,tf in enumerate(torqcsvfiles):
			with open(tf['csvfilename'], 'r') as reader:
				data = reader.readlines()
			lines = [str(k.replace(',- ',',0')) for k in data[0:]]
			orgcol = data[0].split(',')
			newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii','ignore').decode()
			newcolname += '\n'
			lines[0] = newcolname
			nf = f"{tf['csvfilename']}.fix"
			logger.debug(f"[f] {tf['csvfilename']} {nf} nclen:{len(newcolname)} og:{len(orgcol)} l:{len(lines)} l0:{len(lines[0])} dl0:{len(data[0])}")
			with open(nf, 'w') as writer:
				writer.writelines(lines)
				#d = DataFrame(columns=[newcolname], data=lines)
				#dflist.append(d)
				#d.to_csv(nf, quoting=False, index=False)
			torqcsvfiles[idx] = {'csvfilename':tf['csvfilename'], 'csvfilefixed':Path(nf), 'buflen':len(lines),'size':os.stat(tf['csvfilename']).st_size, 'hash':md5(open(tf['csvfilename'], 'rb').read()).hexdigest(), 'dbmode':dbmode}
		return torqcsvfiles


def read_torq_profile(filename):
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n') for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = to_datetime(pdata_date).to_pydatetime()
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
		return torq_trip
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')

def size_format(b):
    if b < 1000:
              return '%i' % b + 'B'
    elif 1000 <= b < 1000000:
        return '%.1f' % float(b/1000) + 'KB'
    elif 1000000 <= b < 1000000000:
        return '%.1f' % float(b/1000000) + 'MB'
    elif 1000000000 <= b < 1000000000000:
        return '%.1f' % float(b/1000000000) + 'GB'
    elif 1000000000000 <= b:
        return '%.1f' % float(b/1000000000000) + 'TB'


def database_init(engine):
	# close_all_sessions()
	meta = MetaData(engine)
	t1 = datetime.now()
	t=TorqEntry()
	temptables=[k.name for k in t.__mapper__.tables[0].columns]
	# tt2=[k.name for k in t.__mapper__.tables[0].columns]
	
	try:
		#logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping temptables')
		#Base.metadata.drop_all(bind=engine, tables=[t.__mapper__.tables[0].columns], checkfirst=False)
		for t in temptables:
			table = meta.tables.get(t)
			if table:
				Base.metadata.drop_all(bind=engine, tables=[table], checkfirst=False)
	except (OperationalError, ProgrammingError, AttributeError) as e:
		logger.error(f'dropall temptables {e}')
	try:
		logger.debug(f'[dbinit] {(datetime.now() - t1).total_seconds()} dropping from {meta}')
		Base.metadata.drop_all(bind=engine, tables=[TorqLogEntry.__table__, TorqEntry.__table__, TorqFile.__table__, TorqTrip.__table__], checkfirst=True)
	except (OperationalError, ProgrammingError) as e:
		logger.error(f'dropall {e}')
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


def clean_files(filelist):
	newfilelist = {}
	dflist = []
	for idx, tf in enumerate(filelist):
		with open(tf['filename'], 'r') as reader:
			data = reader.readlines()
		# lines= [(k.replace('  ','')) for idx,k in enumerate(data[1:]) if k.count('-') <=9 and len(k)>10]
		# lines0 = [(k.replace('  ','')).encode('ascii', 'ignore') for idx,k in enumerate(data[1:]) if k.count('-') <=11]
		#lines0 = [str((k.replace('   ','\n').encode('ascii', 'ignore'))) for k in data]]
		lines = [str(k.replace(',- ',',0')) for k in data[0:]]
		# lines = [k.encode('ascii', 'ignore') for k in lines_0]
		orgcol = data[0].split(',')
		newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii','ignore').decode()
		newcolname += '\n'
		#newcolname=[str(k).encode('ascii','ignore').decode() for k in newcolname.split(',')]
		#newcolname = f'id,{newcolname}'
		if newcolname.count(',') > 70 or 'CO' in newcolname or '2022' in orgcol: # or len(newcolname) == 1301:
			logger.warning(f'[22] nc {newcolname} oc:{orgcol}')
		# lines.insert(0, newcolname)
		lines[0] = newcolname
		nf = f"{tf['filename']}.fix"
		logger.debug(f"[f] {tf['filename']} {nf} nclen:{len(newcolname)} og:{len(orgcol)} l:{len(lines)} l0:{len(lines[0])} dl0:{len(data[0])}")
		with open(nf, 'w') as writer:
			writer.writelines(lines)
			#d = DataFrame(columns=[newcolname], data=lines)
			#dflist.append(d)
			#d.to_csv(nf, quoting=False, index=False)
		newfilelist[idx] = ({'filename':nf, 'buflen':len(lines), 'hash':md5(open(nf, 'rb').read()).hexdigest(), 'dbmode':'mysql'})
	return newfilelist
	# torqcsvfiles = [({'filename':k, 'size':os.stat(k).st_size, 'hash':md5(open(k, 'rb').read()).hexdigest(), 'dbmode':dbmode}) for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE]
