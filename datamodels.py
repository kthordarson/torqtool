import os
import uuid
from datetime import datetime
from enum import unique
from hashlib import md5
from pathlib import Path

from loguru import logger
from numpy import nan
from pandas import DataFrame, Series, concat, to_datetime
from sqlalchemy import (BIGINT, DDL, BigInteger, Column, DateTime, Float,
                        ForeignKey, Integer, MetaData, Numeric, String, Table,
                        create_engine, inspect, select, text)
from sqlalchemy.exc import (ArgumentError, DataError, IntegrityError,
                            InternalError, OperationalError, ProgrammingError)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists

from torqsqlstrings import sqlcmds


def genuuid():
	return str(uuid.uuid4())


def send_torq_trip(tf=None, tripdict=None, session=None, engine=None):
	# logger.debug(f'[stt] tf:{tf} `td:{tripdict} s:{session} e:{engine}')
	if not tripdict:
		logger.error(f'[stt] no tripdict tf={tf}')
		return None
	distance = tripdict['distance']
	fuelcost = tripdict['fuelcost']
	fuelused = tripdict['fuelused']
	distancewhilstconnectedtoobd = tripdict['distancewhilstconnectedtoobd']
	time = tripdict['time']
	tripdate = tripdict['tripdate']
	profile = tripdict['profile']
	csvfilename = tripdict['csvfilename']
	csvhash = tripdict['csvhash']
	sql = ''
	if engine.name == 'mysql':
		engine.execute('SET FOREIGN_KEY_CHECKS=0')
		sql = f"""insert into torqtrips (csvfilename, csvhash, distance, fuelcost, fuelused, distancewhilstconnectedtoobd, tripdate, profile, time) values ("{csvfilename}", "{csvhash}", "{distance}", "{fuelcost}", "{fuelused}", "{distancewhilstconnectedtoobd}", "{tripdate}", "{profile}","{time}");"""
	if engine.name == 'sqlite':
		sql = f'insert into torqtrips (csvfilename, csvhash, distance, fuelcost, fuelused, distancewhilstconnectedtoobd, tripdate, profile, time) values ("{csvfilename}", "{csvhash}",  "{distance}", "{fuelcost}", "{fuelused}", "{distancewhilstconnectedtoobd}", "{tripdate}", "{profile}","{time}");'
	if engine.name == 'postgresql':
		sql = f"insert into torqtrips (csvfilename, csvhash, distance, fuelcost, fuelused, distancewhilstconnectedtoobd, tripdate, profile, time) values ('{csvfilename}', '{csvhash}', '{distance}', '{fuelcost}', '{fuelused}', '{distancewhilstconnectedtoobd}', '{tripdate}', '{profile}','{time}') returning id;"
	try:
		res = engine.execute(sql)
		if engine.name == 'postgresql':
			id = res.fetchone()[0]
		else:
			id = res.lastrowid
			engine.execute('SET FOREIGN_KEY_CHECKS=1')
		return id
	except (DataError, IntegrityError) as e:
		logger.error(f'[sql] errcode {e.code} errargs: {e.args[0]}')
		return None


def get_trip_profile(filename):
	filename = Path(filename)
	p_filename = os.path.join(filename.parent, 'profile.properties')
	with open(p_filename, 'r') as f:
		pdata_ = f.readlines()
	if len(pdata_) == 8:
		pdata = [l.strip('\n').lower() for l in pdata_ if not l.startswith('#')]
		try:
			pdata_date = str(pdata_[1][1:]).strip('\n')
			tripdate = to_datetime(pdata_date)
			tripdate = tripdate.strftime('%Y-%m-%d %H:%M:%S')
		# logger.info(f'[tripdate] {tripdate}')
		except (OperationalError, Exception) as e:
			logger.error(f'[readsend] {e}')
			tripdate = None
		trip_profile = dict([k.split('=') for k in pdata])
		torq_trip = {}
		torq_trip['fuelcost'] = float(trip_profile['fuelcost'])
		torq_trip['fuelused'] = float(trip_profile['fuelused'])
		torq_trip['distancewhilstconnectedtoobd'] = float(trip_profile['distancewhilstconnectedtoobd'])
		torq_trip['distance'] = float(trip_profile['distance'])
		torq_trip['time'] = float(trip_profile['time'])
		torq_trip['filename'] = p_filename
		torq_trip['csvfilename'] = filename
		torq_trip['csvhash'] = md5(open(filename, 'rb').read()).hexdigest()
		torq_trip['tripdate'] = tripdate
		torq_trip['profile'] = trip_profile['profile']
		return torq_trip
	else:
		logger.warning(f'[p] {filename} len={len(pdata_)}')


def database_init(engine, dburl, filelist):
	sess = sessionmaker(bind=engine)
	session = sess()
	tables = ['torqtrips', 'torqlogs', 'torqfiles', 'torqdata']
	for t in tables:
		logger.info(f'[dbprep] drop {t}')
		if engine.name == 'mysql':
			try:
				session.execute('SET FOREIGN_KEY_CHECKS=0;')
				session.commit()
			except (InternalError, ProgrammingError) as e:
				logger.error(f'[dbinit] {e}')
				session.rollback()
		sqldrop = f'DROP TABLE IF EXISTS {t} CASCADE;'
		session.execute(sqldrop)
		session.commit()
		if engine.name == 'mysql':
			try:
				session.execute('SET FOREIGN_KEY_CHECKS=1;')
				session.commit()
			except (InternalError, ProgrammingError) as e:
				logger.error(f'[dbinit] {e}')
				session.rollback()
	create_cmd = sqlcmds[engine.name]
	for c in create_cmd:
		session.execute(sqlcmds[engine.name][c])
		session.commit()
	logger.info(f'[dbprep] create done')
	for tf in filelist:
		csvfile = tf['csvfilename']
		csvhash = tf['csvhash']
		csvfilefixed = tf['csvfilefixed']
		fixedhash = md5(open(csvfilefixed, 'rb').read()).hexdigest()
		sql = f"insert into torqfiles (csvfilename, csvhash, csvfilefixed, fixedhash) values ('{csvfile}','{csvhash}','{csvfilefixed}','{fixedhash}');"
		# logger.debug(sql)
		try:
			session.execute(sql)
			session.commit()
		except (OperationalError, ArgumentError) as e:
			logger.error(f'[i] {e}')
			session.rollback()


def prepdb(filelist=None, engine=None, args=None, dburl=None, Base=None):
	if args.combinecsv:
		return filelist
	logger.info(f'[prepdb] files:{len(filelist)}')
	tables = ['torqtrips', 'torqlogs', 'torqfiles']
	Session = sessionmaker(bind=engine)
	session = Session()
	if args.init_db:
		database_init(engine, dburl, filelist)
		return filelist
	else:
		sql = f'select * from torqfiles;'
		torqdbfiles = session.execute(sql).fetchall()
		hlist = [k['csvhash'] for k in torqdbfiles]
		for tf in filelist:
			csvfile = tf['csvfilename']
			csvhash = tf['csvhash']
			csvfilefixed = tf['csvfilefixed']
			fixedhash = md5(open(csvfilefixed, 'rb').read()).hexdigest()
			# fixedhash = tf['fixedhash']
			if csvhash not in [k['csvhash'] for k in torqdbfiles]:
				sql = f'insert into torqfiles (csvfilename, csvhash, csvfilefixed, fixedhash) values ("{csvfile}","{csvhash}","{csvfilefixed}","{fixedhash}");'
				try:
					session.execute(sql)
					session.commit()
				except ProgrammingError as e:
					logger.error(f'[prepdb] {e}')
					session.rollback()
			# logger.debug(sql)
			else:
				logger.warning(f'[prepdb] {csvfile} already in db')
		newfiles = [k for k in filelist if k['csvhash'] not in hlist]
		return newfiles
