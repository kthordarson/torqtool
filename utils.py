# utils and db things here
# grep -rl 612508207723425200000000000000000000000 | xargs -i@ sed -i 's/612508207723425200000000000000000000000/0/g' @
# grep -rl ∞ | xargs -i@ sed -i 's/∞/0/g' @
# grep -rl ∞ | xargs -i@ sed -i 's/∞/0/g' @
# grep -r -P -n "[^\x00-\x7F]"
# sed 's/\xc2\x91\|\xc2\x92\|\xc2\xa0\|\xe2\x80\x8e/0/g'
# ��
# kth@fiskur:~/development/torqlogs$ echo -n "�" | od -An -tx1 - | sed 's/ /\\x/g'
# \xef\xbf\xbd
# kth@fiskur:~/development/torqlogs$ echo -n "�" | iconv -f utf8 -t utf16 - | od -An -tx2 | sed 's/.*/\U&/; s/^ FEFF//; s/ /\\u/g'
# \uFFFD
# pcregrep --color='auto' -n "[^[:ascii:]]" /home/kth/development/torqlogs/1625496025126/trackLog.csv
# 1625496025126 <88><9e> 0A C2 88 C2 9E
# perl -i.bak -pe 's/[^[:ascii:]]//g' asdf1.csv
import os
import re
from datetime import datetime
from hashlib import md5
from pathlib import Path
from datamap import entry_datamap

from loguru import logger
from pandas import DataFrame, Series, concat, to_datetime
from sqlalchemy import Column, MetaData, String, create_engine, select
from sqlalchemy.exc import (DataError, IntegrityError, OperationalError,
                            ProgrammingError)
from sqlalchemy.ext.declarative import declarative_base

MIN_FILESIZE = 2500


def checkcsv(searchpath: Path):
	# In [97]: krem=[[b for b in badvals if b in bl] for bl in data]
	badvals = ['-', 'NaN', '0', 'â', r'0']
	# ∞
	#  '\\xe2\\x88\\x9e'
	if not isinstance(searchpath, Path):
		searchpath = Path(searchpath)
	csvfiles = [k for k in searchpath.glob("**/trackLog.csv")]
	result = {}
	total_errors = 0
	csverrs = 0
	for cidx, csv in enumerate(csvfiles):
		# 612508207723425200000000000000000000000
		result[cidx] = {'csvfile': str(csv), 'errs': csverrs, 'lines': [], 'linenumbers': [], 'line': []}
		# result[cidx]['csvfilename'] = csv
		# result[cidx]['errcount'] = 0
		with open(csv, 'r') as f:
			csvdata = f.readlines()
		if len(csvdata) > 2:
			minlen = min([len(k) for k in csvdata[1:]])
			maxlen = max([len(k) for k in csvdata[1:]])
			avglen = sum([len(k) for k in csvdata[1:]]) / len(csvdata[1:])
			# logger.debug(f'[csv] {csv} min:{minlen} max:{maxlen} avg:{avglen} diff: {maxlen-avglen}')
			for idx, line in enumerate(csvdata[1:]):
				try:
					nl = line.encode('ascii')
				except UnicodeEncodeError as e:
					logger.warning(f'[uerr] {csv} idx:{idx}  {e} line:{line}')
				if len(line) > maxlen - 1:
					# result[cidx]['errcount'] += 1
					total_errors += 1
					# res = {'csvfile':str(csv), 'linenumber':idx, 'line':line}
					result[cidx]['line'] = line
					result[cidx]['errs'] += 1
					result[cidx]['linenumbers'].append(idx)
					csverrs += 1
	logger.info(f'[chk] total_errors {total_errors} res:{len(result)}')
	# resjson = json.dumps(result)
	# f = open('errdump.json', 'w')
	# f.write(resjson)
	# f.close()
	return result


def replace_all(text, dic):
	for i, j in dic.items():
		text = text.replace(i, j)
	return text

def fix_csv_file(tf):
	# read csv file, replace badvals and fix column names
	# returns a buff with the fixed csv file
	with open(tf['csvfilename'], 'r') as reader:
		try:
			data = reader.readlines()
		except UnicodeDecodeError as e:
			logger.error(f'[fix] {e} {tf["csvfilename"]}')
			return None
	badvals = {
		#'-': '0',
		"'-'": '0',
		'"-"': '0',
		',-,': ',0,',
		'∞': '0',
		#',-' : ',0',
		'â': '0',
		'₂': '',
		'°': '',
		'Â°': '0',
		'Â': '0',
		'612508207723425200000000000000000000000': '0',
		'340282346638528860000000000000000000000': '0',
		'-3402823618710077500000000000000000000': '0'}
	lines0 = [k for k in data if not k.startswith('-')]
	lines = [replace_all(b, badvals) for b in lines0]
	orgcol = data[0].split(',')
	newcolname = ','.join([re.sub(r'\W', '', col) for col in orgcol]).encode('ascii', 'ignore').decode()
	newcolname += '\n'
	newcolname = newcolname.lower()
	if 'co0ingkmaveragegkm' in newcolname:
		# logger.warning(f'co0ingkmaveragegkm in {tf["csvfilename"]}')
		newcolname = newcolname.replace('co0ingkmaveragegkm', 'coingkmaveragegkm')
	if 'co0ingkminstantaneousgkm' in newcolname:
		# logger.warning(f'co0ingkminstantaneousgkm in {tf["csvfilename"]}')
		newcolname = newcolname.replace('co0ingkminstantaneousgkm', 'coingkminstantaneousgkm')
	lines[0] = newcolname
	return lines

def save_fixed_csv(lines, nf):
	if not os.path.exists(nf):
		if lines:
			logger.debug(f"[f] saving fixed csv file {nf} lines:{len(lines)}")
			with open(file=nf, mode='w', encoding='utf-8', newline='') as writer:
				writer.writelines(lines)
		else:
			logger.warning(f'[s] no lines to save {nf}')
	else:
		logger.warning(f'[f] {nf} exists...')

def get_csv_files(searchpath: Path,  dbmode=None):
	# scan searchpath for csv files
	torqcsvfiles = [({
		'csvfilename': k, # original csv file
		'csvfilefixed': f'{k}.fixed.csv', # fixed csv file
		'size': os.stat(k).st_size,
		'dbmode': dbmode}) for k in searchpath.glob("**/trackLog.csv") if k.stat().st_size >= MIN_FILESIZE] # and not os.path.exists(f'{k}.fixed.csv')]
	logger.info(f'[getcsv] sending {len(torqcsvfiles)} files to fixer')
	for idx, tf in enumerate(torqcsvfiles):
		if os.path.exists(tf['csvfilefixed']):
			# logger.warning(f'[g] {tf["csvfilefixed"]} exists')
			with open(tf['csvfilefixed'], 'r') as reader:
				fixedlines = reader.readlines()
		else:
			fixedlines = fix_csv_file(tf)
			save_fixed_csv(fixedlines, tf['csvfilefixed'])
		csvhash = md5(open(torqcsvfiles[idx]['csvfilename'], 'rb').read()).hexdigest()
		fixedhash = md5(open(torqcsvfiles[idx]['csvfilefixed'], 'rb').read()).hexdigest()
		torqcsvfiles[idx] = {
			'csvfilename': tf['csvfilename'],
			'csvfilefixed' : tf['csvfilefixed'],
			'csvhash': csvhash,
			'fixedhash': fixedhash,
			'csvtimestamp': f'{datetime.fromtimestamp(int(tf["csvfilename"].parts[-2][0:10]))}',
			'dbmode': dbmode,
		}
	return torqcsvfiles


def size_format(b):
	if b < 1000:
		return '%i' % b + 'B'
	elif 1000 <= b < 1000000:
		return '%.1f' % float(b / 1000) + 'KB'
	elif 1000000 <= b < 1000000000:
		return '%.1f' % float(b / 1000000) + 'MB'
	elif 1000000000 <= b < 1000000000000:
		return '%.1f' % float(b / 1000000000) + 'GB'
	elif 1000000000000 <= b:
		return '%.1f' % float(b / 1000000000000) + 'TB'


def xfix_csv_file(csvfile: str, replace_vals: dict, overwrite=False):
	with open(csvfile, 'r') as reader:
		data = reader.read()
	fixed = replace_all(data, replace_vals)
	outfilename = str(csvfile).replace('.csv', '.fixed.csv')
	with open(outfilename, 'w') as writer:
		writer.write(fixed)


def get_bad_vals(csvfile: str):
	with open(csvfile, 'r') as reader:
		data = reader.readlines()
	for line in data:
		l0 = line.split(',')
		for lx in l0:
			try:
				l1 = lx.encode('ascii')
			except (UnicodeEncodeError, UnicodeDecodeError) as e:
				logger.error(f'unicodeerr: {e} in {csvfile} lt={type(line)} l={line}')
			except AttributeError as e:
				logger.error(f'AttributeError: {e} in {csvfile} lt={type(line)} l={line}')

def fix_nulls(engine):
	read_sql = None
	for k in entry_datamap:
		df = read_sql(f'select {k} from torqlogs where {k} is null', engine)
		if len(df) >= 1:
			sqlcmd = f'update torqlogs set {k} = 0 where {k} IS NULL;'
			engine.execute(sqlcmd)
			# df0=read_sql(f'select {k} from torqlogs where {k} is null', engine)
			logger.debug(f'fixnulls k:{k} l:{len(k)}')

def convert_datetime(val):
	newval = 0
	if val == '-':
		logger.warning(f'[cd] v:{val} ')
		return to_datetime('2000-01-01', errors='raise').to_numpy()
	try:
		newval = to_datetime(val, errors='raise', format='mixed').to_numpy() # , infer_datetime_format=False
	except AttributeError as e:
		newval = val.strip()[2:]
		logger.warning(f'[cd] {e} v:{val} n:{newval}')
		newval = to_datetime(newval)
	return newval

def get_engine(args):
	if args == 'mysql':
		dburl = f"mysql+pymysql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}?charset=utf8mb4"
	# return create_engine(dburl, pool_size=200, max_overflow=0)
	if args == 'postgresql':
		dburl = f"postgresql://{args.dbuser}:{args.dbpass}@{args.dbhost}/{args.dbname}"
	if args == 'sqlite':
		dburl = f'sqlite:///torqfiskurdb'
	else:
		dburl = 'none'
	return create_engine(dburl)

def chunks(l, n):
	for i in range(0, len(l), n):
		yield l.iloc[i:i + n]
