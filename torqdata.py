from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from torqcols import allcols


def _normalize_col_name(value: str) -> str:
	return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _get_torqlogs_columns(session) -> list[str]:
	rows = session.execute(text("PRAGMA table_info(torqlogs)")).all()
	return [row[1] for row in rows]

def get_trip_data(trip, session):
	resdata = []
	actual_columns = _get_torqlogs_columns(session)
	normalized_actual = {_normalize_col_name(col): col for col in actual_columns}

	for c in allcols:
		actual_col = normalized_actual.get(_normalize_col_name(c))
		if not actual_col:
			continue
		res = None
		try:
			# res = pd.read_sql(f'SELECT fileid, MIN("{actual_col}") as min_{c}, MAX("{actual_col}") as max_{c}, AVG("{actual_col}") as avg_{c} FROM torqlogs WHERE fileid = "{trip}"', engine)
			res = session.execute(text(f'SELECT fileid, MIN("{actual_col}") as min_{c}, MAX("{actual_col}") as max_{c}, AVG("{actual_col}") as avg_{c} FROM torqlogs WHERE fileid = "{trip}"')).fetchall()
			resdata.append(res)
		except OperationalError as e:
			if e.code != 'e3q8':
				logger.warning(f'[err] col={c} mapped={actual_col} code={e} {e.statement}')
				res = None
	logger.info(f'[trip] id:{trip} len={len(resdata)}')
	return resdata

if __name__ == '__main__':
	TORQDBHOST = 'elitedesk'
	TORQDBUSER = 'torq'
	TORQDBPASS = 'dzt3f5jCvMlbUvRG'

	dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/torq?charset=utf8mb4"
	engine = create_engine(dburl)
	logger.info(f'[engine] {engine}')
	Session = sessionmaker(bind=engine)
	session = Session()

	max_results = 3
	toptrips = None
	topdata = []
	try:
		# toptrips=session.query(Torqtrips.id, Torqtrips.distance).order_by(Torqtrips.distance.desc()).limit(10).all()
		toptrips = session.execute(text(f'select id from torqtrips order by distance desc limit {max_results}')).fetchall()
		# toptrips = pd.read_sql(f'select id from torqtrips order by distance desc limit {max_results}', session)
		for trip in toptrips:
			td = get_trip_data(trip[0], session)
			topdata.append(td)
			logger.info(f'[td] trip={trip[0]} (len={len(td)} type={type(td)}) toptrips={len(toptrips)} {type(toptrips)} td={len(topdata)}')
	except OperationalError as e:
		logger.error(f'[e] code={e} args={e.args[0]} {type(toptrips)}')
	for t in topdata:
		for c in t:
			logger.info(f'[topdata] {c} ')  # {c.tripid} {c.name} {c.values}')
