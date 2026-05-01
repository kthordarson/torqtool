from loguru import logger
from sqlalchemy import text, inspect
from sqlalchemy.exc import OperationalError
from converter import get_args
from utils import get_engine_session
from torqcols import allcols


def _normalize_col_name(value: str) -> str:
	return "".join(ch.lower() for ch in str(value) if ch.isalnum())


def _get_torqlogs_columns(session) -> list[str]:
	cache_key = "torqlogs_columns"
	if cache_key not in session.info:
		inspector = inspect(session.get_bind())
		session.info[cache_key] = [str(col["name"]) for col in inspector.get_columns("torqlogs")]
	return session.info[cache_key]


def _build_trip_data_query(actual_columns_by_requested: list[tuple[str, str]]):
	select_parts = [":trip as fileid"]
	for requested_col, actual_col in actual_columns_by_requested:
		select_parts.append(f'MIN("{actual_col}") as min_{requested_col}')
		select_parts.append(f'MAX("{actual_col}") as max_{requested_col}')
		select_parts.append(f'AVG("{actual_col}") as avg_{requested_col}')
	query = "SELECT " + ", ".join(select_parts) + " FROM torqlogs WHERE fileid = :trip"
	return text(query)

def get_trip_data(trip, session):
	resdata = {'trip': trip, 'data': []}
	actual_columns = _get_torqlogs_columns(session)
	normalized_actual = {_normalize_col_name(col): col for col in actual_columns}
	requested_columns = []
	logger.info(f'[get_trip_data] trip={trip} actual_columns={len(actual_columns)} normalized_actual={len(normalized_actual)}')
	for c in allcols:
		actual_col = normalized_actual.get(_normalize_col_name(c))
		if not actual_col:
			continue
		requested_columns.append((c, actual_col))

	if not requested_columns:
		logger.info(f'[trip] id:{trip} len=0')
		return resdata

	try:
		row = session.execute(_build_trip_data_query(requested_columns), {"trip": trip}).one()
		row_data = row._mapping
		for c, actual_col in requested_columns:
			res = [(row_data["fileid"], row_data[f"min_{c}"], row_data[f"max_{c}"], row_data[f"avg_{c}"])]
			resdata['data'].append({'col': c, 'actual_col': actual_col, 'result': res})
			logger.debug(f'[get_trip_data] trip={trip} col={c} actual_col={actual_col} res={res} resdata={len(resdata["data"])}')
	except OperationalError as e:
		if e.code != 'e3q8':
			logger.warning(f'[err] trip={trip} code={e} {e.statement}')
	logger.info(f'[trip] id:{trip} len={len(resdata["data"])}')
	return resdata

if __name__ == '__main__':
	# TORQDBHOST = 'elitedesk'
	# TORQDBUSER = 'torq'
	# TORQDBPASS = 'dzt3f5jCvMlbUvRG'

	# dburl = f"mysql+pymysql://{TORQDBUSER}:{TORQDBPASS}@{TORQDBHOST}/torq?charset=utf8mb4"
	args = get_args('torqdata')
	session = get_engine_session(args)
	logger.info(f'[s] {session.get_bind()}')

	max_results = 3
	toptrips = None
	topdata = []
	try:
		# toptrips=session.query(Torqtrips.id, Torqtrips.distance).order_by(Torqtrips.distance.desc()).limit(10).all()
		toptrips = session.execute(text(f'select id from torqtrips order by distance desc limit {max_results}')).fetchall()
		logger.debug(f'[toptrips] {len(toptrips)}')
		# toptrips = pd.read_sql(f'select id from torqtrips order by distance desc limit {max_results}', session)
		for trip in toptrips:
			td = get_trip_data(trip[0], session)
			topdata.append(td)
			logger.info(f'[td] trip={trip[0]} (len={len(td)} type={type(td)}) toptrips={len(toptrips)} {type(toptrips)} td={len(topdata)}')
	except OperationalError as e:
		logger.error(f'[e] code={e} args={e.args[0]} {type(toptrips)}')
	print(f'data: {topdata}')
