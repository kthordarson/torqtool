#!/usr/bin/python3

# fixers in here

import os
import shutil
from hashlib import md5
from pathlib import Path
import random
import pandas as pd
import polars as pl
from loguru import logger
from sqlalchemy.exc import NoResultFound

from datamodels import TorqFile
from utils import (
    get_engine_session,
    get_fixed_lines,
    get_sanatized_column_names,
    convert_string_to_datetime,
)


def replace_headers(newfiles: list, args):
    """
    newfiles a list of new files we need to process / send
    strip leading spaces off the column headers
    returns dict with two list of files, successfully processed files, and error files
    """

    res = {
        "files_to_read": [],
        "errorfiles": [],
    }
    for f in newfiles:
        if fix_column_names(f, args):
            res["files_to_read"].append(f)
        else:
            res["errorfiles"].append(f)
    if len(res["errorfiles"]) > 0:
        logger.warning(f"errors: {len(res['errorfiles'])} {res['errorfiles']}")
    else:
        logger.info(f'replace_headers fixed {len(res["files_to_read"])} / {len(res["errorfiles"])} / {len(newfiles)} files')
    return res


def fix_column_names(csvfile: str, args):
    """
    strip leading spaces from column names and saves the fil
    # todo skip files that have been fixed already, by checking in the database
    """
    # subchars = [', ',',Â','∞','Â°F','Â°','â°','â']
    try:
        with open(csvfile, "r") as f:
            rawdata = f.readlines()
        # for badchar in subchars:
        rawdata[0] = get_sanatized_column_names(rawdata[0])  # re.sub(badchar,',',rawdata[0])
        # rawdata[0] = re.sub(', ',',',rawdata[0])
        # rawdata[0] = re.sub('Â','',rawdata[0])
        # rawdata[0] = re.sub('∞','',rawdata[0])
        # rawdata[0] = re.sub('Â°F','F',rawdata[0])
        # rawdata[0] = re.sub('Â°','',rawdata[0])
        # rawdata[0] = re.sub('â°','',rawdata[0])
        # rawdata[0] = re.sub('â','',rawdata[0])
        if not args.skipwrites:
            bf = f"{csvfile}.colfixbak"
            bakfile = Path(args.bakpath).joinpath(Path(bf).parts[-1])
            if not os.path.exists(bakfile):
                shutil.copy(csvfile, bakfile)
                logger.info(f"writing columns to {csvfile}")
                with open(csvfile, "w") as f:
                    f.writelines(rawdata)
            else:
                if args.extradebug:
                    pass  # logger.warning(f"backup file exists {bakfile} skipping write {csvfile}")
        else:
            pass  # logger.info(f'skipping write {csvfile}')
    except Exception as e:
        logger.error(f"{type(e)} {e} in {csvfile}")
        return False
    finally:
        return True


def check_and_fix_logs(logfiles, args):
    # iterate all log files (that have not been fixed) , check for bad chars, remove them
    # skip files that have been fixed already, by checking in the database
    # new_log_files = []
    # dburl = 'sqlite:///torqfiskur.db'
    # engine = create_engine(dburl, echo=False, connect_args={'check_same_thread': False})
    # Session = sessionmaker(bind=engine)
    # session = Session()
    engine, session = get_engine_session(args)
    for log in logfiles:
        # check if log file has been fixed already, if not fix it
        pass


def drop_bad_columns(logfile: str, savebackup=True):
    # drop  columns with funny names from log file
    # saves to new csv file
    df = pd.read_csv(logfile, nrows=1)
    needbackup = False
    for c in df.columns:
        if len(c) == 0 or c[0].isnumeric():
            logger.warning(f"invalid/empty column {c} in {logfile} dropping...")
            df = df.drop(columns=c)
            needbackup = True
    if needbackup and savebackup:
        bakname = Path(f"{logfile}.bak")
        shutil.copy(logfile, bakname)
        logger.info(f"backed up {logfile} to {bakname}")
        df.to_csv(logfile, index=False)


def drop_empty_columns(logfile: str, savebackup=True):
    # drop empty columns from log file
    # saves to new csv file
    df = pl.read_csv(
        logfile, ignore_errors=True, try_parse_dates=True, truncate_ragged_lines=True
    ).to_pandas()
    needbackup = False
    for c in df.columns:
        lchk = len([k for k in df[c] if k == "-"])
        logger.debug(f"checking {c} in {logfile} l:{lchk} len:{len(df[c])}")
        if lchk == len(df[c]):
            logger.warning(f"column {c} full of - values in {logfile} dropping...")
            df = df.drop(columns=c)
            needbackup = True
    if needbackup and savebackup:
        bakname = Path(f"{logfile}.bak")
        shutil.copy(logfile, bakname)
        logger.info(f"backed up {logfile} to {bakname}")
        df.to_csv(logfile, index=False)
    else:
        logger.info(f"no empty columns in {logfile}")


def get_cols(logpath: str, extglob: str = "**/*.csv", debug=False):
    """
    collect all columns from all csv files in the path
    params: logpath where to search, extglob glob pattern to use
    prep data base columns ....
    """
    columns = {}
    stats = {}
    filestats = {}
    # '/home/kth/development/torq/torqueLogs.bakbak/').glob("**/trackLog-*.bak"
    for logfile in Path(logpath).glob(extglob):
        df = pd.read_csv(logfile, nrows=1)
        # newcolnames = ','.join([re.sub(r'\W', '', col) for col in df.columns]).encode('ascii', 'ignore').decode()
        newcolnames = get_sanatized_column_names(df.columns)
        fs = {
            "filename": logfile.name,
            "newcolnames": newcolnames,
        }
        filestats[logfile.name] = fs
        for c in newcolnames.split(","):
            c = c.lower()  # everything lowercase
            if len(c) == 0 or c[0].isnumeric():
                logger.warning(f"invalid/empty column {c} in {logfile}")
                continue
            if c not in columns:
                info = {"count": 1, "files": [logfile.name]}
                stats[c] = info
                columns[c] = {"count": 1}
                if debug:
                    logger.debug(f"col: {c} {len(columns)} ")
            else:
                columns[c]["count"] += 1
                stats[c]["count"] += 1
                stats[c]["files"].append(logfile.name)
    if debug:
        total_cols = 0
        total_chars = 0
        for f in filestats:
            colcount = len(filestats[f].get("newcolnames").split(","))
            charcnt = len(filestats[f].get("newcolnames"))
            total_cols += colcount
            total_chars += charcnt
            # print(f'{f} {colcount} {charcnt}')
        print(f'{"="*25}')
        print(
            f"avg cols: {total_cols/len(filestats)} avg chars: {total_chars/len(filestats)}"
        )
        for s in stats:
            scnt = stats[s]["count"]
            print(f"{s} {scnt}")
            if scnt == 1:
                for sf in stats[s]["files"]:
                    print(f"\t - {sf}")
    print(f'{"="*25}')
    return stats, columns


def run_fixer(args):
    logger.debug(f"searching {args.logpath} for csv files")
    csvfiles = [k for k in Path(args.logpath).glob("**/trackLog-*.csv")]
    logger.debug(f"found {len(csvfiles)} csv files")
    for f in csvfiles:
        bakname = Path(os.path.join(args.bakpath, Path(f).name))
        if bakname.exists():
            pass  # logger.warning(f"backup file {bakname} exists, skipping")
            continue
        else:
            csvlines = open(f, "r").readlines()
            try:
                fixedlines = get_fixed_lines(f, debug=args.debug)
            except Exception as e:
                logger.error(f"error {type(e)} {e} {f}")
                continue
            shutil.copy(f, bakname)
            logger.debug(f"{f} {len(csvlines)} got {len(fixedlines)}  ")
            with open(f, "w") as f:
                f.writelines(fixedlines)


def new_columns_collector(logdir: str):
    """
    collect column names from all log files, sanitize names
    returns dict with old names mapped to new names
    """
    errors = 0
    readfiles = 0
    files_with_errors = []
    all_columns = []
    filecount = len([k for k in Path(logdir).glob("*.csv")])
    x = filecount // 10
    for idx, k in enumerate(Path(logdir).glob("*.csv")):
        if idx % x == 0:  # progress indicator
            logger.info(f"[{idx}/{filecount}] rf={readfiles} e:{errors} ac: {len(all_columns)}")
        try:
            # only read first line of csv file and select columns
            columns = pl.read_csv(
                k,
                ignore_errors=True,
                try_parse_dates=True,
                truncate_ragged_lines=True,
                n_rows=1,
            ).columns
            # newcolnames = ','.join([re.sub(r'\W', '', col) for col in columns]).encode('ascii', 'ignore').decode().lower().split(',')
            newcolnames = get_sanatized_column_names(columns)
            all_columns.extend(
                [
                    k
                    for k in newcolnames.split(",")
                    if k not in all_columns and k[0].isalpha()
                ]
            )
            readfiles += 1
        except Exception as e:
            print(f"[{idx}/{filecount}] {type(e)} {e} {errors} in {k}")
            errors += 1
            files_with_errors.append(k)
    if errors > 0:
        print(f"plErrors: {files_with_errors}")
    # r = dict([k for k in zip(columns, newcolnames)])
    nclist = [k.strip() for k in newcolnames.split(",")]
    r = dict([k for k in zip(columns, nclist)])
    # foo = dict( zip(columns, newcolnames))
    return r, files_with_errors


def get_raw_columns(logfile: str):
    """
    get the raw header from a csv logfile
    returns dict with logfilename and info
    """
    # coldata = sorted(coldata, key=lambda x: x['colcount'])
    with open(logfile, "r") as f:
        rawh = f.readline()
    return {"logfile": logfile, "header": rawh, "colcount": len(rawh.split(","))}


def get_files_with_errors(logdir: str):
    """
    scan logdir for csv files that have problems
    returns dict with old names mapped to new names
    """

    # count length of each column in raw csv
    # _=[print(f'k:{k} len:{len(k)} at : {idx}') for idx,k in enumerate(rawdata[1].split(','))]

    errors = 0
    readfiles = 0
    files_with_errors = []
    all_columns = []
    filecount = len([k for k in Path(logdir).glob("*.csv")])
    x = filecount // 10
    test_read = None
    for idx, k in enumerate(Path(logdir).glob("*.csv")):
        if idx % x == 0:
            logger.info(
                f"[{idx}/{filecount}] rf={readfiles} e:{errors} ac: {len(all_columns)}"
            )
        try:
            test_read = pl.read_csv(k, try_parse_dates=True, ignore_errors=True)
            if test_read:
                readfiles += 1
        except Exception as e:
            print(f"[{idx}/{filecount}] {type(e)} {e} {errors} in {k}")
            errors += 1
            files_with_errors.append(k)
    if errors > 0:
        logger.warning(f"found {files_with_errors} problem files")
    else:
        logger.info("no problem files found")
    # foo = dict( zip(columns, newcolnames))
    return files_with_errors


def split_file(logfile: str, session=None):
    """
    split a log file where multiple lines of column headers are present
    param: logfile = full path and name of file
    """
    with open(logfile, "r") as f:
        rawdata = f.readlines()
    rawdatalen0 = len(rawdata)
    # strip invalid lines that start with '-'
    rawdata = [k for k in rawdata if not k.startswith("-")]
    rawdatalen1 = len(rawdata)
    # find all lines with gps in them, skip first line
    split_list = [{"linenumber": idx, "linedata": k} for idx, k in enumerate(rawdata) if "gps" in k.lower()][1:]

    # grab timestamps of lines before and after split to determine if split is needed or not
    # if timestamps are close, no split is needed, combine file and rescan else split into multiple files
    if len(split_list) == 0:
        logger.warning(f"no split markers found in {logfile} should not happen!")
        return
    logger.info(f"found {len(split_list)} split markers in {logfile} {rawdatalen0=} {rawdatalen1=}")
    gpstime_diff = 0
    devicetime_diff = 0
    line_offset = 1
    try:
        for idx, marker in enumerate(split_list):
            if marker.get("linenumber") == 1:
                logger.warning(f"invalid {marker=} {logfile=}\n{split_list=}")
                break

            rawgpstime_before_split = rawdata[marker.get("linenumber") - line_offset].split(",")[0]
            if rawgpstime_before_split == "-":
                line_offset = random.randint(1, 5)
                rawgpstime_before_split = rawdata[marker.get("linenumber") - line_offset].split(",")[0]

            rawgpstime_after_split = rawdata[marker.get("linenumber") + line_offset].split(",")[0]
            if rawgpstime_after_split == "-":
                line_offset = random.randint(1, 5)
                rawgpstime_after_split = rawdata[marker.get("linenumber") + line_offset].split(",")[0]
            if rawgpstime_after_split == "-" or rawgpstime_before_split == "-":
                logger.warning(f"invalid {marker=} {logfile=}\n{split_list=}")
                break
            try:
                gpstime_before_split = convert_string_to_datetime(rawgpstime_before_split)
                gpstime_after_split = convert_string_to_datetime(rawgpstime_after_split)
                devicetime_before_split = convert_string_to_datetime(rawdata[marker.get("linenumber") - line_offset].split(",")[1])
                devicetime_after_split = convert_string_to_datetime(rawdata[marker.get("linenumber") + line_offset].split(",")[1])

                gpstime_diff += (gpstime_after_split - gpstime_before_split).seconds
                devicetime_diff += (devicetime_after_split - devicetime_before_split).seconds
            except (IndexError, TypeError) as e:
                raw1 = rawdata[marker.get("linenumber") - line_offset]
                logger.error(f"{e} failed to convert date {logfile} {idx=} \n{marker=}\n{raw1=}")
                raise e

        if gpstime_diff <= 900:  # 900=15minutes, combine file parts into one
            logger.info(f"Remove extra header lines from {logfile} at line  {gpstime_diff=} {devicetime_diff=} ")
            # remove lines that start with GPS, keep first line, write to file (overwrite)
            skip_lines = [k.get("linenumber") for k in split_list]
            shutil.copy(logfile, f"{logfile}.splitbak")
            with open(logfile, "w") as f:
                for idx, line in enumerate(rawdata):
                    if idx not in skip_lines:
                        f.write(line.strip() + "\n")
            logger.debug(f"wrote fixed {logfile}")
            if session:
                try:
                    torqfile = (session.query(TorqFile).filter(TorqFile.csvfile == logfile).one())
                except NoResultFound as e:
                    torqfile = TorqFile(csvfile=logfile, csvhash=md5(open(logfile, "rb").read()).hexdigest(), )
                    logger.warning(f"{e} creating new torqfile: {torqfile}")
                session.add(torqfile)
                session.commit()
                logger.debug(f"database updated for {logfile} torqfileid: {torqfile.fileid} tf={torqfile}")
            return logfile
        elif gpstime_diff >= 900:  # more that five seconds, split file
            # newbufferlen = len(rawdata[marker['linenumber']:])
            logger.warning(
                f"Splitting {logfile} {gpstime_diff=} {devicetime_diff=} {split_list}"
            )  # \n\tgpsbefore: {gpstime_before_split} gpsafter: {gpstime_after_split}\n\tdevtimebefore: {devicetime_before_split} devtimeafter: {devicetime_after_split}' )
            marker = split_list[0]
            # generate new filename for split
            rawdate = rawdata[marker.get("linenumber") + 1].split(",")[1]
            try:
                newdate = convert_string_to_datetime(rawdate)
                basename = ("trackLog-split-" + newdate.strftime("%Y-%b-%d_%H-%M") + ".csv")
            except TypeError as e:
                logger.error(f"{e} failed to convert date {logfile} {rawdate=}")
                raise e  # return None # basename = 'trackLog-split-' + datetime.now().strftime('%Y-%b-%d_%H-%M') + '.csv'
            newlogfile = os.path.join(Path(logfile).parent, basename)
            start_line = [k.get("linenumber") for k in split_list][0]
            new_rawdata = rawdata[start_line:]
            with open(newlogfile, "w") as f:
                for idx, line in enumerate(new_rawdata):
                    f.write(line.strip() + "\n")
            spname = f"{logfile}.baksplit"
            shutil.move(logfile, spname)
            logger.debug(f"wrote fixed {basename} old: {spname} new: {newlogfile}")
            if session:
                torqfile = TorqFile(
                    csvfile=newlogfile,
                    csvhash=md5(open(newlogfile, "rb").read()).hexdigest(),
                )
                session.add(torqfile)
                session.commit()
                logger.debug(
                    f"database updated for {basename} torqfileid: {torqfile.fileid}"
                )
            return newlogfile

    except TypeError as e:
        logger.error(f"splitter failed {type(e)} {e} {logfile=}")
        raise e
    except Exception as e:
        logger.error(f"unhandled Exception splitter failed {type(e)} {e} {logfile=}")
        raise e


if __name__ == "__main__":
    pass
