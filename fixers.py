#!/usr/bin/python3
import os
import shutil
from hashlib import md5
from pathlib import Path
import random
from loguru import logger
from sqlalchemy.exc import NoResultFound
from datamodels import TorqFile
from utils import convert_string_to_datetime

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
