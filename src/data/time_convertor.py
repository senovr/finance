import datetime
import time


def utc2date(UTC):
    date1 = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(UTC))
    return date1


def date2utc(date):
    convert = datetime.datetime.strptime(date, "%Y:%m:%d %H:%M:%S").timetuple()
    utc = time.mktime(convert)
    return utc


def date2seconds(date):
    try:
        convert = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S").timetuple()
        seconds = time.mktime(convert)
    except Exception:
        seconds = 12345678900
    return seconds


def seconds2date(seconds):
    date = datetime.fromtimestamp(seconds).strftime("%A, %B %d, %Y %I:%M:%S")
    # 'Sunday, January 29, 2017 08:30:00'
    return date


def TimeStamp2seconds(TimeStamp):
    seconds = TimeStamp.dt.total_seconds()
    return seconds


##-------------------------------------------------------------------------------------------------
# https://www.geeksforgeeks.org/python-program-to-convert-seconds-into-hours-minutes-and-seconds/
##-------------------------------------------------------------------------------------------------
def convert_via_naive(seconds):
    """ Convert seconds into hours, minutes and seconds (naive algorithm).

    Parameters
    ----------
    seconds : int (float?)

    Returns
    -------
    string

    n = 12345
    print(convert(n))
    >> 3:25:45
    """
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    time_string = "%d:%02d:%02d" % (hour, minutes, seconds)
    return time_string


##-------------------------------------------------------------------------------------------------
def convert_via_naive_divmod(seconds):

    min, sec = divmod(seconds, 60)
    hour, min = divmod(min, 60)

    time_string = "%d:%02d:%02d" % (hour, min, sec)
    return time_string


##-------------------------------------------------------------------------------------------------
def convert_via_datetime(n):
    time_string = str(datetime.timedelta(seconds=n))
    return time_string


##-------------------------------------------------------------------------------------------------
def convert_via_time(seconds):
    time_string = time.strftime("%H:%M:%S", time.gmtime(seconds))
    return time_string
