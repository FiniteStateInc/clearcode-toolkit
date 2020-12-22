import datetime
import time

from dateutil.tz import tzutc


def utcnow():  # type: () -> datetime.datetime
    """
    Returns a UTC now timestamp with an unambiguous time zone.
    See https://docs.python.org/3/library/datetime.html#datetime.datetime.utcnow
    :rtype: datetime.datetime
    """
    return datetime.datetime.now(datetime.timezone.utc)


def as_utc(dt):  # type: (datetime.datetime) -> datetime.datetime
    """
    Converts a datetime to UTC.  If already in UTC, returns it unmodified.
    :param dt: A datetime.  It must have an unambiguous (non-None) tz value.
    :return: The datetime in UTC
    """
    tz = dt.tzinfo

    if not tz:
        raise ValueError('The timestamp has an ambiguous time zone')

    if tz in [datetime.timezone.utc, tzutc()]:
        return dt

    return datetime.datetime.fromtimestamp(time.mktime(dt.timetuple()), tz=datetime.timezone.utc)


def from_epoch_to_utc_dt(epoch_seconds):
    """
    Converts a seconds epoch to UTC, can include millisecond precision past decimal point
    :epoch_seconds: Seconds elapsed since epoch (int or float)
    :return: The datetime in UTC
    """
    return datetime.datetime.fromtimestamp(epoch_seconds, datetime.timezone.utc)
