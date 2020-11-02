"""
DIRAC Times module
Support for basic Date and Time operations
based on system datetime module.

It provides common interface to UTC timestamps,
converter to string types and back.

The following datetime classes are used in the returned objects:

  - dateTime  = datetime.datetime
  - date      = datetime.date
  - time      = datetime.timedelta

Useful timedelta constant are also provided to
define time intervals.

Notice: datetime.timedelta objects allow multiplication and division by interger
but not by float. Thus:

  - DIRAC.Times.second * 1.5             is not allowed
  - DIRAC.Times.second * 3 / 2           is allowed

An timeInterval class provides a method to check
if a give datetime is in the defined interval.

"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import time as nativetime
import datetime
import six
import sys

__RCSID__ = "$Id$"


# Some useful constants for time operations
microsecond = datetime.timedelta(microseconds=1)
second = datetime.timedelta(seconds=1)
minute = datetime.timedelta(minutes=1)
hour = datetime.timedelta(hours=1)
day = datetime.timedelta(days=1)
week = datetime.timedelta(days=7)

dt = datetime.datetime(2000, 1, 1)


def timeThis(method):
  """ Function to be used as a decorator for timing other functions/methods
  """

  def timed(*args, **kw):
    """ What actually times
    """
    ts = nativetime.time()
    result = method(*args, **kw)
    if sys.stdout.isatty():
      return result
    te = nativetime.time()

    pre = dt.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC ")

    try:
      pre += args[0].log.getName() + '/' + args[0].log.getSubName() + '   TIME: ' + args[0].transString
    except AttributeError:
      try:
        pre += args[0].log.getName() + '    TIME: ' + args[0].transString
      except AttributeError:
        try:
          pre += args[0].log.getName() + '/' + args[0].log.getSubName() + '   TIME: '
        except AttributeError:
          pre += 'TIME: '
    except IndexError:
      pre += 'TIME: '

    argsLen = ''
    if args:
      try:
        if isinstance(args[1], (list, dict)):
          argsLen = "arguments len: %d" % len(args[1])
      except IndexError:
        if kw:
          try:
            if isinstance(list(list(kw.items())[0])[1], (list, dict)):
              argsLen = "arguments len: %d" % len(list(list(kw.items())[0])[1])
          except IndexError:
            argsLen = ''

    print("%s Exec time ===> function %r %s -> %2.2f sec" % (pre, method.__name__, argsLen, te - ts))
    return result

  return timed


def dateTime():
  """
  Return current UTC datetime, as datetime.datetime object
  """
  return dt.utcnow()


def date(myDateTime=None):
  """
  Return current UTC date, as datetime.date object
  if a _dateTimeType is pass as argument its associated date is returned
  """
  if isinstance(myDateTime, _dateTimeType):
    return myDateTime.date()
  return dateTime().date()


def time(myDateTime=None):
  """
  Return current UTC time, as datetime.time object
  if a _dateTimeType is pass as argument its associated time is returned
  """
  if not isinstance(myDateTime, _dateTimeType):
    myDateTime = dateTime()
  return myDateTime - datetime.datetime(myDateTime.year, myDateTime.month, myDateTime.day)


def toEpoch(dateTimeObject=None):
  """
  Get seconds since epoch
  """
  if not dateTimeObject:
    dateTimeObject = dateTime()
  return nativetime.mktime(dateTimeObject.timetuple())


def fromEpoch(epoch):
  """
  Get datetime object from epoch
  """
  return dt.fromtimestamp(epoch)


def to2K(dateTimeObject=None):
  """
  Get seconds, with microsecond precission, since 2K
  """
  if not dateTimeObject:
    dateTimeObject = dateTime()
  delta = dateTimeObject - dt
  return delta.days * 86400 + delta.seconds + delta.microseconds / 1000000.


def from2K(seconds2K=None):
  """
  Get date from seconds since 2K
  """
  if not seconds2K:
    seconds2K = to2K(dt)
  return dt + int(seconds2K) * second + int(seconds2K % 1 * 1000000) * microsecond


def toString(myDate=None):
  """
  Convert to String
  if argument type is neither _dateTimeType, _dateType, nor _timeType
  the current dateTime converted to String is returned instead

  Notice: datetime.timedelta are converted to strings using the format:
    [day] days [hour]:[min]:[sec]:[microsec]
    where hour, min, sec, microsec are always positive integers,
    and day carries the sign.
  To keep internal consistency we are using:
    [hour]:[min]:[sec]:[microsec]
    where min, sec, microsec are alwys positive intergers and hour carries the
    sign.
  """
  if isinstance(myDate, _dateTimeType):
    return str(myDate)

  elif isinstance(myDate, _dateType):
    return str(myDate)

  elif isinstance(myDate, _timeType):
    return '%02d:%02d:%02d.%06d' % (myDate.days * 24 + myDate.seconds / 3600,
                                    myDate.seconds % 3600 / 60,
                                    myDate.seconds % 60,
                                    myDate.microseconds)
  else:
    return toString(dateTime())


def fromString(myDate=None):
  """
  Convert date/time/datetime String back to appropriated objects

  The format of the string it is assume to be that returned by toString method.
  See notice on toString method
  On Error, return None
  """
  if isinstance(myDate, six.string_types):
    if myDate.find(' ') > 0:
      dateTimeTuple = myDate.split(' ')
      dateTuple = dateTimeTuple[0].split('-')
      try:
        return (datetime.datetime(year=dateTuple[0],
                                  month=dateTuple[1],
                                  day=dateTuple[2]) +
                fromString(dateTimeTuple[1]))
        # return dt.combine( fromString( dateTimeTuple[0] ),
        #                                   fromString( dateTimeTuple[1] ) )
      except BaseException:
        try:
          return (datetime.datetime(year=int(dateTuple[0]),
                                    month=int(dateTuple[1]),
                                    day=int(dateTuple[2])) +
                  fromString(dateTimeTuple[1]))
        except ValueError:
          return None
        # return dt.combine( fromString( dateTimeTuple[0] ),
        #                                   fromString( dateTimeTuple[1] ) )
    elif myDate.find(':') > 0:
      timeTuple = myDate.replace('.', ':').split(':')
      try:
        if len(timeTuple) == 4:
          return datetime.timedelta(hours=int(timeTuple[0]),
                                    minutes=int(timeTuple[1]),
                                    seconds=int(timeTuple[2]),
                                    microseconds=int(timeTuple[3]))
        elif len(timeTuple) == 3:
          try:
            return datetime.timedelta(hours=int(timeTuple[0]),
                                      minutes=int(timeTuple[1]),
                                      seconds=int(timeTuple[2]),
                                      microseconds=0)
          except ValueError:
            return None
        else:
          return None
      except BaseException:
        return None
    elif myDate.find('-') > 0:
      dateTuple = myDate.split('-')
      try:
        return datetime.date(int(dateTuple[0]), int(dateTuple[1]), int(dateTuple[2]))
      except BaseException:
        return None

  return None


class timeInterval(object):
  """
     Simple class to define a timeInterval object able to check if a given
     dateTime is inside
  """

  def __init__(self, initialDateTime, intervalTimeDelta):
    """
       Initialization method, it requires the initial dateTime and the
       timedelta that define the limits.
       The upper limit is not included thus it is [begin,end)
       If not properly initialized an error flag is set, and subsequent calls
       to any method will return None
    """
    if (not isinstance(initialDateTime, _dateTimeType) or
            not isinstance(intervalTimeDelta, _timeType)):
      self.__error = True
      return None
    self.__error = False
    if intervalTimeDelta.days < 0:
      self.__startDateTime = initialDateTime + intervalTimeDelta
      self.__endDateTime = initialDateTime
    else:
      self.__startDateTime = initialDateTime
      self.__endDateTime = initialDateTime + intervalTimeDelta

  def includes(self, myDateTime):
    """
    """
    if self.__error:
      return None
    if not isinstance(myDateTime, _dateTimeType):
      return None
    if myDateTime < self.__startDateTime:
      return False
    if myDateTime >= self.__endDateTime:
      return False
    return True


def queryTime(f):
  """ Decorator to measure the function call time
  """
  def measureQueryTime(*args, **kwargs):
    start = nativetime.time()
    result = f(*args, **kwargs)
    if result['OK'] and 'QueryTime' not in result:
      result['QueryTime'] = nativetime.time() - start
    return result
  return measureQueryTime


_dateTimeType = type(dateTime())
_dateType = type(date())
_timeType = type(time())

_allTimeTypes = (_dateTimeType, _timeType)
_allDateTypes = (_dateTimeType, _dateType)
_allTypes = (_dateTimeType, _dateType, _timeType)
