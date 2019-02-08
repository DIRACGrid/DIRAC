"""Logging Filter based on pattern."""

__RCSID__ = '$Id$'


class PatternFilter(object):
  """Filter module to set loglevel per module.

  ::

    Resources
    {
      LogBackends
      {
        <backend>
        {
          Filter = MyPatternFilter
        }
      }
      LogFilters
      {
         MyPatternFilter
         {
           Type = PatternFilter
           Accept = Some, Words, To, Match
           Reject = Foo, Bar, Baz, DEBUG
         }
      }
    }

  Only print log lines matching words in Accept and reject lines which contains words in Reject
  """
  def __init__(self, optionDict):
    """Contruct the object, set the base LogLevel to DEBUG, and parse the options."""
    self._configDict = {}
    self._configDict['Accept'] = [w.strip() for w in optionDict.get('Accept', '').split(',') if w.strip()]
    self._configDict['Reject'] = [w.strip() for w in optionDict.get('Reject', '').split(',') if w.strip()]

  def __filter(self, record):
    """Check if accept or reject.

    :returns: boolean for filter value
    """
    msgPs = [record.args[0], record.varmessage]
    result = (self._configDict['Accept'] or any(w in msg for w in self._configDict['Accept'] for msg in msgPs)) and \
             not any(w in msg for w in self._configDict['Reject'] for msg in msgPs)
    return result

  def filter(self, record):
    """Filter records based on the path of the logger."""
    return self.__filter(record)
