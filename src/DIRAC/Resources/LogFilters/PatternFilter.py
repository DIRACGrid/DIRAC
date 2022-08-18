"""Logging Filter based on pattern."""


class PatternFilter:
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
             Plugin = PatternFilter
             Accept = Some, Words, To, Match
             Reject = Foo, Bar, Baz, DEBUG
           }
        }
      }

    Only print log lines matching words in Accept and reject lines which contains words in Reject.
    A line must both be accepted and not rejected to be passed!
    """

    def __init__(self, optionDict):
        """Contruct the object, set the base LogLevel to DEBUG, and parse the options."""

        self._accept = [w.strip() for w in optionDict.get("Accept", "").split(",") if w.strip()]
        self._reject = [w.strip() for w in optionDict.get("Reject", "").split(",") if w.strip()]

    def __filter(self, record):
        """Check if accept or reject.

        :returns: boolean for filter value
        """
        msgPs = [record.args[0], record.varmessage]
        # if accept is empty, we accept everything and only reject will matter
        accepted = not self._accept or any(w in msg for w in self._accept for msg in msgPs)
        if not accepted:  # if accepted is False we just stop here
            return False
        rejected = any(w in msg for w in self._reject for msg in msgPs)
        return not rejected  # accepted must be True

    def filter(self, record):
        """Filter records based on the path of the logger."""
        return self.__filter(record)
