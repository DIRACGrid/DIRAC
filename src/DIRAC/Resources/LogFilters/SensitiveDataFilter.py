"""Logging Filter based on sensitive data."""
import collections.abc
import re


class SensitiveDataFilter:
    """Filter module to replace sensitive data by "***REDACTED***".

    ::

      Resources
      {
        LogBackends
        {
          <backend>
          {
            Filter = MySensitiveDataFilter
          }
        }
        LogFilters
        {
           MySensitiveDataFilter
           {
             Plugin = SensitiveDataFilter
           }
        }
      }

    This filter is attached to every logger instances within DIRAC by default.
    """

    __redacted = "***REDACTED***"

    def __init__(self, optionDict=None):
        """Nothing to do"""
        pass

    def __filter(self, record):
        """Check whether the record contains sensitive data such as a certificate.

        :returns: boolean for filter value
        """
        # a list of sensitive words to replace
        sensitiveData = [
            r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
            r"-----BEGIN PRIVATE KEY-----.*?-----END PRIVATE KEY-----",
        ]

        # record.args can be a tuple
        # record.args[0] contains the fixed part of the log in this case
        # see https://github.com/DIRACGrid/DIRAC/blob/v8.0.13/src/DIRAC/FrameworkSystem/private/standardLogging/Logging.py#L427
        # record.args can also be a dict
        # record.args contains the fixed part of the log in this case
        # see https://github.com/python/cpython/blob/v3.11.1/Lib/logging/__init__.py#L301-L317
        if isinstance(record.args, collections.abc.Mapping):
            fixedMessage = record.args
        else:
            fixedMessage = record.args[0]

        for sensitiveExpression in sensitiveData:
            fixedMessage = re.sub(
                sensitiveExpression,
                self.__redacted,
                f"{fixedMessage}",
                flags=re.DOTALL,
            )

            # record.varmessage contains the variable part of the log
            # this is specific to the DIRAC Logging object (see Logging._createLogRecord())
            record.varmessage = re.sub(
                sensitiveExpression,
                self.__redacted,
                f"{record.varmessage}",
                flags=re.DOTALL,
            )

        # Replace record.args by the processed fixedMessage
        record.args = (fixedMessage,)
        return True

    def filter(self, record):
        """Filter records based on the sensitive data it contains."""
        return self.__filter(record)
