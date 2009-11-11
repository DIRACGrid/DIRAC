""" collects:

      - exceptions
"""

#############################################################################
# exceptions
#############################################################################

class RSSException(Exception):
  pass

class InvalidRes(RSSException):
  pass

class InvalidStatus(RSSException):
  pass

class InvalidPolicyType(RSSException):
  pass

class InvalidService(RSSException):
  pass

