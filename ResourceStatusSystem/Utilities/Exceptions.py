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

class InvalidSiteType(RSSException):
  pass

class InvalidServiceType(RSSException):
  pass

class InvalidResourceType(RSSException):
  pass

class InvalidPolicyType(RSSException):
  pass

class InvalidService(RSSException):
  pass

class InvalidView(RSSException):
  pass

