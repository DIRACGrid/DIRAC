""" collects:

      - exceptions
"""

from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidSiteType, \
    ValidServiceType, ValidResourceType, PolicyTypes


#############################################################################

class RSSException(Exception):

  def __init__(self, message = ""):
    self.message = message
    Exception.__init__(self, message)

  def __str__(self):
    return "Generic Exception in the RSS: \n" + repr(self.message)

#############################################################################

class InvalidRes(RSSException):

  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid resource type: \nshould be in " + repr(ValidRes) + repr(self.message)

#############################################################################

class InvalidStatus(RSSException):

  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid status type: \nshould be in " + repr(ValidStatus) + repr(self.message)

#############################################################################

class InvalidSiteType(RSSException):

  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid resource type: \nshould be in " + repr(ValidSiteType) + repr(self.message)

#############################################################################

class InvalidServiceType(RSSException):

  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid service type: \nshould be in " + repr(ValidServiceType) + repr(self.message)

#############################################################################

class InvalidResourceType(RSSException):

  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid resource type: \nshould be in " + repr(ValidResourceType) + repr(self.message)

#############################################################################

class InvalidPolicyType(RSSException):

  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid policy type: \nshould be in " + repr(PolicyTypes) + repr(self.message)

#############################################################################

class InvalidStateValueDict(RSSException):

  def __init__(self,  message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid dictionnary for the state values. " + repr(self.message)

#############################################################################
