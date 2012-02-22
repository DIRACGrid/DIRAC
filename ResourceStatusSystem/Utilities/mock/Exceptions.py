from DIRAC.ResourceStatusSystem.mock import ValidRes, ValidStatus, ValidSiteType, \
    ValidServiceType, ValidResourceType
    
class RSSException(Exception):
  
  def __init__(self, message = ""):
  
    self.message    = message 
    Exception.__init__( self, message )

  def __str__(self):
    return "Generic Exception in the RSS: \n" + repr(self.message)

################################################################################

class RSSDBException( RSSException ):
  """
  DB exception
  """

  def __init__( self, message = "" ):
    self.message = message
    RSSException.__init__( self, message )

  def __str__( self ):
    return "Exception in the RSS DB: " + repr( self.message )

################################################################################

class InvalidRes(RSSException):
  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid resource type: \nshould be in " + repr(ValidRes) + repr(self.message)

#################################################################################

class InvalidStatus(RSSException):
  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid status type: \nshould be in " + repr(ValidStatus) + repr(self.message)

#################################################################################

class InvalidSiteType(RSSException):
  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid site type: \nshould be in " + repr(ValidSiteType) + repr(self.message)

#################################################################################

class InvalidServiceType(RSSException):
  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Invalid service type: \nshould be in " + repr(ValidServiceType) + repr(self.message)

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    