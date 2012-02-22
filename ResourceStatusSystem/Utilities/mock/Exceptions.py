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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    