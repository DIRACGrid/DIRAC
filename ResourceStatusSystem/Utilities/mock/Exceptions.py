from DIRAC.ResourceStatusSystem.mock import ValidRes, ValidStatus, ValidSiteType, \
    ValidServiceType, ValidResourceType
    
################################################################################

class RSSDBException( Exception ):
  """
  DB exception
  """

  def __init__( self, message = "" ):
    self.message = message
    Exception.__init__( self, message )

  def __str__( self ):
    return "Exception in the RSS DB: " + repr( self.message )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    