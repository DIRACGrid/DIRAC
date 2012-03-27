# $HeadURL $
''' Exceptions

  RSS Exceptions. Will be obsolete soon.

'''

from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidSiteType, \
    ValidServiceType, ValidResourceType

__RCSID__  = '$Id: $'

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