########################################################################
# $HeadURL$
########################################################################

""" ProfileManager manages web user profiles
    in the DISET framework
"""

__RCSID__ = "$Id$"

import types
import os
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR, gConfig, rootPath
from DIRAC.FrameworkSystem.DB.UserProfileDB import UserProfileDB
from DIRAC.Core.Security import Properties

gUPDB = False

def initializeUserProfileManagerHandler( serviceInfo ):
  global gUPDB
  try:
    gUPDB = UserProfileDB()
  except Exception, e:
    return S_ERROR( "Can't initialize UserProfileDB: %s"  % str(e) )
  return S_OK()

class UserProfileManagerHandler( RequestHandler ):

  types_retrieveProfileVar = [ types.StringType, types.StringType ]
  def export_retrieveProfileVar( self, action, varName ):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveVar( userName, userGroup, action, varName )

  types_retrieveProfileAllVars = [ types.StringType ]
  def export_retrieveProfileAllVars( self, action ):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveAllVars( userName, userGroup, action )

  types_storeProfileVar = [ types.StringType, types.StringType, types.StringType ]
  def export_storeProfileVar( self, action, varName, data ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.storeVar( userName, userGroup, action, varName, data )

  types_deleteProfileVar = [ types.StringType, types.StringType ]
  def export_deleteProfileVar( self, action, varName ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.deleteVar( userName, userGroup, action, varName )
  
  types_storeHashTag = [ types.StringType ]
  def export_storeHashTag( self, tagName ):
    """ Set hash tag
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.storeHashTag( userName, userGroup, tagName )
  
  types_retrieveHashTag = [ types.StringType ]
  def export_retrieveHashTag( self, hashTag ):
    """ Get hash tag
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveHashTag( userName, userGroup, hashTag )

  types_retrieveAllHashTags = []
  def export_retrieveAllHashTags( self ):
    """ Get all hash tags
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    print "ASDASDASD"
    return gUPDB.retrieveAllHashTags( userName, userGroup )

  types_deleteProfiles = [ types.ListType ]
  def export_deleteProfiles( self, userList ):
    """
    Delete profiles for a list of users
    """
    credDict = self.getRemoteCredentials()
    requesterUserName = credDict[ 'username' ]
    if Properties.SERVICE_ADMINISTRATOR in credDict[ 'properties' ]:
      admin = True
    else:
      admin = False
    for entry in userList:
      userName = entry
      if admin or userName == requesterUserName:
        result = gUPDB.deleteProfile( userName )
        if not result[ 'OK' ]:
          return result
    return S_OK()
