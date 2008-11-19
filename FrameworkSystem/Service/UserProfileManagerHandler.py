########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Service/UserProfileManagerHandler.py,v 1.2 2008/11/19 10:19:33 acasajus Exp $
########################################################################

""" WebProfileManager manages web user profiles
    in the DISET framework
"""

__RCSID__ = "$Id: UserProfileManagerHandler.py,v 1.2 2008/11/19 10:19:33 acasajus Exp $"

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

  types_retrieveWebProfileData = [ types.StringType, types.StringType ]
  def export_retrieveWebProfileData( self, action, dataKey ):
    """ Get profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.retrieveWebData( userName, userGroup, action, dataKey )

  types_storeWebProfileData = [ types.StringType, types.StringType, types.StringType ]
  def export_storeWebProfileData( self, action, dataKey, data ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.storeWebData( userName, userGroup, action, dataKey, data )

  types_deleteWebProfileData = [ types.StringType, types.StringType ]
  def export_deleteWebProfileData( self, action, dataKey ):
    """ Set profile data for web
    """
    credDict = self.getRemoteCredentials()
    userName = credDict[ 'username' ]
    userGroup = credDict[ 'group' ]
    return gUPDB.deleteWebData( userName, userGroup, action, dataKey )

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
