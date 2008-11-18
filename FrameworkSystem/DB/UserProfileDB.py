########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/DB/UserProfileDB.py,v 1.1 2008/11/18 15:58:19 acasajus Exp $
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id: UserProfileDB.py,v 1.1 2008/11/18 15:58:19 acasajus Exp $"

import time
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

class UserProfileDB(DB):

  def __init__( self ):
    DB.__init__(self,'UserProfileDB','Framework/UserProfileDB', 10 )
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ])

  def __initializeDB(self):
    """
    Create the tables
    """
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesD = {}

    if 'up_Profiles' not in tablesInDB:
      tablesD[ 'up_Profiles' ] = { 'Fields' : { 'Id' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                                   'UserName' : 'VARCHAR(255) NOT NULL',
                                                   'UserGroup' : 'VARCHAR(255) NOT NULL',
                                                  },
                                        'PrimaryKey' : 'Id',
                                        'UniqueIndexes' : { 'UG' : [ 'UserName', 'UserGroup' ] }
                                      }
    if 'up_WebProfilesData' not in tablesInDB:
      tablesD[ 'up_WebProfilesData' ] = { 'Fields' : { 'ProfileId' : 'INTEGER',
                                                  'Action' : 'VARCHAR(255) NOT NULL',
                                                  'DataKey' : 'VARCHAR(255) NOT NULL',
                                                  'Data' : 'BLOB'
                                                  },
                                      'PrimaryKey' : [ 'ProfileId', 'Action', 'DataKey' ]
                                     }
    return self._createTables( tablesD )

  def deleteProfile( self, userName, userGroup = False ):
    """
    Delete the profiles for a user
    """
    selectSQL = "SELECT Id from `up_Profiles` WHERE UserName='%s'" % userName
    if userGroup:
      selectSQL ="%s AND UserGroup='%s'" % userGroup
    result = self._query( selectSQL )
    if not result[ 'OK' ]:
      return result
    for id in result[ 'Value' ]:
      id = id[0]
      delSQL = "DELETE FROM `up_WebProfilesData` WHERE ProfileId=%s" % id
      result = self._update( delSQL )
      if not result[ 'OK' ]:
        return result
      delSQL = "DELETE FROM `up_Profiles` WHERE Id=%s" % id
      result = self._update( delSQL )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def getProfileId( self, userName, userGroup, insertIfMissing = True, connObj = False ):
    """
    Get the profile id and insert if missing if possible
    """
    selectSQL = "SELECT Id FROM `up_Profiles` WHERE UserName='%s' AND UserGroup='%s'" % ( userName,
                                                                                             userGroup )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      return S_OK( data[0][0] )
    elif not insertIfMissing:
      return S_ERROR( "No profile for user %s@%s" % ( userName, userGroup ) )
    insertSQL = "INSERT INTO `up_Profiles` ( Id, UserName, UserGroup ) VALUES ( 0, '%s', '%s' )" % ( userName,
                                                                                                       userGroup )
    result = self._update( insertSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    if 'lastRowId' in result:
      return S_OK( result['lastRowId'] )
    return self.getProfileId( userName, userGroup, insertIfMissing = False )

  def __webProfileDataCond( self, profileId, action, dataKey ):
    return "ProfileId=%s AND Action='%s' AND DataKey='%s'" % ( profileId, action, dataKey )

  def retrieveWebDataByProfileId( self, profileId, action, dataKey, connObj = False ):
    """
    Get a data entry for a profile
    """
    selectSQL = "SELECT data FROM `up_WebProfilesData` WHERE %s" % self.__webProfileDataCond( profileId, action, dataKey )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      return S_OK( data[0][0] )
    return S_ERROR( "No data for combo profile %s action %s dataKey %s" % ( profileId, action, dataKey) )

  def storeWebDataByProfileId( self, profileId, action, dataKey, data, connObj = False ):
    """
    Set a data entry for a profile
    """
    result = self._escapeString( data )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    insertSQL = "INSERT INTO `up_WebProfilesData` ( ProfileId, Action, DataKey, Data ) VALUES ( %s, '%s', '%s', %s )" % ( profileId,
                                                                                                                            action,
                                                                                                                            dataKey,
                                                                                                                            data )
    result = self._update( insertSQL, conn = connObj )
    if result[ 'OK' ]:
      return result
    #If error and not duplicate -> real error
    if result[ 'Message' ].find( "Duplicate entry" ) == -1:
      return result
    updateSQL = "UPDATE `up_WebProfilesData` set Data=%s WHERE %s" % ( data,
                                                                       self.__webProfileDataCond( profileId,
                                                                                               action,
                                                                                               dataKey ) )
    return self._update( updateSQL, conn = connObj )


  def retrieveWebData( self, userName, userGroup, action, dataKey ):
    """
    Helper for getting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getProfileId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      profileId = result[ 'Value' ]
      return self.retrieveWebDataByProfileId( profileId, action, dataKey, connObj = connObj  )
    finally:
      connObj.close()

  def storeWebData( self, userName, userGroup, action, dataKey, data ):
    """
    Helper for setting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getProfileId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      profileId = result[ 'Value' ]
      return self.storeWebDataByProfileId( profileId, action, dataKey, data, connObj = connObj  )
    finally:
      connObj.close()
