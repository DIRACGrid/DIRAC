########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/DB/UserProfileDB.py,v 1.3 2009/10/07 14:25:06 acasajus Exp $
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id: UserProfileDB.py,v 1.3 2009/10/07 14:25:06 acasajus Exp $"

import time
import md5
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
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

    if 'up_Users' not in tablesInDB:
      tablesD[ 'up_Users' ] = { 'Fields' : { 'Id' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                                   'UserName' : 'VARCHAR(255) NOT NULL',
                                                   'UserGroup' : 'VARCHAR(255) NOT NULL',
                                                   'LastAccess' : 'DATETIME'
                                                  },
                                        'PrimaryKey' : 'Id',
                                        'UniqueIndexes' : { 'UG' : [ 'UserName', 'UserGroup' ] }
                                      }
    if 'up_ProfilesData' not in tablesInDB:
      tablesD[ 'up_ProfilesData' ] = { 'Fields' : { 'UserId' : 'INTEGER',
                                                       'Profile' : 'VARCHAR(255) NOT NULL',
                                                       'VarName' : 'VARCHAR(255) NOT NULL',
                                                       'Data' : 'BLOB'
                                                  },
                                      'PrimaryKey' : [ 'UserId', 'Profile', 'VarName' ],
                                      'Indexes' : { 'ProfileKey' : [ 'UserId', 'VarName' ] }
                                     }
    if 'up_HashTags' not in tablesInDB:
      tablesD[ 'up_HashTags' ] = { 'Fields' : { 'UserId' : 'INTEGER',
                                                'HashTag' : 'VARCHAR(32) NOT NULL',
                                                'TagName' : 'VARCHAR(255) NOT NULL',
                                                'LastAccess' : 'DATETIME'
                                              },
                                    'PrimaryKey' : [ 'UserId', 'TagName' ],
                                    'Indexes' : { 'HashKey' : [ 'UserId', 'HashTag' ] }
                                  }
    return self._createTables( tablesD )

  def deleteProfile( self, userName, userGroup = False ):
    """
    Delete the profiles for a user
    """
    selectSQL = "SELECT Id from `up_Users` WHERE UserName='%s'" % userName
    if userGroup:
      selectSQL ="%s AND UserGroup='%s'" % userGroup
    result = self._query( selectSQL )
    if not result[ 'OK' ]:
      return result
    for id in result[ 'Value' ]:
      id = id[0]
      delSQL = "DELETE FROM `up_ProfilesData` WHERE UserId=%s" % id
      result = self._update( delSQL )
      if not result[ 'OK' ]:
        return result
      delSQL = "DELETE FROM `up_Users` WHERE Id=%s" % id
      result = self._update( delSQL )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def getUserId( self, userName, userGroup, insertIfMissing = True, connObj = False ):
    """
    Get the profile id and insert if missing if possible
    """
    selectSQL = "SELECT Id FROM `up_Users` WHERE UserName='%s' AND UserGroup='%s'" % ( userName,
                                                                                             userGroup )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      return S_OK( data[0][0] )
    elif not insertIfMissing:
      return S_ERROR( "No profile for user %s@%s" % ( userName, userGroup ) )
    insertSQL = "INSERT INTO `up_Users` ( Id, UserName, UserGroup ) VALUES ( 0, '%s', '%s' )" % ( userName,
                                                                                                       userGroup )
    result = self._update( insertSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    if 'lastRowId' in result:
      return S_OK( result['lastRowId'] )
    return self.getUserId( userName, userGroup, insertIfMissing = False )

  def __webProfileDataCond( self, userId, profileId, varName = False ):
    condSQL = [ 'UserId=%s' % userId, 'Profile="%s"' % profileId ]
    if varName:
      condSQL.append( 'VarName="%s"' % varName )
    return " AND ".join( condSQL )

  def retrieveVarByUserId( self, userId, profileId, varName, connObj = False ):
    """
    Get a data entry for a profile
    """
    selectSQL = "SELECT data FROM `up_ProfilesData` WHERE %s" % self.__webProfileDataCond( userId, profileId, varName )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      return S_OK( data[0][0] )
    return S_ERROR( "No data for combo profile %s profileId %s varName %s" % ( userId, profileId, varName) )

  def retrieveAllVarsByUserId( self, userId, profileId, connObj = False ):
    """
    Get a data entry for a profile
    """
    selectSQL = "SELECT varName, data FROM `up_ProfilesData` WHERE %s" % self.__webProfileDataCond( userId, profileId )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    return S_OK( dict( data ) )

  def deleteVarByUserId( self, userId, profileId, varName, connObj = False ):
    """
    Remove a data entry for a profile
    """
    selectSQL = "DELETE FROM `up_ProfilesData` WHERE %s" % self.__webProfileDataCond( userId, profileId, varName )
    return self._update( selectSQL, conn = connObj )

  def storeVarByUserId( self, userId, profileId, varName, data, connObj = False ):
    """
    Set a data entry for a profile
    """
    result = self._escapeString( data )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    insertSQL = "INSERT INTO `up_ProfilesData` ( UserId, Profile, VarName, Data ) VALUES ( %s, '%s', '%s', %s )" % ( userId,
                                                                                                                            profileId,
                                                                                                                            varName,
                                                                                                                            data )
    result = self._update( insertSQL, conn = connObj )
    if result[ 'OK' ]:
      return result
    #If error and not duplicate -> real error
    if result[ 'Message' ].find( "Duplicate entry" ) == -1:
      return result
    updateSQL = "UPDATE `up_ProfilesData` set Data=%s WHERE %s" % ( data,
                                                                       self.__webProfileDataCond( userId,
                                                                                               profileId,
                                                                                               varName ) )
    return self._update( updateSQL, conn = connObj )
  
  def storeHashTagByUserId( self, userId, tagName, hashTag = False, connObj = False ):
    """
    Set a data entry for a profile
    """
    if not hashTag:
      hashTag = md5.md5()
      hashTag.update( "%s;%s;%s" % ( Time.dateTime(), userId, tagName ) )
      hashTag = hashTag.hexdigest()
    hashTagUnescaped = hashTag
    result = self._escapeString( hashTag )
    if not result[ 'OK' ]:
      return result
    hashTag = result[ 'Value' ]
    result = self._escapeString( tagName )
    if not result[ 'OK' ]:
      return result
    tagName = result[ 'Value' ]
    insertSQL = "INSERT INTO `up_HashTags` ( UserId, TagName, HashTag ) VALUES ( %s, %s, %s )" % ( userId, tagName, hashTag )
    result = self._update( insertSQL, conn = connObj )
    if result[ 'OK' ]:
      return S_OK( hashTagUnescaped )
    #If error and not duplicate -> real error
    if result[ 'Message' ].find( "Duplicate entry" ) == -1:
      return result
    updateSQL = "UPDATE `up_HashTags` set HashTag=%s WHERE UserId = %s AND TagName = %s" % ( hashTag, userId, tagName )
    result = self._update( updateSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    return S_OK( hashTagUnescaped )

  def retrieveHashTagByUserId( self, userId, hashTag, connObj = False ):
    """
    Get a data entry for a profile
    """
    result = self._escapeString( hashTag )
    if not result[ 'OK' ]:
      return result
    hashTag = result[ 'Value' ]
    selectSQL = "SELECT HashTag FROM `up_HashTags` WHERE UserId = %s AND HashTag = %s" % ( userId, hashTag )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      return S_OK( data[0][0] )
    return S_ERROR( "No data for combo userId %s hashTag %s" % ( userId, hashTag ) )

  def retrieveAllHashTagsByUserId( self, userId, tagName, connObj = False ):
    """
    Get a data entry for a profile
    """
    result = self._escapeString( tagName )
    if not result[ 'OK' ]:
      return result
    tagName = result[ 'Value' ]
    selectSQL = "SELECT HashTag, TagName FROM `up_HashTags` WHERE UserId = %s" % ( userId )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    return S_OK( dict( data ) )

  def retrieveVar( self, userName, userGroup, profileId, varName ):
    """
    Helper for getting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userId = result[ 'Value' ]
      return self.retrieveVarByUserId( userId, profileId, varName, connObj = connObj  )
    finally:
      connObj.close()
      
  def retrieveAllVars( self, userName, userGroup, profileId ):
    """
    Helper for getting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userId = result[ 'Value' ]
      return self.retrieveAllVarsByUserId( userId, profileId, connObj = connObj  )
    finally:
      connObj.close()

  def storeVar( self, userName, userGroup, profileId, varName, data ):
    """
    Helper for setting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userId = result[ 'Value' ]
      return self.storeVarByUserId( userId, profileId, varName, data, connObj = connObj  )
    finally:
      connObj.close()

  def deleteVar( self, userName, userGroup, profileId, varName ):
    """
    Helper for deleteting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userId = result[ 'Value' ]
      return self.deleteVarByUserId( userId, profileId, varName, connObj = connObj  )
    finally:
      connObj.close()
      
  def storeHashTag( self, userName, userGroup, tagName, hashTag = False ):
    """
    Helper for deleteting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userId = result[ 'Value' ]
      return self.storeHashTagByUserId( userId, tagName, hashTag, connObj = connObj  )
    finally:
      connObj.close()
      
  def retrieveHashTag( self, userName, userGroup, hashTag ):
    """
    Helper for deleteting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userId = result[ 'Value' ]
      return self.retrieveHashTagByUserId( userId, hashTag, connObj = connObj  )
    finally:
      connObj.close()
      
  def retrieveAllHashTags( self, userName, userGroup ):
    """
    Helper for deleteting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserId( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userId = result[ 'Value' ]
      return self.retrieveAllHashTagsByUserId( userId, connObj = connObj  )
    finally:
      connObj.close()