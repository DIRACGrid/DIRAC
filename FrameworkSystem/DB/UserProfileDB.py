########################################################################
# $HeadURL$
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id$"

import time, types
try:
  import hashlib as md5
except:
  import md5
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Base.DB import DB

class UserProfileDB( DB ):

  def __init__( self ):
    DB.__init__( self, 'UserProfileDB', 'Framework/UserProfileDB', 10 )
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ] )

  def __initializeDB( self ):
    """
    Create the tables
    """
    self.__permValues = [ 'USER', 'GROUP', 'VO', 'ALL' ]
    self.__permAttrs = [ 'ReadAccess' ]
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesD = {}

    if 'up_Users' not in tablesInDB:
      tablesD[ 'up_Users' ] = { 'Fields' : { 'Id' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                             'UserName' : 'VARCHAR(32) NOT NULL',
                                             'LastAccess' : 'DATETIME'
                                            },
                                        'PrimaryKey' : 'Id',
                                        'UniqueIndexes' : { 'U' : [ 'UserName' ] }
                                      }

    if 'up_Groups' not in tablesInDB:
      tablesD[ 'up_Groups' ] = { 'Fields' : { 'Id' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                              'UserGroup' : 'VARCHAR(32) NOT NULL',
                                              'LastAccess' : 'DATETIME'
                                            },
                                        'PrimaryKey' : 'Id',
                                        'UniqueIndexes' : { 'G' : [ 'UserGroup' ] }
                                      }

    if 'up_VOs' not in tablesInDB:
      tablesD[ 'up_VOs' ] = { 'Fields' : { 'Id' : 'INTEGER AUTO_INCREMENT NOT NULL',
                                           'VO' : 'VARCHAR(32) NOT NULL',
                                           'LastAccess' : 'DATETIME'
                                         },
                                        'PrimaryKey' : 'Id',
                                        'UniqueIndexes' : { 'VO' : [ 'VO' ] }
                                      }

    if 'up_ProfilesData' not in tablesInDB:
      tablesD[ 'up_ProfilesData' ] = { 'Fields' : { 'UserId' : 'INTEGER',
                                                    'GroupId' : 'INTEGER',
                                                    'VOId' : 'INTEGER',
                                                    'Profile' : 'VARCHAR(255) NOT NULL',
                                                    'VarName' : 'VARCHAR(255) NOT NULL',
                                                    'Data' : 'BLOB',
                                                    'ReadAccess' : 'VARCHAR(10) DEFAULT "USER"'
                                                  },
                                      'PrimaryKey' : [ 'UserId', 'GroupId', 'Profile', 'VarName' ],
                                      'Indexes' : { 'ProfileKey' : [ 'UserId', 'GroupId', 'Profile' ],
                                                    'UserKey' : [ 'UserId' ] }
                                     }

    if 'up_HashTags' not in tablesInDB:
      tablesD[ 'up_HashTags' ] = { 'Fields' : { 'UserId' : 'INTEGER',
                                                'GroupId' : 'INTEGER',
                                                'VOId' : 'INTEGER',
                                                'HashTag' : 'VARCHAR(32) NOT NULL',
                                                'TagName' : 'VARCHAR(255) NOT NULL',
                                                'LastAccess' : 'DATETIME'
                                              },
                                    'PrimaryKey' : [ 'UserId', 'GroupId', 'TagName' ],
                                    'Indexes' : { 'HashKey' : [ 'UserId', 'HashTag' ] }
                                  }
    return self._createTables( tablesD )

  def __getUserId( self, userName, insertIfMissing = True, connObj = False ):
    return self.__getObjId( userName, 'UserName', 'up_Users', insertIfMissing, connObj )

  def __getGroupId( self, groupName, insertIfMissing = True, connObj = False ):
    return self.__getObjId( groupName, 'UserGroup', 'up_Groups', insertIfMissing, connObj )

  def __getVOId( self, voName, insertIfMissing = True, connObj = False ):
    return self.__getObjId( voName, 'VO', 'up_VOs', insertIfMissing, connObj )

  def __getObjId( self, objValue, varName, tableName, insertIfMissing = True, connObj = False ):
    result = self._escapeString( objValue )
    if not result[ 'OK' ]:
      return result
    sqlObjValue = result[ 'Value' ]
    selectSQL = "SELECT Id FROM `%s` WHERE %s = %s" % ( tableName, varName, sqlObjValue )
    result = self._query( selectSQL, connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      id = data[0][0]
      self._update ( "UPDATE `%s` SET LastAccess = UTC_TIMESTAMP() WHERE Id = %s" % ( tableName, id ) )
      return S_OK( id )
    if not insertIfMissing:
      return S_ERROR( "No entry %s for %s defined in the DB" % ( objValue, varName ) )
    insertSQL = "INSERT INTO `%s` ( Id, %s, LastAccess ) VALUES ( 0, %s, UTC_TIMESTAMP() )" % ( tableName, varName, sqlObjValue )
    result = self._update( insertSQL, connObj )
    if not result[ 'OK' ]:
      return result
    return S_OK( result[ 'lastRowId' ] )

  def getUserGroupIds( self, userName, userGroup, insertIfMissing = True, connObj = False ):
    result = self.__getUserId( userName, insertIfMissing, connObj = connObj )
    if not result[ 'OK' ]:
      return result
    userId = result[ 'Value' ]
    result = self.__getGroupId( userGroup, insertIfMissing, connObj = connObj )
    if not result[ 'OK' ]:
      return result
    groupId = result[ 'Value' ]
    userVO = Registry.getVOForGroup( userGroup )
    if not userVO:
      userVO = "undefined"
    result = self.__getVOId( userVO, insertIfMissing, connObj = connObj )
    if not result[ 'OK' ]:
      return result
    voId = result[ 'Value' ]
    return S_OK( ( userId, groupId, voId ) )

  def deleteUserProfile( self, userName, userGroup = False ):
    """
    Delete the profiles for a user
    """
    result = self.__getUserId( userName )
    if not result[ 'OK' ]:
      return result
    userId = result[ 'Value' ]
    sqlCond = [ 'UserId=%s' % userId ]
    if userGroup:
      result = self.__getGroupId( userGroup )
      if not result[ 'OK' ]:
        return result
      groupId = result[ 'Value' ]
      sqlCond.append( "GroupId=%s" % userGroup )
    delSQL = "DELETE FROM `up_ProfilesData` WHERE %s" % " AND ".join( sqlCond )
    result = self._update( delSQL )
    if not result[ 'OK' ] or not userGroup:
      return result
    delSQL = "DELETE FROM `up_Users` WHERE Id = %s" % userId
    return self._update( delSQL )

  def __webProfileUserDataCond( self, userIds, sqlProfileName = False, sqlVarName = False ):
    condSQL = [ '`up_ProfilesData`.UserId=%s' % userIds[0],
                '`up_ProfilesData`.GroupId=%s' % userIds[1],
                '`up_ProfilesData`.VOId=%s' % userIds[2] ]
    if sqlProfileName:
      condSQL.append( '`up_ProfilesData`.Profile=%s' % sqlProfileName )
    if sqlVarName:
      condSQL.append( '`up_ProfilesData`.VarName=%s' % sqlVarName )
    return " AND ".join( condSQL )

  def __webProfileReadAccessDataCond( self, userIds, ownerIds, sqlProfileName, sqlVarName = False ):
    permCondSQL = []
    permCondSQL.append( '`up_ProfilesData`.UserId = %s AND `up_ProfilesData`.GroupId = %s' % ( ownerIds[0], ownerIds[1] ) )
    permCondSQL.append( '`up_ProfilesData`.GroupId=%s AND `up_ProfilesData`.ReadAccess="GROUP"' % userIds[1] )
    permCondSQL.append( '`up_ProfilesData`.VOId=%s AND `up_ProfilesData`.ReadAccess="VO"' % userIds[2] )
    permCondSQL.append( '`up_ProfilesData`.ReadAccess="ALL"' )
    sqlCond = []
    sqlCond.append( '`up_ProfilesData`.Profile = %s' % sqlProfileName )
    if sqlVarName:
      sqlCond.append( "`up_ProfilesData`.VarName = %s" % ( sqlVarName ) )
    #Perms
    sqlCond.append( "( ( %s ) )" % " ) OR ( ".join( permCondSQL ) )
    return " AND ".join( sqlCond )

  def __parsePerms( self, perms, addMissing = True ):
    normPerms = {}
    for pName in self.__permAttrs:
      if pName not in perms:
        if addMissing:
          normPerms[ pName ] = self.__permValues[0]
        continue
      else:
        permVal = perms[ pName ].upper()
        for nV in self.__permValues:
          if nV == permVal:
            normPerms[ pName ] = nV
            break
        if pName not in normPerms and addMissing:
          normPerms[ pName ] = self.__permValues[0]

    return normPerms

  def retrieveVarById( self, userIds, ownerIds, profileName, varName, connObj = False ):
    """
    Get a data entry for a profile
    """
    result = self._escapeString( profileName )
    if not result[ 'OK' ]:
      return result
    sqlProfileName = result[ 'Value' ]

    result = self._escapeString( varName )
    if not result[ 'OK' ]:
      return result
    sqlVarName = result[ 'Value' ]

    selectSQL = "SELECT data FROM `up_ProfilesData` WHERE %s" % self.__webProfileReadAccessDataCond( userIds, ownerIds,
                                                                                                     sqlProfileName, sqlVarName )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      return S_OK( data[0][0] )
    return S_ERROR( "No data for userIds %s profileName %s varName %s" % ( userIds, profileName, varName ) )

  def retrieveAllUserVarsById( self, userIds, profileName, connObj = False ):
    """
    Get a data entry for a profile
    """
    result = self._escapeString( profileName )
    if not result[ 'OK' ]:
      return result
    sqlProfileName = result[ 'Value' ]

    selectSQL = "SELECT varName, data FROM `up_ProfilesData` WHERE %s" % self.__webProfileUserDataCond( userIds, sqlProfileName )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    return S_OK( dict( data ) )

  def retrieveUserProfilesById( self, userIds, connObj = False ):
    """
    Get all profiles and data for a user
    """
    selectSQL = "SELECT Profile, varName, data FROM `up_ProfilesData` WHERE %s" % self.__webProfileUserDataCond( userIds )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    dataDict = {}
    for row in data:
      if row[0] not in dataDict:
        dataDict[ row[0] ] = {}
      dataDict[ row[0] ][ row[1] ] = row[2 ]
    return S_OK( dataDict )

  def retrieveVarPermsById( self, userIds, ownerIds, profileName, varName, connObj = False ):
    """
    Get a data entry for a profile
    """
    result = self._escapeString( profileName )
    if not result[ 'OK' ]:
      return result
    sqlProfileName = result[ 'Value' ]

    result = self._escapeString( varName )
    if not result[ 'OK' ]:
      return result
    sqlVarName = result[ 'Value' ]

    selectSQL = "SELECT %s FROM `up_ProfilesData` WHERE %s" % ( ", ".join( self.__permAttrs ),
                                                                self.__webProfileReadAccessDataCond( userIds, ownerIds,
                                                                                                     sqlProfileName, sqlVarName )
                                                              )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      permDict = {}
      for i in range( len( self.__permAttrs ) ):
        permDict[ self.__permAttrs[ i ] ] = data[0][i]
      return S_OK( permDict )
    return S_ERROR( "No data for userIds %s profileName %s varName %s" % ( userIds, profileName, varName ) )

  def deleteVarByUserId( self, userIds, profileName, varName, connObj = False ):
    """
    Remove a data entry for a profile
    """
    result = self._escapeString( profileName )
    if not result[ 'OK' ]:
      return result
    sqlProfileName = result[ 'Value' ]

    result = self._escapeString( varName )
    if not result[ 'OK' ]:
      return result
    sqlVarName = result[ 'Value' ]

    selectSQL = "DELETE FROM `up_ProfilesData` WHERE %s" % self.__webProfileUserDataCond( userIds, sqlProfileName, sqlVarName )
    return self._update( selectSQL, conn = connObj )

  def storeVarByUserId( self, userIds, profileName, varName, data, perms, connObj = False ):
    """
    Set a data entry for a profile
    """
    sqlInsertValues = []
    sqlInsertKeys = []

    sqlInsertKeys.append( ( 'UserId', userIds[0] ) )
    sqlInsertKeys.append( ( 'GroupId', userIds[1] ) )
    sqlInsertKeys.append( ( 'VOId', userIds[2] ) )

    result = self._escapeString( profileName )
    if not result[ 'OK' ]:
      return result
    sqlProfileName = result[ 'Value' ]
    sqlInsertKeys.append( ( 'Profile', sqlProfileName ) )

    result = self._escapeString( varName )
    if not result[ 'OK' ]:
      return result
    sqlVarName = result[ 'Value' ]
    sqlInsertKeys.append( ( 'VarName', sqlVarName ) )

    result = self._escapeString( data )
    if not result[ 'OK' ]:
      return result
    sqlInsertValues.append( ( 'Data', result[ 'Value' ] ) )

    normPerms = self.__parsePerms( perms )
    for k in normPerms:
      sqlInsertValues.append( ( k, '"%s"' % normPerms[ k ] ) )

    sqlInsert = sqlInsertKeys + sqlInsertValues
    insertSQL = "INSERT INTO `up_ProfilesData` ( %s ) VALUES ( %s )" % ( ", ".join( [ f[0] for f in sqlInsert ] ),
                                                                         ", ".join( [ str( f[1] ) for f in sqlInsert ] ) )
    result = self._update( insertSQL, conn = connObj )
    if result[ 'OK' ]:
      return result
    #If error and not duplicate -> real error
    if result[ 'Message' ].find( "Duplicate entry" ) == -1:
      return result
    updateSQL = "UPDATE `up_ProfilesData` SET %s WHERE %s" % ( ", ".join( [ "%s=%s" % f for f in sqlInsertValues ] ),
                                                               self.__webProfileUserDataCond( userIds,
                                                                                              sqlProfileName,
                                                                                              sqlVarName ) )
    return self._update( updateSQL, conn = connObj )

  def setUserVarPermsById( self, userIds, profileName, varName, perms ):

    result = self._escapeString( profileName )
    if not result[ 'OK' ]:
      return result
    sqlProfileName = result[ 'Value' ]

    result = self._escapeString( varName )
    if not result[ 'OK' ]:
      return result
    sqlVarName = result[ 'Value' ]

    nPerms = self.__parsePerms( perms, False )
    if not nPerms:
      return S_OK()
    sqlPerms = ",".join( [ "%s='%s'" % ( k, nPerms[k] ) for k in nPerms ] )

    updateSql = "UPDATE `up_ProfilesData` SET %s WHERE %s" % ( sqlPerms,
                                                               self.__webProfileUserDataCond( userIds,
                                                                                              sqlProfileName,
                                                                                              sqlVarName ) )
    return self._update( updateSql )

  def retrieveVar( self, userName, userGroup, ownerName, ownerGroup, profileName, varName, connObj = False ):
    """
    Get a data entry for a profile
    """
    result = self.getUserGroupIds( userName, userGroup )
    if not result[ 'OK' ]:
      return result
    userIds = result[ 'Value' ]

    result = self.getUserGroupIds( ownerName, ownerGroup )
    if not result[ 'OK' ]:
      return result
    ownerIds = result[ 'Value' ]

    return self.retrieveVarById( userIds, ownerIds, profileName, varName, connObj )

  def retrieveUserProfiles( self, userName, userGroup, connObj = False ):
    """
    Helper for getting data
    """
    result = self.getUserGroupIds( userName, userGroup )
    if not result[ 'OK' ]:
      return result
    userIds = result[ 'Value' ]
    return self.retrieveUserProfilesById( userIds, connObj )

  def retrieveAllUserVars( self, userName, userGroup, profileName, connObj = False ):
    """
    Helper for getting data
    """
    result = self.getUserGroupIds( userName, userGroup )
    if not result[ 'OK' ]:
      return result
    userIds = result[ 'Value' ]
    return self.retrieveAllUserVarsById( userIds, profileName, connObj )

  def retrieveVarPerms( self, userName, userGroup, ownerName, ownerGroup, profileName, varName, connObj = False ):
    result = self.getUserGroupIds( userName, userGroup )
    if not result[ 'OK' ]:
      return result
    userIds = result[ 'Value' ]

    result = self.getUserGroupIds( ownerName, ownerGroup, False )
    if not result[ 'OK' ]:
      return result
    ownerIds = result[ 'Value' ]

    return self.retrieveVarPermsById( userIds, ownerIds, profileName, varName, connObj )

  def setUserVarPerms( self, userName, userGroup, profileName, varName, perms ):
    result = self.getUserGroupIds( userName, userGroup )
    if not result[ 'OK' ]:
      return result
    userIds = result[ 'Value' ]
    return self.setUserVarPermsById( userIds, profileName, varName, perms )

  def storeVar( self, userName, userGroup, profileName, varName, data, perms = {} ):
    """
    Helper for setting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserGroupIds( userName, userGroup )
      if not result[ 'OK' ]:
        return result
      userIds = result[ 'Value' ]
      return self.storeVarByUserId( userIds, profileName, varName, data, perms = perms, connObj = connObj )
    finally:
      connObj.close()

  def deleteVar( self, userName, userGroup, profileName, varName ):
    """
    Helper for deleteting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserGroupIds( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userIds = result[ 'Value' ]
      return self.deleteVarByUserId( userIds, profileName, varName, connObj = connObj )
    finally:
      connObj.close()

  def __profilesCondGenerator( self, value, varType, initialValue = False ):
    if type( value ) in types.StringTypes:
      value = [ value ]
    ids = []
    if initialValue:
      ids.append( initialValue )
    for val in value:
      if varType == 'user':
        result = self.__getUserId( val, insertIfMissing = False )
      elif varType == 'group':
        result = self.__getGroupId( val, insertIfMissing = False )
      else:
        result = self.__getVOId( val, insertIfMissing = False )
      if not result[ 'OK' ]:
          continue
      ids.append( result[ 'Value' ] )
    if varType == 'user':
      fieldName = 'UserId'
    elif varType == 'group':
      fieldName = 'GroupId'
    else:
      fieldName = 'VOId'
    return "`up_ProfilesData`.%s in ( %s )" % ( fieldName, ", ".join( [ str( id ) for id in ids ] ) )


  def listVarsById( self, userIds, profileName, filterDict = {} ):
    result = self._escapeString( profileName )
    if not result[ 'OK' ]:
      return result
    sqlProfileName = result[ 'Value' ]
    sqlCond = [ "`up_Users`.Id = `up_ProfilesData`.UserId",
                "`up_Groups`.Id = `up_ProfilesData`.GroupId",
                "`up_VOs`.Id = `up_ProfilesData`.VOId",
                self.__webProfileReadAccessDataCond( userIds, userIds, sqlProfileName ) ]
    if filterDict:
      fD = {}
      for k in filterDict:
        fD[ k.lower() ] = filterDict[ k ]
      filterDict = fD
      for k in ( 'user', 'group', 'vo' ):
        if k in filterDict:
          sqlCond.append( self.__profilesCondGenerator( filterDict[ k ], k ) )

    sqlVars2Get = [ "`up_Users`.UserName", "`up_Groups`.UserGroup", "`up_VOs`.VO", "`up_ProfilesData`.VarName" ]
    sqlQuery = "SELECT %s FROM `up_Users`, `up_Groups`, `up_VOs`, `up_ProfilesData` WHERE %s" % ( ", ".join( sqlVars2Get ),
                                                                                        " AND ".join( sqlCond ) )

    return self._query( sqlQuery )

  def listVars( self, userName, userGroup, profileName, filterDict = {} ):
    result = self.getUserGroupIds( userName, userGroup )
    if not result[ 'OK' ]:
      return result
    userIds = result[ 'Value' ]
    return self.listVarsById( userIds, profileName, filterDict )

  def storeHashTagById( self, userIds, tagName, hashTag = False, connObj = False ):
    """
    Set a data entry for a profile
    """
    if not hashTag:
      hashTag = md5.md5()
      hashTag.update( "%s;%s;%s" % ( Time.dateTime(), userIds, tagName ) )
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
    insertSQL = "INSERT INTO `up_HashTags` ( UserId, GroupId, VOId, TagName, HashTag ) VALUES ( %s, %s, %s, %s, %s )" % ( userIds[0], userIds[1], userIds[2], tagName, hashTag )
    result = self._update( insertSQL, conn = connObj )
    if result[ 'OK' ]:
      return S_OK( hashTagUnescaped )
    #If error and not duplicate -> real error
    if result[ 'Message' ].find( "Duplicate entry" ) == -1:
      return result
    updateSQL = "UPDATE `up_HashTags` set HashTag=%s WHERE UserId = %s AND GroupId = %s AND VOId = %s AND TagName = %s" % ( hashTag, userIds[0], userIds[1], userIds[2], tagName )
    result = self._update( updateSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    return S_OK( hashTagUnescaped )

  def retrieveHashTagById( self, userIds, hashTag, connObj = False ):
    """
    Get a data entry for a profile
    """
    result = self._escapeString( hashTag )
    if not result[ 'OK' ]:
      return result
    hashTag = result[ 'Value' ]
    selectSQL = "SELECT TagName FROM `up_HashTags` WHERE UserId = %s AND GroupId = %s AND VOId = %s AND HashTag = %s" % ( userIds[0], userIds[1], userIds[2], hashTag )
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      return S_OK( data[0][0] )
    return S_ERROR( "No data for combo userId %s hashTag %s" % ( userIds, hashTag ) )

  def retrieveAllHashTagsById( self, userIds, connObj = False ):
    """
    Get a data entry for a profile
    """
    selectSQL = "SELECT HashTag, TagName FROM `up_HashTags` WHERE UserId = %s AND GroupId = %s AND VOId = %s" % userIds
    result = self._query( selectSQL, conn = connObj )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    return S_OK( dict( data ) )

  def storeHashTag( self, userName, userGroup, tagName, hashTag = False ):
    """
    Helper for deleteting data
    """
    result = self._getConnection()
    if not result[ 'OK' ]:
      return result
    connObj = result[ 'Value' ]
    try:
      result = self.getUserGroupIds( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userIds = result[ 'Value' ]
      return self.storeHashTagById( userIds, tagName, hashTag, connObj = connObj )
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
      result = self.getUserGroupIds( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userIds = result[ 'Value' ]
      return self.retrieveHashTagById( userIds, hashTag, connObj = connObj )
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
      result = self.getUserGroupIds( userName, userGroup, connObj = connObj )
      if not result[ 'OK' ]:
        return result
      userIds = result[ 'Value' ]
      return self.retrieveAllHashTagsById( userIds, connObj = connObj )
    finally:
      connObj.close()
