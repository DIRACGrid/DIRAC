########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/DB/Attic/SandboxMetadataDB.py,v 1.1 2009/04/28 16:25:09 acasajus Exp $
########################################################################
""" SandboxMetadataDB class is a front-end to the metadata for sandboxes
"""

__RCSID__ = "$Id: SandboxMetadataDB.py,v 1.1 2009/04/28 16:25:09 acasajus Exp $"

import time
import types
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import List
from DIRAC.Core.Security import Properties, CS

class SandboxMetadataDB(DB):

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'SandboxMetadataDB', 'DataManagement/SandboxMetadataDB', maxQueueSize )
    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % result[ 'Message' ])
    self.__assignedSBGraceDays = 0
    self.__unassignedSBGraceDays = 15

  def __initializeDB(self):
    """
    Create the tables
    """
    result = self._query( "show tables" )
    if not result[ 'OK' ]:
      return result

    tablesInDB = [ t[0] for t in result[ 'Value' ] ]
    tablesToCreate = {}
    self.__tablesDesc = {}

    self.__tablesDesc[ 'sb_Owners' ] = { 'Fields' : { 'OwnerId' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                      'Owner' : 'VARCHAR(32) NOT NULL',
                                                      'OwnerDN' : 'VARCHAR(255) NOT NULL',
                                                      'OwnerGroup' : 'VARCHAR(32) NOT NULL',
                                                    },
                                         'PrimaryKey' : 'OwnerId',
                                        }

    self.__tablesDesc[ 'sb_SandBoxes' ] = { 'Fields' : { 'SBId' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                         'OwnerId' : 'INTEGER UNSIGNED NOT NULL',
                                                         'SEName' : 'VARCHAR(64) NOT NULL',
                                                         'SEPFN' : 'VARCHAR(512) NOT NULL',
                                                         'Bytes' : 'BIGINT NOT NULL DEFAULT 0',
                                                         'RegistrationTime' : 'DATETIME NOT NULL',
                                                         'LastAccessTime' : 'DATETIME NOT NULL',
                                                         'Assigned' : 'TINYINT NOT NULL DEFAULT 0',
                                                        },
                                            'PrimaryKey' : 'SBId',
                                            'Indexes': { 'SBOwner': [ 'OwnerId' ],
                                                       },
                                            'UniqueIndexes' : { 'Location' : [ 'SEName', 'SEPFN' ] }

                                          }

    self.__tablesDesc[ 'sb_EntityMapping' ] = { 'Fields' : { 'SBId' : 'INTEGER UNSIGNED NOT NULL',
                                                             'EntitySetup' : 'VARCHAR(64) NOT NULL',
                                                             'EntityId' : 'VARCHAR(128) NOT NULL',
                                                             'Type' : 'VARCHAR(64) NOT NULL',
                                                        },
                                             'Indexes': { 'Entity': [ 'EntityId', 'EntitySetup' ],
                                                          'SBIndex' : [ 'SBId' ]
                                                        },
                                           }

    for tableName in self.__tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.__tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

  def registerAndGetOwnerId( self, owner, ownerDN, ownerGroup ):
    """
    Get the owner ID and register it if it's not there
    """
    sqlCmd = "SELECT OwnerId FROM `sb_Owners` WHERE Owner='%s' AND OwnerDN='%s' AND OwnerGroup='%s'" % ( owner,
                                                                                                         ownerDN,
                                                                                                         ownerGroup )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 0:
      return S_OK( data[0][0] )
    #Its not there, insert it
    sqlCmd = "INSERT INTO `sb_Owners` ( OwnerId, Owner, OwnerDN, OwnerGroup ) VALUES ( 0, '%s', '%s', '%s' )" % ( owner,
                                                                                                                  ownerDN,
                                                                                                                  ownerGroup )
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      return result
    if 'lastRowId' in result:
      return S_OK( result[ 'lastRowId' ] )
    result = self._query( "SELECT LAST_INSERT_ID()" )
    if not result[ 'OK' ]:
      return S_ERROR( "Can't determine owner id after insertion" )
    return S_OK( result[ 'Value' ][0][0] )

  def registerAndGetSandbox( self, owner, ownerDN, ownerGroup, sbSE, sbPFN, size = 0 ):
    """
    Register a new sandbox in the metadata catalog
    Returns ( sbid, newSandbox )
    """
    result = self.registerAndGetOwnerId( owner, ownerDN, ownerGroup )
    if not result[ 'OK' ]:
      return result
    ownerId = result[ 'Value' ]
    sqlCmd = "INSERT INTO `sb_SandBoxes` ( SBId, OwnerId, SEName, SEPFN, Bytes, RegistrationTime, LastAccessTime )"
    sqlCmd = "%s VALUES ( 0, '%s', '%s', '%s', %d, UTC_TIMESTAMP(), UTC_TIMESTAMP() )" % ( sqlCmd, ownerId, sbSE,
                                                                                           sbPFN, size )
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      if result[ 'Message' ].find( "Duplicate entry" ) == -1 :
        return result
      #It's a duplicate, try to retrieve sbid
      sqlCond = [ "SEPFN='%s'" % sbPFN, "SEName='%s'" % sbSE, "OwnerId='%s'" % ownerId ]
      sqlCmd = "SELECT SBId FROM `sb_SandBoxes` WHERE %s" % " AND ".join( sqlCond )
      result = self._query( sqlCmd )
      if not result[ 'OK' ]:
        return result
      if len( result[ 'Value' ] ) == 0:
        return S_ERROR( "Location %s already exists but doesn't belong to the user or setup" )
      sbId = result[ 'Value' ][0][0]
      self.accessedSandboxById( sbId )
      return S_OK( ( sbId, False ) )
    #Inserted, time to get the id
    if 'lastRowId' in result:
      return S_OK( ( result['lastRowId'], True ) )
    result = self._query( "SELECT LAST_INSERT_ID()" )
    if not result[ 'OK' ]:
      return S_ERROR( "Can't determine sand box id after insertion" )
    return S_OK( ( result[ 'Value' ][0][0], True ) )


  def accessedSandboxById( self, sbId ):
    """
    Update last access time for sb id
    """
    return self.__accessedSandboxByCond( { 'SBId': sbId } )

  def accessedSandboxByLocation( self, seName, sePFN ):
    """
    Update last access time for location
    """
    return self.__accessedSandboxByCond( { 'SEName': self._escapeString( seName )[ 'Value' ],
                                           'SEPFN': self._escapeString( sePFN )[ 'Value' ],
                                          } )

  def __accessedSandboxByCond( self, condDict ):
    sqlCond = [ "%s=%s" % ( key, condDict[ key ] ) for key in condDict ]
    return self._update( "UPDATE `sb_SandBoxes` SET LastAccessTime=UTC_TIMESTAMP() WHERE %s" % " AND ".join( condDict ) )

  def assignSandboxesToEntities( self, entitiesToSandboxList, requesterName, requesterGroup ):
    """
    Assign jobs to entities
    """
    insertValues = []
    sbIds = []
    for entityId, entitySetup, SBType, SEName, SEPFN in entitiesToSandboxList:
      result = self.getSandboxId( SEName, SEPFN, requesterName, requesterGroup )
      if not result[ 'OK' ]:
        self.log.warn( "Cannot find id for %s:%s with requester %s@%s" % ( SEName, SEPFN, requesterName, requesterGroup ) )
      else:
        sbId = result['Value' ]
        sbIds.append( str( sbId ) )
        insertValues.append( "( %s, %s, %s, %d )" % ( self._escapeString( entityId )[ 'Value' ],
                                                      self._escapeString( entitySetup )[ 'Value' ],
                                                      self._escapeString( SBType )[ 'Value' ],
                                                      sbId ) )

    if not insertValues:
      return S_ERROR( "Sandbox does not exist or you're not authorized to assign it" )
    sqlCmd = "INSERT INTO `sb_EntityMapping` ( entityId, entitySetup, Type, SBId ) VALUES %s" % ", ".join( insertValues )
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      return result
    sqlCmd = "UPDATE `sb_SandBoxes` SET Assigned=1 WHERE SBId in ( %s )" % ", ".join( sbIds )
    return self._update( sqlCmd )

  def unassignEntities( self, entitiesDict ):
    """
    Unassign jobs to sandboxes
    entitiesDict = { 'setup' : [ 'entityId', 'entityId' ] }
    """
    for entitySetup in entitiesDict:
      entitiesIds = entitiesDict[ entitySetup ]
      if not entitiesIds:
        continue
      sqlCond = [ "EntitySetup = %s" % self._escapeString( entitySetup )[ 'Value' ] ]
      ids = []
      for entityId in entitiesIds:
        ids.append( self._escapeString( entityId )[ 'Value' ] )
      sqlCond.append( "EntityId in ( %s )" % ", ".join ( [ str(eid) for eid in ids ] ) )
      sqlCmd = "DELETE FROM `sb_EntityMapping` WHERE %s" % " AND ".join( sqlCond )
      result = self._update( sqlCmd )
      if not result[ 'OK' ]:
        gLogger.error( "Cannot unassign entities: %s" % result[ 'Message' ] )
    return S_OK()

  def getSandboxesAssignedToEntity( self, entityId, entitySetup, requesterName, requesterGroup ):
    """
    Get the sandboxes and the type of assignation to the jobId
    """
    sqlTables = [ "`sb_SandBoxes` s", "`sb_EntityMapping` e" ]
    sqlCond = [ "s.SBId = e.SBId",
               "e.EntityId = %s" % self._escapeString( entityId )[ 'Value' ],
               "e.EntitySetup = %s" % self._escapeString( entitySetup )[ 'Value' ] ]
    requesterProps = CS.getPropertiesForGroup( requesterGroup )
    if Properties.JOB_ADMINISTRATOR in requesterProps:
      #Do nothing, just ensure it doesn't fit in the other cases
      pass
    elif Properties.JOB_SHARING in requesterProps:
      sqlTables.append( "`sb_Owners` o" )
      sqlCond.append( "o.OwnerGroup='%s'" % requesterGroup )
      sqlCond.append( "s.OwnerId=o.OwnerId" )
    elif Properties.NORMAL_USER in requesterProps:
      sqlTables.append( "`sb_Owners` o" )
      sqlCond.append( "o.OwnerGroup='%s'" % requesterGroup )
      sqlCond.append( "o.Owner='%s'" % requesterName )
      sqlCond.append( "s.OwnerId=o.OwnerId" )
    else:
      return S_ERROR( "Not authorized to access sandbox" )
    sqlCmd = "SELECT DISTINCT s.SEName, s.SEPFN, e.Type FROM  %s WHERE %s" % ( ", ".join( sqlTables ),
                                                                               " AND ".join( sqlCond ) )
    return self._query( sqlCmd )

  def getUnusedSandboxes( self ):
    """
    Get sandboxes that have been assigned but the job is no longer there
    """
    sqlCond = [ "Assigned AND SBId NOT IN ( SELECT SBId FROM `sb_EntityMapping` ) AND TIMESTAMPDIFF( DAY, LastAccessTime, UTC_TIMESTAMP() ) >= %d" % self.__assignedSBGraceDays,
                "! Assigned AND TIMESTAMPDIFF( DAY, LastAccessTime, UTC_TIMESTAMP() ) >= %s" % self.__unassignedSBGraceDays]
    sqlCmd = "SELECT SBId, SEName, SEPFN FROM `sb_SandBoxes` WHERE ( %s )" % " ) OR ( ".join( sqlCond )
    return self._query( sqlCmd )

  def deleteSandboxes( self, SBIdList ):
    """
    Delete sandboxes
    """
    sqlSBList = ", ".join( [ str(sbid) for sbid in SBIdList ] )
    for table in ( 'sb_SandBoxes', 'sb_EntityMapping' ):
      sqlCmd = "DELETE FROM `%s` WHERE SBId IN ( %s )" % ( table, sqlSBList )
      result = self._update( sqlCmd )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def setLocation( self, SBId, location ):
    """
    Set the Location for a sandbox
    """
    return self._update( "UPDATE `sb_SandBoxes` SET Location='%s' WHERE SBId = %s" % ( location, SBId ) )

  def getSandboxId( self, SEName, SEPFN, requesterName, requesterGroup ):
    """
    Get the sandboxId if it exists
    """
    sqlCond = [ "s.SEPFN=%s" % self._escapeString( SEPFN )['Value'],
                "s.SEName=%s" % self._escapeString( SEName )['Value'],
                's.OwnerId=o.OwnerId' ]
    sqlCmd = "SELECT s.SBId FROM `sb_SandBoxes` s, `sb_Owners` o WHERE"
    requesterProps = CS.getPropertiesForGroup( requesterGroup )
    if Properties.JOB_ADMINISTRATOR in requesterProps:
      #Do nothing, just ensure it doesn't fit in the other cases
      pass
    elif Properties.JOB_SHARING in requesterProps:
      sqlCond.append( "o.OwnerGroup='%s'" % requesterGroup )
    elif Properties.NORMAL_USER in requesterProps:
      sqlCond.append( "o.OwnerGroup='%s'" % requesterGroup )
      sqlCond.append( "o.Owner='%s'" % requesterName )
    else:
      return S_ERROR( "Not authorized to access sandbox" )
    result = self._query( "%s %s" % ( sqlCmd, " AND ".join( sqlCond ) ) )
    if not result[ 'OK' ]:
      return result
    data = result[ 'Value' ]
    if len( data ) > 1:
      self.log.error( "More than one sandbox registered with the same Location!", location )
    if len( data ) == 0:
      return S_ERROR( "No sandbox matches the requirements" )
    return S_OK( data[0][0] )
