'''
Created on May 4, 2015
@author: Corentin Berger
'''

import zlib
import json
from datetime import datetime, timedelta
# from DIRAC
from DIRAC import S_OK, gLogger, S_ERROR

from DIRAC.DataManagementSystem.Client.DataLogging.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DataLogging.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DataLogging.DLCompressedSequence import DLCompressedSequence
from DIRAC.DataManagementSystem.Client.DataLogging.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DataLogging.DLCaller import DLCaller
from DIRAC.DataManagementSystem.Client.DataLogging.DLSequenceAttributeValue import DLSequenceAttributeValue
from DIRAC.DataManagementSystem.Client.DataLogging.DLSequenceAttribute import DLSequenceAttribute
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodCall import DLMethodCall
from DIRAC.DataManagementSystem.Client.DataLogging.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodName import DLMethodName
from DIRAC.DataManagementSystem.Client.DataLogging.DLUserName import DLUserName
from DIRAC.DataManagementSystem.Client.DataLogging.DLGroup import DLGroup
from DIRAC.DataManagementSystem.Client.DataLogging.DLHostName import DLHostName
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.DataManagementSystem.private.DLDecoder import DLDecoder
from DIRAC.DataManagementSystem.Client.DataLogging.DLException import DLException
# from sqlalchemy
from sqlalchemy         import create_engine, Table, Column, MetaData, ForeignKey, Integer, String, DateTime, Enum, exc
from sqlalchemy.orm     import mapper, sessionmaker, relationship, aliased
from sqlalchemy.dialects.mysql import MEDIUMBLOB

# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()

# Description of the DLCompressedSequence table
dataLoggingCompressedSequenceTable = Table( 'DLCompressedSequence', metadata,
                   Column( 'compressedSequenceID', Integer, primary_key = True ),
                   Column( 'value', MEDIUMBLOB ),
                   Column( 'lastUpdate', DateTime, index = True ),
                   Column( 'status', Enum( 'Waiting', 'Ongoing', 'Done' ), server_default = 'Waiting', index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLCompressedSequence object to the dataLoggingCompressedSequenceTable
mapper( DLCompressedSequence, dataLoggingCompressedSequenceTable )

# Description of the DLFile table
dataLoggingFileTable = Table( 'DLFile', metadata,
                   Column( 'fileID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLFile object to the dataLoggingFileTable
mapper( DLFile, dataLoggingFileTable )

# Description of the DLUserName table
dataLoggingUserNameTable = Table( 'DLUserName', metadata,
                   Column( 'userNameID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLUserName object to the dataLoggingUserNameTable
mapper( DLUserName, dataLoggingUserNameTable )

# Description of the DLUserName table
dataLoggingGroupTable = Table( 'DLGroup', metadata,
                   Column( 'groupID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLUserName object to the dataLoggingUserNameTable
mapper( DLGroup, dataLoggingGroupTable )

# Description of the DLUserName table
dataLoggingHostNameTable = Table( 'DLHostName', metadata,
                   Column( 'hostNameID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLUserName object to the dataLoggingUserNameTable
mapper( DLHostName, dataLoggingHostNameTable )

# Description of the DLMethodName table
dataLoggingMethodNameTable = Table( 'DLMethodName', metadata,
                   Column( 'methodNameID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLMethodName object to the dataLoggingMethodNameTable
mapper( DLMethodName, dataLoggingMethodNameTable )

# Description of the DLStorageElement table
dataLoggingStorageElementTable = Table( 'DLStorageElement', metadata,
                   Column( 'storageElementID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLStorageElement object to the dataLoggingStorageElementTable
mapper( DLStorageElement, dataLoggingStorageElementTable )

# Description of the DLCaller table
dataLoggingCallerTable = Table( 'DLCaller', metadata,
                   Column( 'callerID', Integer, primary_key = True ),
                   Column( 'name', String( 255 ), unique = True, index = True ),
                   mysql_engine = 'InnoDB' )
# Map the DLCaller object to the dataLoggingCallerTable
mapper( DLCaller, dataLoggingCallerTable )

# Description of the DLAction table
dataLoggingActionTable = Table( 'DLAction', metadata,
                   Column( 'actionID', Integer, primary_key = True ),
                   Column( 'methodCallID', Integer, ForeignKey( 'DLMethodCall.methodCallID' ) ),
                   Column( 'fileID', Integer, ForeignKey( 'DLFile.fileID' ) ),
                   Column( 'status' , Enum( 'Successful', 'Failed', 'Unknown' ), server_default = 'Unknown' ),
                   Column( 'srcSEID', Integer, ForeignKey( 'DLStorageElement.storageElementID' ) ),
                   Column( 'targetSEID', Integer, ForeignKey( 'DLStorageElement.storageElementID' ) ),
                   Column( 'extra', String( 2048 ) ),
                   Column( 'errorMessage', String( 2048 ) ),
                   mysql_engine = 'InnoDB' )
# Map the DLAction object to the dataLoggingActionTable, with two foreign key constraints,
# and one relationship between attribute file and table DLFile
mapper( DLAction, dataLoggingActionTable,
        properties = { 'file' : relationship( DLFile , lazy = 'joined' ),
                      'srcSE' : relationship( DLStorageElement, foreign_keys = dataLoggingActionTable.c.srcSEID, lazy = 'joined' ),
                      'targetSE' : relationship( DLStorageElement, foreign_keys = dataLoggingActionTable.c.targetSEID, lazy = 'joined' )} )

# Description of the DLSequence table
dataLoggingSequenceTable = Table( 'DLSequence', metadata,
                   Column( 'sequenceID', Integer, primary_key = True ),
                   Column( 'callerID', Integer, ForeignKey( 'DLCaller.callerID' ) ),
                   Column( 'groupID', Integer, ForeignKey( 'DLGroup.groupID' ) ),
                   Column( 'userNameID', Integer, ForeignKey( 'DLUserName.userNameID' ) ),
                   Column( 'hostNameID', Integer, ForeignKey( 'DLHostName.hostNameID' ) ),
                   mysql_engine = 'InnoDB' )
# Map the DLSequence object to the dataLoggingSequenceTable with one relationship between attribute methodCalls and table DLMethodCall
# an other relationship between attribute attributesValues and table DLSequenceAttributeValue
# and one foreign key for attribute caller
mapper( DLSequence, dataLoggingSequenceTable, properties = { 'methodCalls' : relationship( DLMethodCall, lazy = 'joined' ),
                                                             'caller' : relationship( DLCaller, lazy = 'joined' ),
                                                             'group' : relationship( DLGroup, lazy = 'joined' ),
                                                             'userName' : relationship( DLUserName, lazy = 'joined' ),
                                                             'hostName' : relationship( DLHostName, lazy = 'joined' ),
                                                             'attributesValues': relationship( DLSequenceAttributeValue, lazy = 'joined' ) } )

# Description of the DLMethodCall table
dataLoggingMethodCallTable = Table( 'DLMethodCall', metadata,
                   Column( 'methodCallID', Integer, primary_key = True ),
                   Column( 'creationTime', DateTime ),
                   Column( 'methodNameID', Integer, ForeignKey( 'DLMethodName.methodNameID' ) ),
                   Column( 'parentID', Integer, ForeignKey( 'DLMethodCall.methodCallID' ) ),
                   Column( 'sequenceID', Integer, ForeignKey( 'DLSequence.sequenceID' ) ),
                   Column( 'rank', Integer ),
                   mysql_engine = 'InnoDB' )
# Map the DLMethodCall object to the dataLoggingMethodCallTable with one relationship between attribute children and table DLMethodCall
# one foreign key for attribute name on table DLMethodName
# and an other relationship between attribute actions and table DLAction
mapper( DLMethodCall, dataLoggingMethodCallTable  , properties = { 'children' : relationship( DLMethodCall, lazy = 'joined', join_depth = 2 ),
                                                                    'name': relationship( DLMethodName, lazy = 'joined' ),
                                                                    'actions': relationship( DLAction, lazy = 'joined' ) } )
# Description of the DLSequenceAttribute table
dataLoggingSequenceAttribute = Table( 'DLSequenceAttribute', metadata,
                   Column( 'sequenceAttributeID', Integer, primary_key = True ),
                   Column( 'name', String( 128 ) ),
                   mysql_engine = 'InnoDB' )
# Map the DLSequenceAttribute object to the dataLoggingSequenceAttribute
mapper( DLSequenceAttribute, dataLoggingSequenceAttribute )

# Description of the DLSequenceAttributeValue table
dataLoggingSequenceAttributeValue = Table( 'DLSequenceAttributeValue', metadata,
                   Column( 'sequenceID', Integer, ForeignKey( 'DLSequence.sequenceID' ), primary_key = True ),
                   Column( 'sequenceAttributeID', Integer, ForeignKey( 'DLSequenceAttribute.sequenceAttributeID' ), primary_key = True ),
                   Column( 'value', String( 128 ) ),
                   mysql_engine = 'InnoDB' )
# Map the DLSequenceAttributeValue object to the dataLoggingSequenceAttributeValue
# two foreign key on tables DLSequence and DLSequenceAttribute
mapper( DLSequenceAttributeValue, dataLoggingSequenceAttributeValue,
                      properties = { 'sequence' : relationship( DLSequence, lazy = 'joined' ),
                                     'sequenceAttribute' : relationship( DLSequenceAttribute, lazy = 'joined' ) } )

class DataLoggingDB( object ):

  def __getDBConnectionInfo( self, fullname ):
    """
      Collect from the CS all the info needed to connect to the DB.
      This should be in a base class eventually
    """

    result = getDBParameters( fullname )
    if not result[ 'OK' ]:
      raise Exception( 'Cannot get database parameters: %s' % result[ 'Message' ] )

    dbParameters = result[ 'Value' ]
    self.dbHost = dbParameters[ 'Host' ]
    self.dbPort = dbParameters[ 'Port' ]
    self.dbUser = dbParameters[ 'User' ]
    self.dbPass = dbParameters[ 'Password' ]
    self.dbName = dbParameters[ 'DBName' ]


  def __init__( self ):
    """
      init method

      :param self: self reference
    """
    self.f1 = open( '/tmp/insertionTime.txt', 'a' )
    self.f2 = open( '/tmp/betweenTime.txt', 'a' )

    self.log = gLogger.getSubLogger( 'DataLoggingDB' )
    # Initialize the connection info
    self.__getDBConnectionInfo( 'DataManagement/DataLoggingDB' )

    runDebug = ( gLogger.getLevel() == 'DEBUG' )
    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s' % ( self.dbUser, self.dbPass, self.dbHost, self.dbPort, self.dbName ),
                                 echo = runDebug )

    metadata.bind = self.engine
    self.DBSession = sessionmaker( bind = self.engine, autoflush = False, expire_on_commit = False )

    # this dictionaries will serve to save object from database, like that we don't need to do a select all the time for the same object
    self.dictStorageElement = {None:None}
    self.dictFile = {None:None}
    self.dictMethodName = {None:None}
    self.dictCaller = {None:None}
    self.dictUserName = {None:None}
    self.dictHostName = {None:None}
    self.dictGroup = {None:None}
    self.dictSequenceAttribute = {None:None}


  def createTables( self ):
    """ create tables """
    try:
      metadata.create_all( self.engine )
    except Exception, e:
      gLogger.error( "createTables: unexpected exception %s" % e )
      return S_ERROR( "createTables: unexpected exception %s" % e )
    return S_OK()


  def cleanExpiredCompressedSequence( self, expirationTime = 1440 ):
    """
      this method check if the last update of some Compressed Sequence are not older than maxTime ago and if their status is at Ongoing
      if both, we change the status at Waiting

      :param expirationTime, a number of minute
    """
    session = None
    currentTime = datetime.utcnow()
    start = currentTime - timedelta( minutes = expirationTime )
    try:
      session = self.DBSession()
      rows = session.query( DLCompressedSequence )\
                .filter( DLCompressedSequence.status == 'Ongoing', DLCompressedSequence.lastUpdate <= start )\
                .with_for_update().all()
      if rows:
        # if we found some DLCompressedSequence, we change their status
        gLogger.info( "DataLoggingDB.cleanStaledSequencesStatus found %s sequences with status Ongoing since %s minutes, try to insert them"
                       % ( len( rows ), expirationTime ) )
        for sequenceCompressed in rows :
          sequenceCompressed.status = 'Waiting'
          sequenceCompressed.lastUpdate = datetime.now()
          session.merge( sequenceCompressed )
        session.commit()
      else :
        gLogger.info( "DataLoggingDB.cleanStaledSequencesStatus found 0 sequence with status Ongoing" )
        return S_OK( "no sequence to insert" )
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "cleanStaledSequencesStatus: unexpected exception %s" % e )
      raise DLException( "cleanStaledSequencesStatus: unexpected exception %s" % e )
    finally:
      session.close()
    return S_OK()



  def insertCompressedSequence( self, sequence ):
    """
      insert a new compressed sequence
      :param sequence, sequence is a DLSequence JSON representation which is compressed
    """
    session = None
    sequence = DLCompressedSequence( sequence )
    try:
      session = self.DBSession()
      session.add( sequence )
      session.commit()
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "insertCompressedSequence: unexpected exception %s" % e )
      raise DLException( "insertCompressedSequence: unexpected exception %s" % e )
    finally:
      if session :
        session.close()
    return S_OK()


  def moveSequences( self , maxSequenceToMove = 100 ):
    """
      move DLCompressedSequence in DLSequence
      selection of a number of maxSequence DLCompressedSequence in DB
      Update status of them to say that we are trying to insert them
      Trying to insert them
      Update status to say that the insertion is done

      :param maxSequenceToMove: the number of sequences to move per call of this method
    """

    # flush of all dictionaries
    self.dictStorageElement = {None:None}
    self.dictFile = {None:None}
    self.dictMethodName = {None:None}
    self.dictCaller = {None:None}
    self.dictUserName = {None:None}
    self.dictHostName = {None:None}
    self.dictGroup = {None:None}
    self.dictSequenceAttribute = {None:None}

    session = None
    sequences = {}
    beginMove = datetime.utcnow()
    try:
      session = self.DBSession()
      # selection of DLCompressedSequence with status 'Waiting' with a lock on rows that we are trying to select
      rows = session.query( DLCompressedSequence ).filter( DLCompressedSequence.status == 'Waiting' )\
          .order_by( DLCompressedSequence.lastUpdate ).with_for_update().limit( maxSequenceToMove )
      if rows:
        # if we have found some
        for sequenceCompressed in rows :

          sequenceJSON = zlib.decompress( sequenceCompressed.value )
          # decode of the JSON
          sequence = json.loads( sequenceJSON , cls = DLDecoder )
          # we save in sequences dictionary
          sequences[sequenceCompressed] = sequence
          # status update to Ongoing for each DLCompressedSequence
          sequenceCompressed.status = 'Ongoing'
          # we update the lastUpdate value
          sequenceCompressed.lastUpdate = datetime.now()
          session.merge( sequenceCompressed )
        session.commit()

        self.getOrCreateMultiple( session, sequences.values() )

        # we run through items of sequences dictionary
        for sequenceCompressed, sequence in sequences.items() :
          # we set the different attributes with the object get from Data Base
          if sequence.caller:
            sequence.caller = self.dictCaller[sequence.caller.name]

          if sequence.group:
            sequence.group = self.dictGroup[sequence.group.name]

          if sequence.userName:
            sequence.userName = self.dictUserName[sequence.userName.name]

          if sequence.hostName:
            sequence.hostName = self.dictHostName[sequence.hostName.name]

          for mc in sequence.methodCalls :
            if mc.name :
              mc.name = self.dictMethodName[mc.name.name]

            for action in mc.actions :
              if action.file :
                action.file = self.dictFile[action.file.name]

              if action.targetSE:
                action.targetSE = self.dictStorageElement[action.targetSE.name]

              if action.srcSE:
                action.srcSE = self.dictStorageElement[action.srcSE.name]

          try :
            # put sequence into db
            ret = self.__putSequence( session, sequence )
            if not ret['OK']:
              return S_ERROR( ret['Value'] )
            # update of status and lastUpdate
            sequenceCompressed.lastUpdate = datetime.now()
            sequenceCompressed.status = 'Done'
            session.merge( sequenceCompressed )
          except Exception, e:
            gLogger.error( "moveSequences: unexpected exception %s" % e )
            session.rollback()
            # if there is an error we try to insert sequence one by one
            res = self.moveSequencesOneByOne( session, sequences )
            if not res['OK']:
              return res
        session.commit()
      else :
        return S_OK( "no sequence to insert" )
    except Exception, e :
      session.rollback()
      for sequenceCompressed in sequences :
        # status update to Ongoing for each DLCompressedSequence
        sequenceCompressed.status = 'Waiting'
        # we update the lastUpdate value
        sequenceCompressed.lastUpdate = datetime.now()
        session.merge( sequenceCompressed )
      session.commit()
      gLogger.error( "moveSequences: unexpected exception %s" % e )
      raise DLException( "moveSequences: unexpected exception %s" % e )
    finally:
      session.close()
    endMove = datetime.utcnow()
    gLogger.info( "DataLoggingDB.moveSequences, move %s sequences in %s" % ( len( sequences ), ( endMove - beginMove ) ) )
    return S_OK()

  def moveSequencesOneByOne( self, session, sequences ):
    """
      move DLCompressedSequence in DLSequence
      sequences is a list of DLSequence
      Trying to insert a sequence
      Update its status to say that the insertion is done
      We dot that for each sequence in sequences

      :param session: a database session
      :param sequences: a list of DLSequence

    """
    for sequenceCompressed, sequence in sequences.items() :

      if sequence.caller:
        sequence.caller = self.dictCaller[sequence.caller.name]

      if sequence.group:
        sequence.group = self.dictGroup[sequence.group.name]

      if sequence.userName:
        sequence.userName = self.dictUserName[sequence.userName.name]

      if sequence.hostName:
        sequence.hostName = self.dictHostName[sequence.hostName.name]

      for mc in sequence.methodCalls :
        if mc.name :
          mc.name = self.dictMethodName[mc.name.name]

        for action in mc.actions :
          if action.file :
            action.file = self.dictFile[action.file.name]

          if action.targetSE:
            action.targetSE = self.dictStorageElement[action.targetSE.name]

          if action.srcSE:
            action.srcSE = self.dictStorageElement[action.srcSE.name]

      try :
        ret = self.__putSequence( session, sequence )
        if not ret['OK']:
          return S_ERROR( ret['Value'] )
        sequenceCompressed.lastUpdate = datetime.now()
        sequenceCompressed.status = 'Done'
        session.merge( sequenceCompressed )
        session.commit()
      except Exception, e:
        gLogger.error( "moveSequencesOneByOne: unexpected exception %s" % e )
        session.rollback()
        sequenceCompressed.lastUpdate = datetime.now()
        sequenceCompressed.status = 'Waiting'
        session.merge( sequenceCompressed )
        session.commit()
    return S_OK()


  def insertSequenceDirectly( self, sequence ):
    """
      this method insert a sequence JSON compressed directly into database, as a DLSequence and not as a DLCompressedSequence

      :param sequence: a DLSequence
    """
    session = None
    try:
      session = self.DBSession()
      self.getOrCreateMultiple( session, [sequence] )

      if sequence.caller:
          sequence.caller = self.dictCaller[sequence.caller.name]

      if sequence.group:
        sequence.group = self.dictGroup[sequence.group.name]

      if sequence.userName:
        sequence.userName = self.dictUserName[sequence.userName.name]

      if sequence.hostName:
        sequence.hostName = self.dictHostName[sequence.hostName.name]

      for mc in sequence.methodCalls :
        if mc.name :
          mc.name = self.dictMethodName[mc.name.name]

        for action in mc.actions :
          if action.file :
            action.file = self.dictFile[action.file.name]

          if action.targetSE:
            action.targetSE = self.dictStorageElement[action.targetSE.name]

          if action.srcSE:
            action.srcSE = self.dictStorageElement[action.srcSE.name]


      ret = self.__putSequence( session, sequence )
      if not ret['OK']:
        return S_ERROR( ret['Value'] )
      session.commit()
    except Exception, e:
      if session :
        session.rollback()
      gLogger.error( "insertSequenceDirectly: unexpected exception %s" % e )
      raise DLException( "insertSequenceDirectly: unexpected exception %s" % e )
    finally:
      session.close()
    return S_OK()


  def __putSequence( self, session, sequence ):
    """
      put a sequence into database

      :param session: a database session
      :param sequence: a DLSequence

    """
    try:
      for key, value in sequence.extra.items():
        sav = DLSequenceAttributeValue( value )
        sav.sequence = sequence
        res = self.getOrCreate( session, DLSequenceAttribute, key, self.dictSequenceAttribute )
        if not res['OK']:
          return res
        sav.sequenceAttribute = res['Value']
        sequence.attributesValues.append( sav )

      session.merge( sequence )

    except Exception, e:
      gLogger.error( "putSequence: unexpected exception %s" % e )
      raise DLException( "putSequence: unexpected exception %s" % e )
    return S_OK()

  def getOrCreate( self, session, model, value, objDict ):
    """
      get or create a database object

      :param session: a database session
      :param model: the model of object
      :param value, the value to get from DB
      :param objDict, the dictionary where object of model are saved

    """
    try:
      if value is None :
        return S_OK( None )
      elif value not in objDict :
        # select to know if the object is already in database
        instance = session.query( model ).filter_by( name = value ).first()
        if not instance:
          # if the object is not in db, we insert it
          instance = model( value )
          session.add( instance )
          session.commit()
        objDict[value] = instance
        session.expunge( instance )
      return  S_OK( objDict[value] )
    except exc.IntegrityError as e :
      gLogger.info( "IntegrityError: %s" % e )
      session.rollback()
      instance = session.query( model ).filter_by( name = value ).first()
      objDict[value] = instance
      session.expunge( instance )
      return S_OK( instance )
    except Exception, e:
      session.rollback()
      gLogger.error( "getOrCreate: unexpected exception %s" % e )
      return S_ERROR( "getOrCreate: unexpected exception %s" % e )

  def getOrCreateMultiple( self, session, sequences ):
    # different sets to save attributes names
    fileNames = set()
    storageNames = set()
    methodNames = set()
    callerNames = set()
    groupNames = set()
    hostNames = set()
    userNames = set()

    # we run through values of sequences dictionary to save different values to get from databse
    for sequence in sequences :
      # if the attribute is not none we add his name value into the set corresponding to the name of the attribute
      callerNames.add( '%s' % sequence.caller.name if sequence.caller else None )
      groupNames.add( '%s' % sequence.group.name if sequence.group else None )
      userNames.add( '%s' % sequence.userName.name if sequence.userName else None )
      hostNames.add( '%s' % sequence.hostName.name if sequence.hostName else None )
      for mc in sequence.methodCalls :
        methodNames.add( '%s' % mc.name.name if mc.name else None )
        for action in mc.actions :
          fileNames.add( '%s' % action.file.name if action.file else None )
          storageNames.add( '%s' % action.targetSE.name if action.targetSE else None )
          storageNames.add( '%s' % action.srcSE.name if action.srcSE else None )

    # calls of getOrCreate multiple with the different sets
    self._getOrCreateMultiple( session, DLCaller, callerNames, self.dictCaller )
    self._getOrCreateMultiple( session, DLGroup, groupNames, self.dictGroup )
    self._getOrCreateMultiple( session, DLUserName, userNames, self.dictUserName )
    self._getOrCreateMultiple( session, DLHostName, hostNames, self.dictHostName )
    self._getOrCreateMultiple( session, DLMethodName, methodNames, self.dictMethodName )
    self._getOrCreateMultiple( session, DLFile, fileNames, self.dictFile )
    self._getOrCreateMultiple( session, DLStorageElement, storageNames, self.dictStorageElement )


  def _getOrCreateMultiple( self, session, model, values, objDict ):
    """
      get or create a database object

      :param session: a database session
      :param model: the model of object
      :param values, a set of values to get from DB
      :param objDict, the dictionary where object of model are saved

    """
    try:
      # we retrieve the keys in values present in objDict
      # if key is in objDict, thats means that we already have the object from data base
      values -= set( objDict.keys() )
      # select to know if values are already in database
      instances = session.query( model.name, model ).filter( model.name.in_( values ) ).all()
      for name, obj in instances :
        # we found some values, we save them into the dictionary
        objDict[name] = obj
        values.remove( name )
      if values :
        # we have some values which are not into data base and not into objDict, we need to insert them
        for val in values :
          instance = model( val )
          session.add( instance )
          objDict[val] = instance
        session.commit()
      session.expunge_all()
      return  S_OK()
    except exc.IntegrityError as e :
      gLogger.info( "IntegrityError: %s" % e )
      session.rollback()
      for val in values :
        self.getOrCreate( session, model, val, objDict )
      return S_OK()
    except Exception, e:
      session.rollback()
      gLogger.error( "getOrCreate: unexpected exception %s" % e )
      return S_ERROR( "getOrCreate: unexpected exception %s" % e )


  def getSequence( self, lfn = None, callerName = None, before = None, after = None, status = None, extra = None,
                     userName = None, hostName = None, group = None ):
    """
      get all sequence about some criteria

      :param fileName, name of a file
      :param callerName, a caller name
      :param before, a date
      :param after, a date
      :param status, a str in [ Failed, Successful, Unknown ], can be None
      :param extra, a list of tuple [ ( extraArgsName1, value1 ), ( extraArgsName2, value2 ) ]
      :param userName, a DIRAC user name
      :param hostName, an host name
      :param group, a DIRAC group

      :return seqs: a list of DLSequence
    """
    targetSE_alias = aliased( DLStorageElement )
    session = self.DBSession()
    query = session.query( DLSequence )\
              .outerjoin( DLCaller )\
              .outerjoin( DLUserName )\
              .outerjoin( DLGroup )\
              .outerjoin( DLHostName )\
              .outerjoin( DLMethodCall )\
              .outerjoin( DLMethodName )\
              .outerjoin( DLAction )\
              .outerjoin( DLFile )\
              .outerjoin( DLAction.srcSE )\
              .outerjoin( targetSE_alias, DLAction.targetSE )\
              .order_by( DLMethodCall.creationTime )

    if lfn :
      query = query.filter( DLFile.name == lfn )
    if callerName :
      query = query.filter( DLCaller.name == callerName )
    if userName:
      query = query.filter( DLUserName.name == userName )
    if group :
      query = query.filter( DLGroup.name == group )
    if hostName:
      query = query.filter( DLHostName.name == hostName )

    if before and after :
      query = query.filter( DLMethodCall.creationTime.between( after, before ) )
    elif before :
      query = query.filter( DLMethodCall.creationTime <= before )
    elif after :
      query = query.filter( DLMethodCall.creationTime >= after )


    if status :
      query = query.filter( DLAction.status == status )

    if extra :
      extra = extra.split()
      query = query.join( DLSequenceAttributeValue )\
                   .join( DLSequenceAttribute )
      for i in range( len( extra ) / 2 ) :
        query = query.filter( DLSequenceAttribute.name.like( extra[i * 2] ) )\
                     .filter( DLSequenceAttributeValue.value == extra[i * 2 + 1] )
    try :
      seqs = query.distinct( DLSequence.sequenceID ).limit( 500 ).all()
      if seqs :
        for seq in seqs :
          seq.extra = {}
          # here we get the value and name of specific columns of this sequence into extra dictionary
          for av in seq.attributesValues :
            seq.extra[av.sequenceAttribute.name] = av.value
    except Exception, e:
      gLogger.error( "getSequenceOnFile: unexpected exception %s" % e )
      return S_ERROR( "getSequenceOnFile: unexpected exception %s" % e )

    finally:
      session.expunge_all()
      session.close()
    return S_OK( seqs )

  def getSequenceByID( self, IDSeq ):
    """
      get the sequence for the id IDSeq

      :param IDSeq, an id of a sequence

      :return seqs: a list with one DLSequence
    """
    targetSE_alias = aliased( DLStorageElement )
    session = self.DBSession()
    query = session.query( DLSequence )\
              .outerjoin( DLCaller )\
              .outerjoin( DLUserName )\
              .outerjoin( DLGroup )\
              .outerjoin( DLHostName )\
              .outerjoin( DLMethodCall )\
              .outerjoin( DLMethodName )\
              .outerjoin( DLAction )\
              .outerjoin( DLFile )\
              .outerjoin( DLAction.srcSE )\
              .outerjoin( targetSE_alias, DLAction.targetSE )
    try:
      seqs = query.filter( DLSequence.sequenceID == IDSeq ).all()
      if seqs :
        for seq in seqs :
          seq.extra = {}
          # here we get the value and name of specific columns of this sequence into extra dictionary
          for av in seq.attributesValues :
            seq.extra[av.sequenceAttribute.name] = av.value
    except Exception, e:
      gLogger.error( "getSequenceOnFile: unexpected exception %s" % e )
      return S_ERROR( "getSequenceOnFile: unexpected exception %s" % e )

    finally:
      session.expunge_all()
      session.close()
    return S_OK( seqs )


  def getMethodCallOnFile( self, lfn, before, after, status ):
    """
      get all operation about a file's name, before and after are date

      :param lfn, a lfn name
      :param before, a date, can be None
      :param after, a date, can be None
      :param status, a str in [ Failed, Successful, Unknown ], can be None

      :return calls: a list of DLMethodCall
    """
    targetSE_alias = aliased( DLStorageElement )
    session = self.DBSession()
    query = session.query( DLMethodCall )\
                .outerjoin( DLMethodName )\
                .outerjoin( DLAction )\
                .outerjoin( DLFile )\
                .outerjoin( DLAction.srcSE )\
                .outerjoin( targetSE_alias, DLAction.targetSE )\
                .filter( DLFile.name == lfn )\
                .order_by( DLMethodCall.creationTime )
    if before and after :
      query = query.filter( DLMethodCall.creationTime.between( after, before ) )
    elif before :
      query = query.filter( DLMethodCall.creationTime <= before )
    elif after :
      query = query.filter( DLMethodCall.creationTime >= after )

    if status :
      query = query.filter( DLAction.status == status )

    try:
      calls = query.distinct( DLMethodCall.methodCallID ).limit( 1000 )
    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.expunge_all()
      session.close()

    return S_OK( calls )

  def getMethodCallByName( self, name, before, after, status ):
    """
      get all operation about a method call name

      :param name, a method name
      :param before, a date, can be None
      :param after, a date, can be None
      :param status, a str in [ Failed, Successful, Unknown ], can be None

      :return calls: a list of DLMethodCall
    """
    targetSE_alias = aliased( DLStorageElement )
    session = self.DBSession()
    query = session.query( DLMethodCall )\
                .outerjoin( DLMethodName )\
                .outerjoin( DLAction )\
                .outerjoin( DLFile )\
                .outerjoin( DLAction.srcSE )\
                .outerjoin( targetSE_alias, DLAction.targetSE )\
                .filter( DLMethodName.name == name )\
                .order_by( DLMethodCall.creationTime )

    if before and after :
      query = query.filter( DLMethodCall.creationTime.between( after, before ) )
    elif before :
      query = query.filter( DLMethodCall.creationTime <= before )
    elif after :
      query = query.filter( DLMethodCall.creationTime >= after )

    if status :
      query = query.filter( DLAction.status == status )

    try:
      calls = query.distinct( DLMethodCall.methodCallID ).limit( 1000 )
    except Exception, e:
      gLogger.error( "getLFNOperation: unexpected exception %s" % e )
      return S_ERROR( "getLFNOperation: unexpected exception %s" % e )

    finally:
      session.expunge_all()
      session.close()

    return S_OK( calls )
