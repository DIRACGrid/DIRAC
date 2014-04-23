########################################################################
# $HeadURL$
# File: Operation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 12:12:05
########################################################################

""" :mod: Operation
    ===============

    .. module: Operation
    :synopsis: Operation implementation
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Operation implementation
"""
# for properties
# pylint: disable=E0211,W0612,W0142,E1101,E0102,C0103
__RCSID__ = "$Id$"
# #
# @file Operation.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/24 12:12:18
# @brief Definition of Operation class.
# # imports
import datetime
# # from DIRAC
from DIRAC import S_OK
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.private.Record import Record
from DIRAC.RequestManagementSystem.Client.File import File

########################################################################
class Operation( Record ):
  """
  .. class:: Operation

  :param long OperationID: OperationID as read from DB backend
  :param long RequestID: parent RequestID
  :param str Status: execution status
  :param str Type: operation to perform
  :param str Arguments: additional arguments
  :param str SourceSE: source SE name
  :param str TargetSE: target SE names as comma separated list
  :param str Catalog: catalog to use as comma separated list
  :param str Error: error string if any
  :param Request parent: parent Request instance
  """
  # # max files in a single operation
  MAX_FILES = 100

  # # all states
  ALL_STATES = ( "Queued", "Waiting", "Scheduled", "Assigned", "Failed", "Done", "Canceled" )
  # # final states
  FINAL_STATES = ( "Failed", "Done", "Canceled" )

  def __init__( self, fromDict = None ):
    """ c'tor

    :param self: self reference
    :param dict fromDict: attributes dictionary
    """
    Record.__init__( self )
    self._parent = None
    # # sub-request attributes
    # self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["SubmitTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["CreationTime"] = now
    self.__data__["OperationID"] = 0
    self.__data__["RequestID"] = 0
    self.__data__["Status"] = "Queued"

    # # operation files
    self.__files__ = TypedList( allowedTypes = File )
    # # dirty fileIDs
    self.__dirty = []

    # # init from dict
    fromDict = fromDict if fromDict else {}

    self.__dirty = fromDict.get( "__dirty", [] )
    if "__dirty" in fromDict:
      del fromDict["__dirty"]

    for fileDict in fromDict.get( "Files", [] ):
      self.addFile( File( fileDict ) )
    if "Files" in fromDict:
      del fromDict["Files"]

    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown Operation attribute '%s'" % key )
      if key != "Order" and value:
        setattr( self, key, value )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "OperationID" : "INTEGER NOT NULL AUTO_INCREMENT",
               "RequestID" : "INTEGER NOT NULL",
               "Type" : "VARCHAR(64) NOT NULL",
               "Status" : "ENUM('Waiting', 'Assigned', 'Queued', 'Done', 'Failed', 'Canceled', 'Scheduled') "\
                 "DEFAULT 'Queued'",
               "Arguments" : "MEDIUMBLOB",
               "Order" : "INTEGER NOT NULL",
               "SourceSE" : "VARCHAR(255)",
               "TargetSE" : "VARCHAR(255)",
               "Catalog" : "VARCHAR(255)",
               "Error": "VARCHAR(255)",
               "CreationTime" : "DATETIME",
               "SubmitTime" : "DATETIME",
               "LastUpdate" : "DATETIME" },
             'ForeignKeys': {'RequestID': 'Request.RequestID' },
             "PrimaryKey" : "OperationID" }

  # # protected methods for parent only
  def _notify( self ):
    """ notify self about file status change """
    fStatus = set( self.fileStatusList() )

    if fStatus == set( ['Failed'] ):
      # All files Failed -> Failed
      newStatus = 'Failed'
    elif "Waiting" in fStatus:
      newStatus = 'Queued'
    elif 'Scheduled' in fStatus:
      newStatus = 'Scheduled'
    elif 'Failed' in fStatus:
      newStatus = 'Failed'
    else:
      self.__data__['Error'] = ''
      newStatus = 'Done'

    self.__data__["Status"] = newStatus
    if self._parent:
      self._parent._notify()

  def _setQueued( self, caller ):
    """ don't touch """
    if caller == self._parent:
      self.__data__["Status"] = "Queued"

  def _setWaiting( self, caller ):
    """ don't touch as well """
    if caller == self._parent:
      self.__data__["Status"] = "Waiting"

  # # Files arithmetics
  def __contains__( self, opFile ):
    """ in operator """
    return opFile in self.__files__

  def __iadd__( self, opFile ):
    """ += operator """
    if len( self ) >= Operation.MAX_FILES:
      raise RuntimeError( "too many Files in a single Operation" )
    self.addFile( opFile )
    return self

  def addFile( self, opFile ):
    """ add :opFile: to operation """
    if len( self ) > Operation.MAX_FILES:
      raise RuntimeError( "too many Files in a single Operation" )
    if opFile not in self:
      self.__files__.append( opFile )
      opFile._parent = self
    self._notify()

  # # helpers for looping
  def __iter__( self ):
    """ files iterator """
    return self.__files__.__iter__()

  def __getitem__( self, i ):
    """ [] op for opFiles """
    return self.__files__.__getitem__( i )

  def __delitem__( self, i ):
    """ remove file from op, only if OperationID is NOT set """
    if not self.OperationID:
      self.__files__.__delitem__( i )
    else:
      if self[i].FileID:
        self.__dirty.append( self[i].FileID )
      self.__files__.__delitem__( i )
    self._notify()

  def __setitem__( self, i, opFile ):
    """ overwrite opFile """
    self.__files__._typeCheck( opFile )
    toDelete = self[i]
    if toDelete.FileID:
      self.__dirty.append( toDelete.FileID )
    self.__files__.__setitem__( i, opFile )
    opFile._parent = self
    self._notify()

  def fileStatusList( self ):
    """ get list of files statuses """
    return [ subFile.Status for subFile in self ]

  def __len__( self ):
    """ nb of subFiles """
    return len( self.__files__ )

  # # properties
  @property
  def RequestID( self ):
    """ RequestID getter (RO) """
    return self._parent.RequestID if self._parent else -1

  @RequestID.setter
  def RequestID( self, value ):
    """ can't set RequestID by hand """
    self.__data__["RequestID"] = self._parent.RequestID if self._parent else -1

  @property
  def OperationID( self ):
    """ OperationID getter """
    return self.__data__["OperationID"]

  @OperationID.setter
  def OperationID( self, value ):
    """ OperationID setter """
    self.__data__["OperationID"] = long( value ) if value else 0

  @property
  def Type( self ):
    """ operation type prop """
    return self.__data__["Type"]

  @Type.setter
  def Type( self, value ):
    """ operation type setter """
    self.__data__["Type"] = str( value )

  @property
  def Arguments( self ):
    """ arguments getter """
    return self.__data__["Arguments"]

  @Arguments.setter
  def Arguments( self, value ):
    """ arguments setter """
    self.__data__["Arguments"] = value if value else ""

  @property
  def SourceSE( self ):
    """ source SE prop """
    return self.__data__["SourceSE"] if self.__data__["SourceSE"] else ""

  @SourceSE.setter
  def SourceSE( self, value ):
    """ source SE setter """
    value = ",".join( self._uniqueList( value ) )
    if len( value ) > 256:
      raise ValueError( "SourceSE list too long" )
    self.__data__["SourceSE"] = str( value )[:255] if value else ""

  @property
  def sourceSEList( self ):
    """ helper property returning source SEs as a list"""
    return self.SourceSE.split( "," )

  @property
  def TargetSE( self ):
    """ target SE prop """
    return self.__data__["TargetSE"] if self.__data__["TargetSE"] else ""

  @TargetSE.setter
  def TargetSE( self, value ):
    """ target SE setter """
    value = ",".join( self._uniqueList( value ) )
    if len( value ) > 256:
      raise ValueError( "TargetSE list too long" )
    self.__data__["TargetSE"] = value[:255] if value else ""

  @property
  def targetSEList( self ):
    """ helper property returning target SEs as a list"""
    return self.TargetSE.split( "," )

  @property
  def Catalog( self ):
    """ catalog prop """
    return self.__data__["Catalog"]

  @Catalog.setter
  def Catalog( self, value ):
    """ catalog setter """
    value = ",".join( self._uniqueList( value ) )
    if len( value ) > 255:
      raise ValueError( "Catalog list too long" )
    self.__data__["Catalog"] = value if value else ""

  @property
  def catalogList( self ):
    """ helper property returning catalogs as list """
    return self.__data__["Catalog"].split( "," )

  @property
  def Error( self ):
    """ error prop """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, value ):
    """ error setter """
    if type( value ) != str:
      raise TypeError( "Error has to be a string!" )
    self.__data__["Error"] = self._escapeStr( value, 255 )

  @property
  def Status( self ):
    """ Status prop """
    return self.__data__["Status"]

  @Status.setter
  def Status( self, value ):
    """ Status setter """
    if value not in Operation.ALL_STATES:
      raise ValueError( "unknown Status '%s'" % str( value ) )
    if self.__files__:
      self._notify()
    else:
      self.__data__["Status"] = value
      if self._parent:
        self._parent._notify()
    if self.__data__['Status'] == 'Done':
      self.__data__['Error'] = ''

  @property
  def Order( self ):
    """ order prop """
    if self._parent:
      self.__data__["Order"] = self._parent.indexOf( self ) if self._parent else -1
    return self.__data__["Order"]

  @property
  def CreationTime( self ):
    """ operation creation time prop """
    return self.__data__["CreationTime"]

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["CreationTime"] = value

  @property
  def SubmitTime( self ):
    """ subrequest's submit time prop """
    return self.__data__["SubmitTime"]

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submit time setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "SubmitTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["SubmitTime"] = value

  @property
  def LastUpdate( self ):
    """ last update prop """
    return self.__data__["LastUpdate"]

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["LastUpdate"] = value

  def __str__( self ):
    """ str operator """
    return str( self.toJSON()["Value"] )

  def toSQL( self ):
    """ get SQL INSERT or UPDATE statement """
    if not getattr( self, "RequestID" ):
      raise AttributeError( "RequestID not set" )
    colVals = [ ( "`%s`" % column, "'%s'" % getattr( self, column )
                  if type( getattr( self, column ) ) in ( str, datetime.datetime ) else str( getattr( self, column ) ) )
                for column in self.__data__
                if getattr( self, column ) and column not in ( "OperationID", "LastUpdate", "Order" ) ]
    colVals.append( ( "`LastUpdate`", "UTC_TIMESTAMP()" ) )
    colVals.append( ( "`Order`", str( self.Order ) ) )
    # colVals.append( ( "`Status`", "'%s'" % str(self.Status) ) )
    query = []
    if self.OperationID:
      query.append( "UPDATE `Operation` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `OperationID`=%d;\n" % self.OperationID )
    else:
      query.append( "INSERT INTO `Operation` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;\n" % values )

    return S_OK( "".join( query ) )

  def cleanUpSQL( self ):
    """ query deleting dirty records from File table """
    if self.OperationID and self.__dirty:
      fIDs = ",".join( [ str( fid ) for fid in self.__dirty ] )
      return "DELETE FROM `File` WHERE `OperationID` = %s AND `FileID` IN (%s);\n" % ( self.OperationID, fIDs )

  def toJSON( self ):
    """ get json digest """
    digest = dict( zip( self.__data__.keys(),
                        [ str( val ) if val else "" for val in self.__data__.values() ] ) )
    digest["RequestID"] = str( self.RequestID )
    digest["Order"] = str( self.Order )
    if self.__dirty:
      digest["__dirty"] = self.__dirty
    digest["Files"] = []
    for opFile in self:
      opJSON = opFile.toJSON()
      if not opJSON["OK"]:
        return opJSON
      digest["Files"].append( opJSON["Value"] )

    return S_OK( digest )
