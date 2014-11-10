########################################################################
# $HeadURL$
# File: Operation.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/24 12:12:05
########################################################################

"""
:mod: Operation

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
from types import StringTypes
import json
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.private.RMSBase import RMSBase
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.private.JSONUtils import RMSEncoder

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Enum, BLOB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, validates
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.hybrid import hybrid_property


# def order_from_parent( context ):
#   print 'context %s' % context.compiled_parameters
#   orderFromparent = context.current_parameters['Order']
#   print 'order %s' % orderFromparent
#   return orderFromparent

########################################################################
class Operation( object ):
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
  
  _datetimeFormat = '%Y-%m-%d %H:%M:%S'


#   __tablename__ = 'Operation'

#   TargetSE = Column( String( 255 ) )
#   _CreationTime = Column( 'CreationTime', DateTime )
#   SourceSE = Column( String( 255 ) )
#   Arguments = Column( BLOB )
#   Error = Column( String( 255 ) )
#   Type = Column( String( 64 ), nullable = False )
#   _Order = Column( 'Order', Integer, nullable = False )
#   _Status = Column( 'Status', Enum( 'Waiting', 'Assigned', 'Queued', 'Done', 'Failed', 'Canceled', 'Scheduled' ), server_default = 'Queued' )
#   _LastUpdate = Column( 'LastUpdate', DateTime )
#   _SubmitTime = Column( 'SubmitTime', DateTime )
#   _Catalog = Column( 'Catalog', String( 255 ) )
#   OperationID = Column( Integer, primary_key = True )
#
#   RequestID = Column( 'RequestID', Integer,
#                       ForeignKey( 'Request.RequestID', ondelete = 'CASCADE' ),
#                       nullable = False )
#
#
#   __files__ = relationship( 'File',
#                             backref = backref( '_parent', lazy = 'immediate' ),
#                             lazy = 'immediate',
#                             passive_deletes = True,
#                             cascade = "all, delete-orphan" )

#   __dirty = []


  def __init__( self, fromDict = None ):
    """ c'tor

    :param self: self reference
    :param dict fromDict: attributes dictionary
    """
    self._parent = None
    # # sub-request attributes
    # self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )
    now = datetime.datetime.utcnow().replace( microsecond = 0 )

    self._SubmitTime = now
    self._LastUpdate = now
    self._CreationTime = now

    self._Status = "Queued"
    self._Order = 0
    self.__files__ = []

    self.TargetSE = None
    self.SourceSE = None
    self.Arguments = None
    self.Error = None
    self.Type = None
    self._Catalog = None
#     self.OperationID = -1
#     self.RequestID = -2

    fromDict = fromDict if isinstance( fromDict, dict ) else json.loads( fromDict ) if isinstance( fromDict, StringTypes ) else {}


#     self.__dirty = fromDict.get( "__dirty", [] )
#     if "__dirty" in fromDict:
#       del fromDict["__dirty"]


    for fileDict in fromDict.get( "Files", [] ):
      self.addFile( File( fileDict ) )
    if "Files" in fromDict:
      del fromDict["Files"]

    for key, value in fromDict.items():
#       if key not in self.__data__:
#         raise AttributeError( "Unknown Operation attribute '%s'" % key )
      if type( value ) in StringTypes:
        value = value.encode()
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
      self.Error = ''
      newStatus = 'Done'

    # If the status moved to Failed or Done, update the lastUpdate time
    if newStatus in ('Failed', 'Done'):
      if self._Status != newStatus:
        self._LastUpdate = datetime.datetime.utcnow().replace( microsecond = 0 )


    self._Status = newStatus
    if self._parent:
      self._parent._notify()

  def _setQueued( self, caller ):
    """ don't touch """
    if caller == self._parent:
      self._Status = "Queued"

  def _setWaiting( self, caller ):
    """ don't touch as well """
    if caller == self._parent:
      self._Status = "Waiting"

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
#     if not self.OperationID:
#       self.__files__.__delitem__( i )
#     else:
#       if self[i].FileID:
#         self.__dirty.append( self[i].FileID )
    self.__files__.__delitem__( i )
    self._notify()

  def __setitem__( self, i, opFile ):
    """ overwrite opFile """
#     self.__files__._typeCheck( opFile )
#     toDelete = self[i]
#     if toDelete.FileID:
#       self.__dirty.append( toDelete.FileID )
    self.__files__.__setitem__( i, opFile )
    opFile._parent = self
    self._notify()

  def fileStatusList( self ):
    """ get list of files statuses """
    return [ subFile.Status for subFile in self ]

  def __nonzero__( self ):
    """ for comparisons
    """
    return True

  def __len__( self ):
    """ nb of subFiles """
    return len( self.__files__ )

#   # # properties
#   @hybrid_property
#   def RequestID( self ):
#     """ RequestID getter (RO) """
#     return self._parent.RequestID if self._parent else -1
#
#   @RequestID.setter
#   def RequestID( self, value ):
#     """ can't set RequestID by hand """
#     self._RequestID = self._parent.RequestID if self._parent else -1

  @property
  def sourceSEList( self ):
    """ helper property returning source SEs as a list"""
    return self.SourceSE.split( "," )

  @property
  def targetSEList( self ):
    """ helper property returning target SEs as a list"""
    return self.TargetSE.split( "," )

  @hybrid_property
  def Catalog( self ):
    """ catalog prop """
    return self._Catalog

  @Catalog.setter
  def Catalog( self, value ):
    """ catalog setter """
    value = ",".join( self._uniqueList( value ) )
    if len( value ) > 255:
      raise ValueError( "Catalog list too long" )
    self._Catalog = value.encode() if value else ""

  @property
  def catalogList( self ):
    """ helper property returning catalogs as list """
    return self._Catalog.split( "," )

  @hybrid_property
  def Status( self ):
    """ Status prop """
    return self._Status

  @Status.setter
  def Status( self, value ):
    """ Status setter """
    if value not in Operation.ALL_STATES:
      raise ValueError( "unknown Status '%s'" % str( value ) )
    if self.__files__:
      self._notify()
    else:
      # If the status moved to Failed or Done, update the lastUpdate time
      if value in ( 'Failed', 'Done' ):
        if self._Status != value:
          self._LastUpdate = datetime.datetime.utcnow().replace( microsecond = 0 )

      self._Status = value
      if self._parent:
        self._parent._notify()
    if self._Status == 'Done':
      self.Error = ''

  @hybrid_property
  def Order( self ):
    """ order prop """
    if self._parent:
      self._Order = self._parent.indexOf( self ) if self._parent else -1
    return self._Order

  @Order.setter
  def Order( self, value ):
    """ order prop """
    self._Order = value


  @hybrid_property
  def CreationTime( self ):
    """ operation creation time prop """
    return self._CreationTime

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( [datetime.datetime] + list( StringTypes ) ):
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) in StringTypes:
      value = datetime.datetime.strptime( value.split( "." )[0], self._datetimeFormat )
    self._CreationTime = value

  @hybrid_property
  def SubmitTime( self ):
    """ subrequest's submit time prop """
    return self._SubmitTime

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submit time setter """
    if type( value ) not in ( [datetime.datetime] + list( StringTypes ) ):
        raise TypeError( "SubmitTime should be a datetime.datetime!" )
    if type( value ) in StringTypes:
      value = datetime.datetime.strptime( value.split( "." )[0], self._datetimeFormat )
    self._SubmitTime = value

  @hybrid_property
  def LastUpdate( self ):
    """ last update prop """
    return self._LastUpdate

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in ( [datetime.datetime] + list( StringTypes ) ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) in StringTypes:
      value = datetime.datetime.strptime( value.split( "." )[0], self._datetimeFormat )
    self._LastUpdate = value

  def __str__( self ):
    """ str operator """
    return self.toJSON()['Value']



#   def toSQL( self ):
#     """ get SQL INSERT or UPDATE statement """
#     if not getattr( self, "RequestID" ):
#       raise AttributeError( "RequestID not set" )
#     colVals = [ ( "`%s`" % column, "'%s'" % getattr( self, column )
#                   if type( getattr( self, column ) ) in ( str, datetime.datetime )
#                      else str( getattr( self, column ) ) if getattr( self, column ) != None else "NULL" )
#                 for column in self.__data__
#                 if ( column == 'Error' or getattr( self, column ) ) and column not in ( "OperationID", "LastUpdate", "Order" ) ]
#     colVals.append( ( "`LastUpdate`", "UTC_TIMESTAMP()" ) )
#     colVals.append( ( "`Order`", str( self.Order ) ) )
#     # colVals.append( ( "`Status`", "'%s'" % str(self.Status) ) )
#     query = []
#     if self.OperationID:
#       query.append( "UPDATE `Operation` SET " )
#       query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
#       query.append( " WHERE `OperationID`=%d;\n" % self.OperationID )
#     else:
#       query.append( "INSERT INTO `Operation` " )
#       columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
#       values = "(%s)" % ",".join( [ value for column, value in colVals ] )
#       query.append( columns )
#       query.append( " VALUES %s;\n" % values )
#
#     return S_OK( "".join( query ) )

#   def cleanUpSQL( self ):
#     """ query deleting dirty records from File table """
#     if self.OperationID and self.__dirty:
#       fIDs = ",".join( [ str( fid ) for fid in self.__dirty ] )
#       return "DELETE FROM `File` WHERE `OperationID` = %s AND `FileID` IN (%s);\n" % ( self.OperationID, fIDs )

#   def toJSON( self ):
#     """ get json digest """
#     digest = dict( [( key, str( val ) ) for key, val in self.__data__.items()] )
#     digest["RequestID"] = str( self.RequestID )
#     digest["Order"] = str( self.Order )
#     if self.__dirty:
#       digest["__dirty"] = self.__dirty
#     digest["Files"] = [opFile.toJSON()['Value'] for opFile in self]
#
#     return S_OK( digest )

  def toJSON( self ):
    try:
      jsonStr = json.dumps( self, cls = RMSEncoder )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ["Type", "Status", "Arguments",
                 "Order", "SourceSE", "TargetSE", "Catalog", "Error",
                  "CreationTime", "SubmitTime", "LastUpdate"]
    jsonData = {}


    if hasattr( self, 'OperationID' ):
      jsonData['OperationID'] = getattr( self, 'OperationID' )
    if hasattr( self, 'RequestID' ):
      jsonData['RequestID'] = getattr( self, 'RequestID' )

    for attrName in attrNames :
#         jsonData[attrName] = getattr( self, attrName )
        value = getattr( self, attrName )

        if isinstance( value, datetime.datetime ):
          # We convert date time to a string
          jsonData[attrName] = value.strftime( self._datetimeFormat )
        else:
          jsonData[attrName] = value
#     jsonData['__dirty'] = self.__dirty
    jsonData['Files'] = self.__files__

    return jsonData

  @staticmethod
  def _uniqueList( value, sep = "," ):
    """ make unique list from :value: """
    if type( value ) not in ( str, unicode, list ):
      raise TypeError( "wrong type for value" )
    if type( value ) in ( str, unicode ):
      value = value.split( sep )
    return list ( set ( [ str( item ).strip() for item in value if str( item ).strip() ] ) )
