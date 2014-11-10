########################################################################
# $HeadURL $
# File: Request.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/16 13:43:45
########################################################################
"""
:mod: Request

.. module: Request
  :synopsis: request implementation

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

request implementation
"""
# for properties
# pylint: disable=E0211,W0612,W0142,C0103
__RCSID__ = "$Id$"
# #
# @file Request.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/16 13:44:00
# @brief Definition of Request class.

# # imports
import datetime
from types import StringTypes
import json
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.private.RMSBase import RMSBase
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.private.JSONUtils import RMSEncoder

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Enum, BLOB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.orderinglist import ordering_list




########################################################################
class Request( object ):
  """
  .. class:: Request

  :param int RequestID: requestID
  :param str Name: request' name
  :param str OwnerDN: request's owner DN
  :param str OwnerGroup: request owner group
  :param str Setup: DIRAC setup
  :param str SourceComponent: whatever
  :param int JobID: jobID
  :param datetime.datetime CreationTime: UTC datetime
  :param datetime.datetime SubmissionTime: UTC datetime
  :param datetime.datetime LastUpdate: UTC datetime
  :param str Status: request's status
  :param TypedList operations: list of operations
  """

  ALL_STATES = ( "Waiting", "Failed", "Done", "Scheduled", "Assigned", "Canceled" )

  FINAL_STATES = ( "Done", "Failed", "Canceled" )

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'
#   RequestID = -1


#   __tablename__ = 'Request'
#
#   DIRACSetup = Column( String( 32 ) )
#   _CreationTime = Column( 'CreationTime', DateTime )
#   JobID = Column( Integer, server_default = '0' )
#   OwnerDN = Column( String( 255 ) )
#   RequestName = Column( String( 255 ), nullable = False, unique = True )
#   Error = Column( String( 255 ) )
#   _Status = Column( 'Status', Enum( 'Waiting', 'Assigned', 'Done', 'Failed', 'Canceled', 'Scheduled' ), server_default = 'Waiting' )
#   _LastUpdate = Column( 'LastUpdate', DateTime )
#   OwnerGroup = Column( String( 32 ) )
#   _SubmitTime = Column( 'SubmitTime', DateTime )
#   RequestID = Column( Integer, primary_key = True )
#   SourceComponent = Column( BLOB )
#
#
# #   __dirty = []
#   #__operations__ = relationship( 'Operation', backref = '_parent', order_by='Operation.Order' )
#   __operations__ = relationship( 'Operation',
#                                   backref = backref( '_parent', lazy = 'immediate' ),
#                                   order_by = 'Operation._Order',
#                                   lazy = 'immediate',
#                                   passive_deletes = True,
#                                   cascade = "all, delete-orphan"
#                                 )
#   __operations__ = relationship( 'Operation', backref = '_parent', order_by = 'Operation._Order', collection_class = ordering_list( '_Order' ) )

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param fromDict : if false, new request. Can be json string that represents the object, or the dictionary directly
    """
    self.__waiting = None

    now = datetime.datetime.utcnow().replace( microsecond = 0 )
#     self.__data__["CreationTime"] = now
#     self.__data__["SubmitTime"] = now
#     self.__data__["LastUpdate"] = now
#     self.__data__["Status"] = "Done"
#     self.__data__["JobID"] = 0
#     self.__data__["RequestID"] = 0
    self._CreationTime = now
    self._SubmitTime = now
    self._LastUpdate = now
    self._Status = "Done"
    self.JobID = 0
    self.Error = None
    self.DIRACSetup = None
    self.OwnerDN = None
    self.RequestName = None
    self.OwnerGroup = None
    self.SourceComponent = None

    proxyInfo = getProxyInfo()
    if proxyInfo["OK"]:
      proxyInfo = proxyInfo["Value"]
      if proxyInfo["validGroup"] and proxyInfo["validDN"]:
        self.OwnerDN = proxyInfo["identity"]
        self.OwnerGroup = proxyInfo["group"]

    self.__operations__ = []

#     self.__dirty = []


    # The attribute __operations__ is set by the foreign key constrain in the Operation object
#     self.__operations__ = relationship( 'Operation', backref = '_parent' )

    fromDict = fromDict if isinstance( fromDict, dict ) else json.loads( fromDict ) if isinstance( fromDict, StringTypes ) else {}

#     self.__dirty = fromDict.get( "__dirty", [] )
#     if "__dirty" in fromDict:
#       del fromDict["__dirty"]

    for opDict in fromDict.get( "Operations", [] ):
      self +=Operation( opDict )
    if "Operations" in fromDict:
      del fromDict["Operations"]

    for key, value in fromDict.items():
#       if key not in self.__data__:
#         raise AttributeError( "Unknown Request attribute '%s'" % key )

      # The JSON module forces the use of UTF-8, which is not properly
      # taken into account in DIRAC.
      # One would need to replace all the '== str' with 'in StringTypes'
      if type( value ) in StringTypes:
        value = value.encode()

      if value:
        setattr( self, key, value )
    self._notify()

#   @staticmethod
#   def tableDesc():
#     """ get table desc """
#     return { "Fields" :
#              { "RequestID" : "INTEGER NOT NULL AUTO_INCREMENT",
#                "RequestName" : "VARCHAR(255) NOT NULL",
#                "OwnerDN" : "VARCHAR(255)",
#                "OwnerGroup" : "VARCHAR(32)",
#                "Status" : "ENUM('Waiting', 'Assigned', 'Done', 'Failed', 'Canceled', 'Scheduled') DEFAULT 'Waiting'",
#                "Error" : "VARCHAR(255)",
#                "DIRACSetup" : "VARCHAR(32)",
#                "SourceComponent" : "BLOB",
#                "JobID" : "INTEGER DEFAULT 0",
#                "CreationTime" : "DATETIME",
#                "SubmitTime" : "DATETIME",
#                "LastUpdate" : "DATETIME"  },
#              "PrimaryKey" : [ "RequestID" ],
#              'UniqueIndexes': {'RequestName' : [ 'RequestName'] }
#            }

  def _notify( self ):
    """ simple state machine for sub request statuses """
    # # update operations statuses
    self.__waiting = None


    # Update the Order in Operation
    for i in range( len( self.__operations__ ) ):
      self.__operations__[i].Order = i
      self.__operations__[i]._parent = self

    rStatus = "Waiting"
    opStatusList = [ ( op.Status, op ) for op in self ]



    while opStatusList:
      # # Scan all status in order!
      opStatus, op = opStatusList.pop( 0 )

      # # Failed -> Failed
      if opStatus == "Failed":
        rStatus = "Failed"
        break

      # Scheduled -> Scheduled
      if opStatus == "Scheduled":
        if self.__waiting == None:
          self.__waiting = op
          rStatus = "Scheduled"
      # # First operation Queued becomes Waiting if no Waiting/Scheduled before
      elif opStatus == "Queued":
        if self.__waiting == None:
          self.__waiting = op
          op._setWaiting( self )
          rStatus = "Waiting"
      # # First operation Waiting is next to execute, others are queued
      elif opStatus == "Waiting":
        rStatus = "Waiting"
        if self.__waiting == None:
          self.__waiting = op
        else:
          op._setQueued( self )
      # # All operations Done -> Done
      elif opStatus == "Done" and self.__waiting == None:
        rStatus = "Done"
        self._Error = ''
    self.Status = rStatus

  def getWaiting( self ):
    """ get waiting operation if any """
    # # update states
    self._notify()
    return S_OK( self.__waiting )

  # # Operation arithmetics
  def __contains__( self, operation ):
    """ in operator

    :param self: self reference
    :param Operation subRequest: a subRequest
    """
    return bool( operation in self.__operations__ )

  def __iadd__( self, operation ):
    """ += operator for subRequest

    :param self: self reference
    :param Operation operation: sub-request to add
    """
    if operation not in self:
      self.__operations__.append( operation )
      operation._parent = self
      self._notify()
    return self

  def insertBefore( self, newOperation, existingOperation ):
    """ insert :newOperation: just before :existingOperation:

    :param self: self reference
    :param Operation newOperation: Operation to be inserted
    :param Operation existingOperation: previous Operation sibling
    """
    if existingOperation not in self:
      return S_ERROR( "%s is not in" % existingOperation )
    if newOperation in self:
      return S_ERROR( "%s is already in" % newOperation )
    self.__operations__.insert( self.__operations__.index( existingOperation ), newOperation )
#     newOperation._parent = self
    self._notify()
    return S_OK()

  def insertAfter( self, newOperation, existingOperation ):
    """ insert :newOperation: just after :existingOperation:

    :param self: self reference
    :param Operation newOperation: Operation to be inserted
    :param Operation existingOperation: next Operation sibling
    """
    if existingOperation not in self:
      return S_ERROR( "%s is not in" % existingOperation )
    if newOperation in self:
      return S_ERROR( "%s is already in" % newOperation )
    self.__operations__.insert( self.__operations__.index( existingOperation ) + 1, newOperation )
#     newOperation._parent = self
    self._notify()
    return S_OK()

  def addOperation( self, operation ):
    """ add :operation: to list of Operations

    :param self: self reference
    :param Operation operation: Operation to be inserted
    """
    if operation in self:
      return S_ERROR( "This operation is already in!!!" )
    self +=operation
    return S_OK()

  def isEmpty( self ):
    """ Evaluate if the request is empty
    """
    return len( self.__operations__ ) == 0

  def __iter__( self ):
    """ iterator for sub-request """
    return self.__operations__.__iter__()

  def __getitem__( self, i ):
    """ [] op for sub requests """
    return self.__operations__.__getitem__( i )

  def __setitem__( self, i, value ):
    """ self[i] = val """
#     self.__operations__._typeCheck( value )
#     if self[i].OperationID:
#       self.__dirty.append( self[i].OperationID )
    self.__operations__.__setitem__( i, value )
#     value._parent = self

    self._notify()

  def __delitem__( self, i ):
    """ del self[i]"""
#     if not self.RequestID:
#       self.__operations__.__delitem__( i )
#     else:
#       opId = self[i].OperationID
#       if opId:
#         self.__dirty.append( opId )
    self.__operations__.__delitem__( i )
    self._notify()

  def indexOf( self, subReq ):
    """ return index of subReq (execution order) """
    return self.__operations__.index( subReq ) if subReq in self else -1

  def __nonzero__( self ):
    """ for comparisons
    """
    return True

  def __len__( self ):
    """ nb of subRequests """
    return len( self.__operations__ )

  def __str__( self ):
    """ str operator """
    return self.toJSON()['Value']

  def subStatusList( self ):
    """ list of statuses for all operations """
    return [ subReq.Status for subReq in self ]

  # # properties
  @hybrid_property
  def CreationTime( self ):
    """ creation time getter """
    return self._CreationTime

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( [datetime.datetime] + list( StringTypes ) ) :
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) in StringTypes:
      value = datetime.datetime.strptime( value.split( "." )[0], self._datetimeFormat )
    self._CreationTime = value

  @hybrid_property
  def SubmitTime( self ):
    """ request's submission time getter """
    return self._SubmitTime

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submission time setter """
    if type( value ) not in ( [datetime.datetime] + list( StringTypes ) ):
      raise TypeError( "SubmitTime should be a datetime.datetime!" )
    if type( value ) in StringTypes:
      value = datetime.datetime.strptime( value.split( "." )[0], self._datetimeFormat )
    self._SubmitTime = value

  @hybrid_property
  def LastUpdate( self ):
    """ last update getter """
    return self._LastUpdate

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in ( [datetime.datetime] + list( StringTypes ) ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) in StringTypes:
      value = datetime.datetime.strptime( value.split( "." )[0], self._datetimeFormat )
    self._LastUpdate = value

  @hybrid_property
  def Status( self ):
    """ status getter """
    self._notify()
    return self._Status

  @Status.setter
  def Status( self, value ):
    """ status setter """
    if value not in Request.ALL_STATES:
      raise ValueError( "Unknown status: %s" % str( value ) )

    # If the status moved to Failed or Done, update the lastUpdate time
    if value in ( 'Done', 'Failed' ):
      if value != self._Status:
        self.LastUpdate = datetime.datetime.utcnow().replace( microsecond = 0 )

    if value == 'Done':
      self.Error = ''
    self._Status = value

  @property
  def Order( self ):
    """ ro execution order getter """
    self._notify()
    opStatuses = [ op.Status for op in self.__operations__ ]
    return opStatuses.index( "Waiting" ) if "Waiting" in opStatuses else len( opStatuses )


#   def toSQL( self ):
#     """ prepare SQL INSERT or UPDATE statement """
#     colVals = [ ( "`%s`" % column, "'%s'" % value
#                   if type( value ) in ( str, datetime.datetime ) else str( value ) if value != None else "NULL" )
#                 for column, value in self.__data__.items()
#                 if ( column == 'Error' or value ) and column not in  ( "RequestID", "LastUpdate" ) ]
#     colVals.append( ( "`LastUpdate`", "UTC_TIMESTAMP()" ) )
#     query = []
#     if self.RequestID:
#       query.append( "UPDATE `Request` SET " )
#       query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
#       query.append( " WHERE `RequestID`=%d;\n" % self.RequestID )
#     else:
#       query.append( "INSERT INTO `Request` " )
#       columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
#       values = "(%s)" % ",".join( [ value for column, value in colVals ] )
#       query.append( columns )
#       query.append( " VALUES %s;" % values )
#     return S_OK( "".join( query ) )

#   def cleanUpSQL( self ):
#     """ delete query for dirty operations """
#     query = []
#     if self.RequestID and self.__dirty:
#       opIDs = ",".join( [ str( opID ) for opID in self.__dirty ] )
#       query.append( "DELETE FROM `Operation` WHERE `RequestID`=%s AND `OperationID` IN (%s);\n" % ( self.RequestID,
#                                                                                                     opIDs ) )
#       for opID in self.__dirty:
#         query.append( "DELETE FROM `File` WHERE `OperationID`=%s;\n" % opID )
#       return query


#   def toJSON( self ):
#     try:
#       jsonStr = json.dumps( self, cls = RMSEncoder )
#       return S_OK( jsonStr )
#     except Exception, e:
#       return S_ERROR( str( e ) )
  def toJSON( self ):

    jsonStr = json.dumps( self, cls = RMSEncoder )
    return S_OK( jsonStr )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """
    attrNames = ["RequestName", "OwnerDN", "OwnerGroup",
                 "Status", "Error", "DIRACSetup", "SourceComponent",
                  "JobID", "CreationTime", "SubmitTime", "LastUpdate"]
    jsonData = {}


    if hasattr( self, 'RequestID' ):
      jsonData['RequestID'] = getattr( self, 'RequestID' )

    for attrName in attrNames :
      value = getattr( self, attrName )

      if isinstance( value, datetime.datetime ):
        # We convert date time to a string
        jsonData[attrName] = value.strftime( self._datetimeFormat )
      else:
        jsonData[attrName] = value

#     jsonData['RequestID'] = self.RequestID
#     jsonData["__dirty"] = self.__dirty
    jsonData['Operations'] = self.__operations__

    return jsonData

  def getDigest( self ):
    """ return digest for request """
    digest = ['Name:' + self.RequestName]
    for op in self:
      opDigest = [ str( item ) for item in ( op.Type, op.Type, op.Status, op.Order ) ]
      if op.TargetSE:
        opDigest.append( op.TargetSE )
      if op.Catalog:
        opDigest.append( op.Catalog )
      if len( op ):
        opFile = op[0]
        opDigest.append( opFile.LFN )
        opDigest.append( ",...<%d files>" % len( op ) )
      digest.append( ":".join( opDigest ) )
    return S_OK( "\n".join( digest ) )



  def optimize( self ):
    """ Merges together the operations that can be merged. They need to have the following arguments equal:
        * Type
        * Arguments
        * SourceSE
        * TargetSE
        * Catalog
        It also makes sure that the maximum number of Files in an Operation is never overcome.

        CAUTION: this method is meant to be called before inserting into the DB.
                So if the RequestID is not 0, we don't touch

        :return S_ERROR if the Request should not be optimized (because already in the DB
                S_OK(True) if a optimization was carried out
                S_OK(False) if no optimization were carried out
    """

    # Set to True if the request could be optimized
    optimized = False

    # List of attributes that must be equal for operations to be merged
    attrList = ["Type", "Arguments", "SourceSE", "TargetSE", "Catalog" ]
    i = 0

    # If the RequestID is not the default one (0), it probably means
    # the Request is already in the DB, so we don't touch anything
    if self.RequestID != 0:
      return S_ERROR( "Cannot optimize because Request seems to be already in the DB (RequestID %s)" % self.RequestID )

    # We could do it with a single loop (the 2nd one), but by doing this,
    # we can replace
    #   i += 1
    #   continue
    #
    # with
    #   break
    #
    # which is nicer in my opinion
    while i < len( self.__operations__ ):
      while  ( i + 1 ) < len( self.__operations__ ):
        # Some attributes need to be the same
        attrMismatch = False
        for attr in attrList:
          if getattr( self.__operations__[i], attr ) != getattr( self.__operations__[i + 1], attr ):
            attrMismatch = True
            break

        if attrMismatch:
          break

        # We do not do the merge if there are common files in the operations
        fileSetA = set( list( f.LFN for f in self.__operations__[i] ) )
        fileSetB = set( list( f.LFN for f in self.__operations__[i + 1] ) )
        if len( fileSetA & fileSetB ):
          break

        # There is a maximum number of files one can add into an operation
        try:
          while len( self.__operations__[i + 1] ):
            self.__operations__[i] += self.__operations__[i + 1][0]
            del self.__operations__[i + 1][0]
            optimized = True
          del self.__operations__[i + 1]
        except RuntimeError:
          i += 1
      i += 1


    return S_OK( optimized )
