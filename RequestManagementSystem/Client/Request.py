########################################################################
# $HeadURL $
# File: Request.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/16 13:43:45
########################################################################
""" :mod: Request
    =============

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
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TypedList import TypedList
from DIRAC.RequestManagementSystem.private.Record import Record
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.RequestManagementSystem.Client.Operation import Operation

########################################################################
class Request( Record ):
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

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    """
    Record.__init__( self )
    self.__waiting = None

    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["CreationTime"] = now
    self.__data__["SubmitTime"] = now
    self.__data__["LastUpdate"] = now
    self.__data__["Status"] = "Done"
    self.__data__["JobID"] = 0
    self.__data__["RequestID"] = 0

    proxyInfo = getProxyInfo()
    if proxyInfo["OK"]:
      proxyInfo = proxyInfo["Value"]
      if proxyInfo["validGroup"] and proxyInfo["validDN"]:
        self.OwnerDN = proxyInfo["identity"]
        self.OwnerGroup = proxyInfo["group"]

    self.__dirty = []
    self.__operations__ = TypedList( allowedTypes = Operation )

    fromDict = fromDict if fromDict else {}

    self.__dirty = fromDict.get( "__dirty", [] )
    if "__dirty" in fromDict:
      del fromDict["__dirty"]

    for opDict in fromDict.get( "Operations", [] ):
      self +=Operation( opDict )
    if "Operations" in fromDict:
      del fromDict["Operations"]

    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown Request attribute '%s'" % key )
      if value:
        setattr( self, key, value )
    self._notify()

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "RequestID" : "INTEGER NOT NULL AUTO_INCREMENT",
               "RequestName" : "VARCHAR(255) NOT NULL",
               "OwnerDN" : "VARCHAR(255)",
               "OwnerGroup" : "VARCHAR(32)",
               "Status" : "ENUM('Waiting', 'Assigned', 'Done', 'Failed', 'Canceled', 'Scheduled') DEFAULT 'Waiting'",
               "Error" : "VARCHAR(255)",
               "DIRACSetup" : "VARCHAR(32)",
               "SourceComponent" : "BLOB",
               "JobID" : "INTEGER DEFAULT 0",
               "CreationTime" : "DATETIME",
               "SubmitTime" : "DATETIME",
               "LastUpdate" : "DATETIME"  },
             "PrimaryKey" : [ "RequestID" ],
             'UniqueIndexes': {'RequestName' : [ 'RequestName'] }
           }

  def _notify( self ):
    """ simple state machine for sub request statuses """
    self.__waiting = None
    # # update operations statuses

    rStatus = "Waiting"
    opStatusList = [ ( op.Status, op ) for op in self ]

    self.__waiting = None

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
    newOperation._parent = self
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
    newOperation._parent = self
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
    self.__operations__._typeCheck( value )
    if self[i].OperationID:
      self.__dirty.append( self[i].OperationID )
    self.__operations__.__setitem__( i, value )
    value._parent = self
    self._notify()

  def __delitem__( self, i ):
    """ del self[i]"""
    if not self.RequestID:
      self.__operations__.__delitem__( i )
    else:
      opId = self[i].OperationID
      if opId:
        self.__dirty.append( opId )
      self.__operations__.__delitem__( i )
    self._notify()

  def indexOf( self, subReq ):
    """ return index of subReq (execution order) """
    return self.__operations__.index( subReq ) if subReq in self else -1

  def __len__( self ):
    """ nb of subRequests """
    return len( self.__operations__ )

  def __str__( self ):
    """ str operator """
    return str( self.toJSON()["Value"] )

  def subStatusList( self ):
    """ list of statuses for all operations """
    return [ subReq.Status for subReq in self ]

  # # properties

  @property
  def RequestID( self ):
    """ request ID getter """
    return self.__data__["RequestID"]

  @RequestID.setter
  def RequestID( self, value ):
    """ requestID setter (shouldn't be RO???) """
    self.__data__["RequestID"] = long( value ) if value else 0

  @property
  def RequestName( self ):
    """ request's name getter """
    return self.__data__["RequestName"]

  @RequestName.setter
  def RequestName( self, value ):
    """ request name setter """
    if type( value ) != str:
      raise TypeError( "RequestName should be a string" )
    self.__data__["RequestName"] = value[:128]

  @property
  def OwnerDN( self ):
    """ request owner DN getter """
    return self.__data__["OwnerDN"]

  @OwnerDN.setter
  def OwnerDN( self, value ):
    """ request owner DN setter """
    if type( value ) != str:
      raise TypeError( "OwnerDN should be a string!" )
    self.__data__["OwnerDN"] = value

  @property
  def OwnerGroup( self ):
    """ request owner group getter  """
    return self.__data__["OwnerGroup"]

  @OwnerGroup.setter
  def OwnerGroup( self, value ):
    """ request owner group setter """
    if type( value ) != str:
      raise TypeError( "OwnerGroup should be a string!" )
    self.__data__["OwnerGroup"] = value

  @property
  def DIRACSetup( self ):
    """ DIRAC setup getter  """
    return self.__data__["DIRACSetup"]

  @DIRACSetup.setter
  def DIRACSetup( self, value ):
    """ DIRAC setup setter """
    if type( value ) != str:
      raise TypeError( "setup should be a string!" )
    self.__data__["DIRACSetup"] = value

  @property
  def SourceComponent( self ):
    """ source component getter  """
    return self.__data__["SourceComponent"]

  @SourceComponent.setter
  def SourceComponent( self, value ):
    """ source component setter """
    if type( value ) != str:
      raise TypeError( "Setup should be a string!" )
    self.__data__["SourceComponent"] = value

  @property
  def JobID( self ):
    """ jobID getter """
    return self.__data__["JobID"]

  @JobID.setter
  def JobID( self, value = 0 ):
    """ jobID setter """
    self.__data__["JobID"] = long( value ) if value else 0

  @property
  def CreationTime( self ):
    """ creation time getter """
    return self.__data__["CreationTime"]

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( datetime.datetime, str ) :
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["CreationTime"] = value

  @property
  def SubmitTime( self ):
    """ request's submission time getter """
    return self.__data__["SubmitTime"]

  @SubmitTime.setter
  def SubmitTime( self, value = None ):
    """ submission time setter """
    if type( value ) not in ( datetime.datetime, str ):
      raise TypeError( "SubmitTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["SubmitTime"] = value

  @property
  def LastUpdate( self ):
    """ last update getter """
    return self.__data__["LastUpdate"]

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in  ( datetime.datetime, str ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["LastUpdate"] = value

  @property
  def Status( self ):
    """ status getter """
    self._notify()
    return self.__data__["Status"]

  @Status.setter
  def Status( self, value ):
    """ status setter """
    if value not in Request.ALL_STATES:
      raise ValueError( "Unknown status: %s" % str( value ) )
    if value == 'Done':
      self.__data__['Error'] = ''
    self.__data__["Status"] = value

  @property
  def Order( self ):
    """ ro execution order getter """
    self._notify()
    opStatuses = [ op.Status for op in self.__operations__ ]
    return opStatuses.index( "Waiting" ) if "Waiting" in opStatuses else len( opStatuses )

  @property
  def Error( self ):
    """ error getter """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, value ):
    """ error setter """
    if type( value ) != str:
      raise TypeError( "Error has to be a string!" )
    self.__data__["Error"] = self._escapeStr( value, 255 )

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) in ( str, datetime.datetime ) else str( value ) )
                for column, value in self.__data__.items()
                if value and column not in  ( "RequestID", "LastUpdate" ) ]
    colVals.append( ( "`LastUpdate`", "UTC_TIMESTAMP()" ) )
    query = []
    if self.RequestID:
      query.append( "UPDATE `Request` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `RequestID`=%d;\n" % self.RequestID )
    else:
      query.append( "INSERT INTO `Request` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return S_OK( "".join( query ) )

  def cleanUpSQL( self ):
    """ delete query for dirty operations """
    query = []
    if self.RequestID and self.__dirty:
      opIDs = ",".join( [ str( opID ) for opID in self.__dirty ] )
      query.append( "DELETE FROM `Operation` WHERE `RequestID`=%s AND `OperationID` IN (%s);\n" % ( self.RequestID,
                                                                                                    opIDs ) )
      for opID in self.__dirty:
        query.append( "DELETE FROM `File` WHERE `OperationID`=%s;\n" % opID )
      return query
  # # digest
  def toJSON( self ):
    """ serialize to JSON format """
    digest = dict( zip( self.__data__.keys(),
                        [ str( val ) if val else "" for val in self.__data__.values() ] ) )
    digest["RequestID"] = self.RequestID
    digest["Operations"] = []
    digest["__dirty"] = self.__dirty
    for op in self:
      opJSON = op.toJSON()
      if not opJSON["OK"]:
        return opJSON
      digest["Operations"].append( opJSON["Value"] )
    return S_OK( digest )

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

