########################################################################
# File: Request.py
# Date: 2012/07/16 13:43:45
########################################################################
"""
:mod: Request

.. module: Request
  :synopsis: request implementation

request implementation
"""
# for properties
# pylint: disable=E0211,W0612,C0103
__RCSID__ = "$Id$"


# # imports
import datetime
from types import StringTypes
import json
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.private.JSONUtils import RMSEncoder

from types import NoneType

from sqlalchemy.ext.hybrid import hybrid_property





########################################################################
class Request( object ):
  """
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
  :param datetime.datetime NotBefore: UTC datetime
  :param str Status: request's status
  :param TypedList.TypedList operations: list of operations

  It is managed by SQLAlchemy, so the RequestID should never be set by hand (except when constructed from
  JSON of course...)
  """

  ALL_STATES = ( "Waiting", "Failed", "Done", "Scheduled", "Assigned", "Canceled" )

  FINAL_STATES = ( "Done", "Failed", "Canceled" )

  _datetimeFormat = '%Y-%m-%d %H:%M:%S'


  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param fromDict : if false, new request. Can be json string that represents the object, or the dictionary directly
    """
    self.__waiting = None

    now = datetime.datetime.utcnow().replace( microsecond = 0 )

    self._CreationTime = now
    self._SubmitTime = now
    self._LastUpdate = now
    # the time before which the request should not be executed
    # If None, no delay
    self._NotBefore = now
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

    fromDict = fromDict if isinstance( fromDict, dict )\
               else json.loads( fromDict ) if isinstance( fromDict, StringTypes )\
                else {}


    if "Operations" in fromDict:
      for opDict in fromDict.get( "Operations", [] ):
        self +=Operation( opDict )

      del fromDict["Operations"]

    for key, value in fromDict.items():
      # The JSON module forces the use of UTF-8, which is not properly
      # taken into account in DIRAC.
      # One would need to replace all the '== str' with 'in StringTypes'
      if type( value ) in StringTypes:
        value = value.encode()

      if value:
        setattr( self, key, value )

    self._notify()


  def _notify( self ):
    """ simple state machine for sub request statuses """
    # # update operations statuses
    self.__waiting = None


    # Update the Order in Operation, and set the parent
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
    :param Operation.Operation subRequest: a subRequest
    """
    return bool( operation in self.__operations__ )

  def __iadd__( self, operation ):
    """ += operator for subRequest

    :param self: self reference
    :param Operation.Operation operation: sub-request to add
    """
    if operation not in self:
      self.__operations__.append( operation )
      operation._parent = self
      self._notify()
    return self

  def insertBefore( self, newOperation, existingOperation ):
    """ insert :newOperation: just before :existingOperation:

    :param self: self reference
    :param Operation.Operation newOperation: Operation to be inserted
    :param Operation.Operation existingOperation: previous Operation sibling
    """
    if existingOperation not in self:
      return S_ERROR( "%s is not in" % existingOperation )
    if newOperation in self:
      return S_ERROR( "%s is already in" % newOperation )
    self.__operations__.insert( self.__operations__.index( existingOperation ), newOperation )
    self._notify()
    return S_OK()

  def insertAfter( self, newOperation, existingOperation ):
    """ insert :newOperation: just after :existingOperation:

    :param self: self reference
    :param Operation.Operation newOperation: Operation to be inserted
    :param Operation.Operation existingOperation: next Operation sibling
    """
    if existingOperation not in self:
      return S_ERROR( "%s is not in" % existingOperation )
    if newOperation in self:
      return S_ERROR( "%s is already in" % newOperation )
    self.__operations__.insert( self.__operations__.index( existingOperation ) + 1, newOperation )
    self._notify()
    return S_OK()

  def addOperation( self, operation ):
    """ add :operation: to list of Operations

    :param self: self reference
    :param Operation.Operation operation: Operation to be inserted
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

    self.__operations__.__setitem__( i, value )
    self._notify()

  def __delitem__( self, i ):
    """ del self[i]"""

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
  def NotBefore( self ):
    """ Getter for NotBefore time"""
    return self._NotBefore

  @NotBefore.setter
  def NotBefore( self, value = None ):
    """ Setter for the NotBefore time """
    if type( value ) not in ( [NoneType] + [datetime.datetime] + list( StringTypes ) ):
      raise TypeError( "NotBefore should be a datetime.datetime!" )
    if type( value ) in StringTypes:
      value = datetime.datetime.strptime( value.split( "." )[0], self._datetimeFormat )
    self._NotBefore = value


  def delayNextExecution( self, deltaTime ):
    """This helper sets the NotBefore attribute in deltaTime minutes
       in the future
       :param deltaTime : time in minutes before next execution
    """
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    extraDelay = datetime.timedelta( minutes = deltaTime )
    self._NotBefore = now + extraDelay

    return S_OK()


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

  def toJSON( self ):
    """ Returns the JSON formated string that describes the request """

    jsonStr = json.dumps( self, cls = RMSEncoder )
    return S_OK( jsonStr )


  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """

    attrNames = ['RequestID', "RequestName", "OwnerDN", "OwnerGroup",
                 "Status", "Error", "DIRACSetup", "SourceComponent",
                  "JobID", "CreationTime", "SubmitTime", "LastUpdate", "NotBefore"]
    jsonData = {}

    for attrName in attrNames :

      # RequestID might not be set since it is managed by SQLAlchemy
      if not hasattr( self, attrName ):
        continue

      value = getattr( self, attrName )

      if isinstance( value, datetime.datetime ):
        # We convert date time to a string
        jsonData[attrName] = value.strftime( self._datetimeFormat )
      else:
        jsonData[attrName] = value

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

    # If the RequestID is not the default one (0), it probably means
    # the Request is already in the DB, so we don't touch anything
    if self.RequestID:
      return S_ERROR( "Cannot optimize because Request seems to be already in the DB (RequestID %s)" % self.RequestID )
    # Set to True if the request could be optimized
    optimized = False
    # Recognise Failover request series
    repAndRegList = []
    removeRepList = []
    i = 0
    while i < len( self.__operations__ ):
      insertNow = True
      if i < len( self.__operations__ ) - 1:
        op1 = self.__operations__[i]
        op2 = self.__operations__[i + 1]
        if getattr( op1, 'Type' ) == 'ReplicateAndRegister' and \
           getattr( op2, 'Type' ) == 'RemoveReplica':
          fileSetA = set( list( f.LFN for f in op1 ) )
          fileSetB = set( list( f.LFN for f in op2 ) )
          if fileSetA == fileSetB:
            # Source is useless if failover
            if 'FAILOVER' in op1.SourceSE:
              op1.SourceSE = ''
            repAndRegList.append( ( op1.TargetSE, op1 ) )
            removeRepList.append( ( op2.TargetSE, op2 ) )
            del self.__operations__[i]
            del self.__operations__[i]
            # If we are at the end of the request, we must insert the new operations
            insertNow = ( i == len( self.__operations__ ) )
      # print i, self.__operations__[i].Type if i < len( self.__operations__ ) else None, len( repAndRegList ), insertNow
      if insertNow:
        if repAndRegList:
          # We must insert the new operations there
          # i.e. insert before operation i (if it exists)
          # Replication first, removeReplica next
          optimized = True
          insertBefore = self.__operations__[i] if i < len( self.__operations__ ) else None
          # print 'Insert new operations before', insertBefore
          for op in \
            [op for _targetSE, op in sorted( repAndRegList )] + \
            [op for _targetSE, op in sorted( removeRepList )]:
            _res = self.insertBefore( op, insertBefore ) if insertBefore else self.addOperation( op )
            # Skip the newly inserted operation
            i += 1
          repAndRegList = []
          removeRepList = []
        else:
          # Skip current operation
          i += 1
      else:
        # Just to show that in that case we don't increment i
        pass

    # List of attributes that must be equal for operations to be merged
    attrList = ["Type", "Arguments", "SourceSE", "TargetSE", "Catalog" ]

    i = 0
    while i < len( self.__operations__ ):
      while i < len( self.__operations__ ) - 1:
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
        if fileSetA & fileSetB:
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
