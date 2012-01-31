import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Security import Properties
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client.Job.JobState import JobState

class JobStoreHandler( RequestHandler ):

  __jobStateMethods = []

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    cls.jobDB = JobDB()
    result = cls.jobDB._getConnection()
    log = cls.srv_getLog()
    if not result[ 'OK' ]:
      log.warn( "Could not connect to JobDB (%s). Resorting to RPC" % result[ 'Message' ] )
    result[ 'Value' ].close()
    #Try to do magic
    myStuff = dir( cls )
    for method in dir( JobState ):
      if method.find( "set" ) != 0 and method.find( "get" ) != 0 and method.find( "execute" ) != 0:
        continue
      if "export_%s" % method in myStuff:
        log.info( "Skipping method %s. It's already defined in the Handler" % method )
        continue
      log.info( "Mimicking method %s" % method )
      setattr( cls, "auth_%s" % method, [ 'all' ] )
      setattr( cls, "types_%s" % method, [ ( types.IntType, types.LongType ) ] )
      print "export_%s" % method
      setattr( cls, "export_%s" % method, cls.__mimeticFunction )
    return S_OK()

  def __mimeticFunction( self, *args ):
    print "MIMETIC!!", self, self.srv_getActionTuple(), args
    method = self.srv_getActionTuple()[1]
    jid = args[0]
    args = args[1:]
    if not self.__clientHasAccess( jid ):
      return S_ERROR( "You're not authorized to access jid %s" % jid )
    return getattr( self.__getJobState( jid ), method )( *args )

  def __getJobState( self, jid ):
    return JobState( jid, forceLocal = True )

  def __clientHasAccess( self, jid ):
    result = self.jobDB.getJobAttributes( jid, [ 'Owner', 'OwnerDN', 'OwnerGroup' ] )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot retrieve owner for jid %s" % jid )
    ownerDict = result[ 'Value' ]
    credDict = self.srv_getRemoteCredentials()
    idString = "%s@%s (%s)" % ( credDict[ 'username' ], credDict[ 'group' ], credDict[ 'DN' ] )
    if Properties.JOB_ADMINISTRATOR in credDict[ 'properties' ]:
      self.srv_log.verbose( "%s is job admin of jid %s" % ( idString, jid ) )
      return True
    if credDict[ 'username' ] == ownerDict[ 'Owner' ]:
      self.srv_log.verbose( "%s is owner of jid %s" % ( idString, jid ) )
      return True
    if Properties.JOB_SHARING in credDict[ 'properties' ] and \
        credDict[ 'group' ] == ownerDict[ 'OwnerGroup' ]:
      self.srv_log.verbose( "%s is sharing group with jid %s" % ( idString, jid ) )
      return True
    self.srv_log.verbose( "%s is NOT allowed to access jid %s" % ( idString, jid ) )
    return False

#Manifests

  auth_getManifest = "all"
  types_getManifest = [ ( types.IntType, types.LongType ) ]
  def export_getManifest( self, jid ):
    if not self.__clientHasAccess( jid ):
      return S_ERROR( "You're not authorized to access jid %s" % jid )
    return self.__getJobState( jid ).getManifest( rawData = True )

  auth_setManifest = "all"
  types_setManifest = [ ( types.IntType, types.LongType ), types.StringTypes ]
  def export_setManifest( self, jid, manifest ):
    if not self.__clientHasAccess( jid ):
      return S_ERROR( "You're not authorized to access jid %s" % jid )
    return self.__getJobState( jid ).setManifest( manifest )

#Status

  auth_setStatus = "all"
  types_setStatus = [ ( types.IntType, types.LongType ), types.StringTypes, types.StringTypes ]
  def export_setStatus( self, jid, majorStatus, minorStatus ):
    if not self.__clientHasAccess( jid ):
      return S_ERROR( "You're not authorized to access jid %s" % jid )
    return self.__getJobState( jid ).setStatus( majorStatus. minorStatus )

  auth_getStatus = "all"
  types_getStatus = [ ( types.IntType, types.LongType ) ]
  def export_getStatus( self, jid ):
    if not self.__clientHasAccess( jid ):
      return S_ERROR( "You're not authorized to access jid %s" % jid )
    return self.__getJobState( jid ).getStatus()

