import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Security import Properties
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client.Job.JobState import JobState

class JobStoreHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    cls.jobDB = JobDB()
    result = cls.jobDB._getConnection()
    if not result[ 'OK' ]:
      gLogger.warn( "Could not connect to JobDB (%s). Resorting to RPC" % result[ 'Message' ] )
    result[ 'Value' ].close()
    return S_OK()

  def __getJobState( self, jid ):
    return JobState( jid, forceLocal = True )

  def __clientHasAccess( self, jid ):
    result = self.jobDB.getJobAttributes( jid, [ 'Owner', 'OwnerDN', 'OwnerGroup' ] )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot retrieve owner for jid %s" % jid )
    ownerTuple = result[ 'Value' ]
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


