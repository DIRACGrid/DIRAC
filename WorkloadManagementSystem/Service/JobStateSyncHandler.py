import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Security import Properties
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState

__RCSID__ = "$Id$"

class JobStateSyncHandler( RequestHandler ):

  __jobStateMethods = []

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    cls.jobDB = JobDB()
    result = cls.jobDB._getConnection()
    if not result[ 'OK' ]:
      cls.log.warn( "Could not connect to JobDB (%s). Resorting to RPC" % result[ 'Message' ] )
    result[ 'Value' ].close()
    #Try to do magic
    myStuff = dir( cls )
    jobStateStuff = dir( JobState )
    for method in jobStateStuff:
      if "export_%s" % method in myStuff:
        cls.log.info( "Wrapping method %s. It's already defined in the Handler" % method )
#        defMeth = getattr( cls, "export_%s" % method )
#        setattr( cls, "_usr_def_%s" % method, defMeth )
#        setattr( cls, "types_%s" % method, [ ( types.IntType, types.LongType ), types.TupleType ] )
#        setattr( cls, "export_%s" % method, cls.__unwrapAndCall )
        continue
      elif 'right_%s' % method in jobStateStuff:
        cls.log.info( "Mimicking method %s" % method )
        setattr( cls, "auth_%s" % method, [ 'all' ] )
        setattr( cls, "types_%s" % method, [ ( types.IntType, types.LongType ), types.TupleType ] )
        setattr( cls, "export_%s" % method, cls.__mimeticFunction )
    return S_OK()

  def __unwrapArgs( self, margs ):
    if len( margs ) < 1 or type( margs[0] ) != types.TupleType or ( len( margs ) > 1 and type( margs[1] ) != types.DictType ):
      return S_ERROR( "Invalid arg stub. Expected tuple( args, kwargs? ), received %s" % str( margs ) )
    if len( margs ) == 1:
      return S_OK( ( margs[0], {} ) )
    else:
      return S_OK( ( margs[0], margs[1] ) )

  def __mimeticFunction( self, jid, margs ):
    method = self.srv_getActionTuple()[1]
    result = self.__unwrapArgs( margs )
    if not result[ 'OK' ]:
      return result
    args, kwargs = result[ 'Value' ]
    if not self.__clientHasAccess( jid ):
      return S_ERROR( "You're not authorized to access jid %s" % jid )
    return getattr( self.__getJobState( jid ), method )( *args, **kwargs )

  def __unwrapAndCall( self, jid, margs ):
    method = self.srv_getActionTuple()[1]
    result = self.__unwrapArgs( margs )
    if not result[ 'OK' ]:
      return result
    args, kwargs = result[ 'Value' ]
    if not self.__clientHasAccess( jid ):
      return S_ERROR( "You're not authorized to access jid %s" % jid )
    return getattr( self, "_usr_def_%s" % method )( jid, *args, **kwargs )

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
      self.log.verbose( "%s is job admin of jid %s" % ( idString, jid ) )
      return True
    if credDict[ 'username' ] == ownerDict[ 'Owner' ]:
      self.log.verbose( "%s is owner of jid %s" % ( idString, jid ) )
      return True
    if Properties.JOB_SHARING in credDict[ 'properties' ] and \
        credDict[ 'group' ] == ownerDict[ 'OwnerGroup' ]:
      self.log.verbose( "%s is sharing group with jid %s" % ( idString, jid ) )
      return True
    self.log.verbose( "%s is NOT allowed to access jid %s" % ( idString, jid ) )
    return False

#Manifests

  auth_getManifest = "all"
  types_getManifest = [ ( types.IntType, types.LongType ) ]
  def export_getManifest( self, jid ):
    return self.__getJobState( jid ).getManifest( rawData = True )

