
import types
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.WorkloadManagementSystem.Client.Job.JobManifest import JobManifest
from DIRAC.Core.DISET.RPCClient import RPCClient

class JobState( object ):

  __jobDB = None

  def __init__( self, jid ):
    self.__jid = jid
    self.__manifest = False
    #Init DB if there
    if JobState == None:
      try:
        from DIRAC.WorkloadManagementSystem.DN.JobDB import JobDB
        JobState.__jobDB = JobDB()
        result = JobState.__jobDB.getConnection()
        if not result[ 'OK' ]:
          gLogger.warn( "Could not connect to JobDB (%s). Resorting to RPC" % result[ 'Message' ] )
        JobState.__jobDB = False
      except ImportError:
        JobState.__jobDB = False

  @property
  def jid( self ):
    return self.__jid

  def __getStore( self ):
    if JobState.__jobDB:
      return ( JobState.__jobDB, True )
    return ( RPCClient( "WorkloadManagement/JobStore" ), False )


  def getManifest( self ):
    store, local = self.__getStore()
    if not local:
      return store.getManifest( self.__jid )
    #Remote is getManifest, local is setJobJDL directly to the DB
    #TODO: Automate!
    return self.__getStore().getManifest( self.__jid )

  def setManifest( self, jobManifest ):

#
# Attributes
# 

  def setStatus( self, majorStatus, minorStatus ):
    if JobState.__jobDB:
      source = JobState.__jobDB
    else:
      source = RPCClient( "WorkloadManagement/JobStateUpdate" )
    return source.setJobStatus( self.__jid, majorStatus, minorStatus )


  def getStatus( self ):
    if JobState.__jobDB:
      source = JobState.__jobDB
      return source.getAttributesForJobList( jobIDs, [ 'Status', 'MinorStatus'] )
    result = RPCClient( "WorkloadManagement/JobMonitoring" )
    if not result[ 'OK' ]:
      return result
    print result[ 'Value' ]


  @TracedMethod
  def setMinorStatus( self, minorStatus ):
    self.__attrCache[ 'minorStatus' ] = minorStatus
    #TODO: Sync DB

