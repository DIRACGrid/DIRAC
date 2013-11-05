from DIRAC import S_OK
from DIRAC.Core.Utilities import DEncode
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest

class OptimizationTask( object ):

  def __init__( self, jid ):
    if isinstance( jid, CachedJobState ):
      self.__jobState = jid
    else:
      self.__jobState = CachedJobState( jid )
    self.__tqReady = False
    self.__splitManifests = False

  def splitJobInto( self, manifests ):
    self.__splitManifests = manifests

  def setTQReady( self ):
    self.__tqReady = True

  def serialize( self ):
    stub = { 'job': self.__jobState.serialize(), 'tq' : self.__tqReady }
    if self.__splitManifests:
      stub[ 'manifests' ] = [ man.dumpAsCFG() for man in self.__splitManifests ]
    return DEncode.encode( stub )

  @property
  def tqReady( self ):
    return self.__tqReady

  @property
  def splitManifests( self ):
    return self.__splitManifests

  @property
  def jobState( self ):
    return self.__jobState

  @classmethod
  def deserialize( cls, stub ):
    stub = DEncode.decode( stub )[0]
    result = CachedJobState.deserialize( stub[ 'job' ] )
    if not result[ 'OK' ]:
      return result
    ot = cls( result[ 'Value' ] )
    if 'manifests' in stub:
      ot.splitJobInto( [ JobManifest( data ) for data in  stub[ 'manifests' ] ] )
    if stub.get( 'tq', False ):
      ot.setTQReady()
    return S_OK( ot )
