import types
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState

class OptimizationTask( object ):

  def __init__( self, jid ):
    if isinstance( jid, CachedJobState ):
      self.__jobState = jid
    else:
      self.__jobState = CachedJobState( jid )
    self.__tqReady = False
    self.__parametricManifests = False

  def setParametricManifests( self, manifests ):
    self.__parametricManifests = manifests

  def setTQReady( self ):
    self.__tqReady = True

  @property
  def jobState( self ):
    return self.__jobState

  def serialize( self ):
    stub = { 'job': self.__jobState.serialize(), 'tq' : self.__tqReady }
    if self.__parametricManifests:
      stub[ 'manifests' ] = self.__parametricManifests
    return DEncode.encode( stub )

  @property
  def tqReady( self ):
    return self.__tqReady

  @classmethod
  def deserialize( cls, stub ):
    stub = DEncode.decode( stub )[0]
    result = CachedJobState.deserialize( stub[ 'job' ] )
    if not result[ 'OK' ]:
      return result
    ot = cls( result[ 'Value' ] )
    if 'manifests' in stub:
      ot.setParametricManifests( stub[ 'manifests' ] )
    if stub.get( 'tq', False ):
      ot.setTQReady()
    return S_OK( ot )
