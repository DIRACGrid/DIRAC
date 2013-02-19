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
    self.__parametricManifests = False

  def setParametricManifests( self, manifests ):
    self.__parametricManifests = manifests

  @property
  def jobState( self ):
    return self.__jobState

  def serialize( self ):
    stub = { 'job': self.__jobState.serialize() }
    if self.__parametricManifests:
      stub[ 'manifests' ] = self.__parametricManifests
    return DEncode.encode( stub )

  @classmethod
  def deserialize( cls, stub ):
    stub = DEncode.decode( stub )[0]
    result = CachedJobState.deserialize( stub[ 'job' ] )
    if not result[ 'OK' ]:
      return result
    ot = cls( result[ 'Value' ] )
    if 'manifests' in stub:
      ot.setParametricManifests( stub[ 'manifests' ] )
    return S_OK( ot )
