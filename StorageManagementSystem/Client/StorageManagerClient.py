""" Class that contains client access to the StorageManagerDB handler.
"""
__RCSID__ = "$Id$"

import random

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Client                         import Client
from DIRAC.Core.Utilities.Proxy                     import executeWithUserProxy
from DIRAC.DataManagementSystem.Client.DataManager  import DataManager
from DIRAC.Resources.Storage.StorageElement         import StorageElement

@executeWithUserProxy
def getFilesToStage( lfnList ):
  """ Utility that returns out of a list of LFNs those files that are offline,
      and those for which at least one copy is online
  """
  onlineLFNs = set()
  offlineLFNsDict = {}

  if not lfnList:
    return S_OK( {'onlineLFNs':list( onlineLFNs ), 'offlineLFNs': offlineLFNsDict} )

  dm = DataManager()

  lfnListReplicas = dm.getActiveReplicas( lfnList, getUrl = False )
  if not lfnListReplicas['OK']:
    return lfnListReplicas

  seToLFNs = dict()

  if lfnListReplicas['Value']['Failed']:
    return S_ERROR( "Failures in getting replicas" )
  for lfn, ld in lfnListReplicas['Value']['Successful'].iteritems():
    for se in ld:
      seToLFNs.setdefault( se, list() ).append( lfn )

  failed = {}
  for se, lfnsInSEList in seToLFNs.iteritems():
    fileMetadata = StorageElement( se ).getFileMetadata( lfnsInSEList )
    if not fileMetadata['OK']:
      failed.update( dict.fromkeys( lfnsInSEList, fileMetadata['Message'] ) )
    else:
      failed.update( fileMetadata['Value']['Failed'] )
      # is there at least one online?
      for lfn, mDict in fileMetadata['Value']['Successful'].iteritems():
        if mDict['Cached']:
          onlineLFNs.add( lfn )

  # If the file was found staged, ignore possible errors
  for lfn in set( failed ) & onlineLFNs:
    failed.pop( lfn )
  if failed:
    reasons = sorted( set( failed.values() ) )
    return S_ERROR( 'Could not get metadata for %d files: %s' % ( len( failed ), ','.join( reasons ) ) )
  offlineLFNs = set( lfnList ) - onlineLFNs


  for offlineLFN in offlineLFNs:
    ses = lfnListReplicas['Value']['Successful'][offlineLFN].keys()
    random.shuffle( ses )
    se = ses[0]
    offlineLFNsDict.setdefault( se, list() ).append( offlineLFN )

  return S_OK( {'onlineLFNs':list( onlineLFNs ), 'offlineLFNs': offlineLFNsDict} )


class StorageManagerClient( Client ):
  """ This is the client to the StorageManager service, so even if it is not seen, it exposes all its RPC calls
  """

  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( 'StorageManagement/StorageManager' )
