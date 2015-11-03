""" Class that contains client access to the StorageManagerDB handler.
"""
__RCSID__ = "$Id$"

import random

from DIRAC import S_OK, S_ERROR, gLogger
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
      failed[se] = dict.fromkeys( lfnsInSEList, fileMetadata['Message'] )
    else:
      failed[se] = fileMetadata['Value']['Failed']
      # is there at least one online?
      for lfn, mDict in fileMetadata['Value']['Successful'].iteritems():
        if mDict['Cached']:
          onlineLFNs.add( lfn )

  # If the file was found staged, ignore possible errors, but print out errors
  if failed:
    for se, seFailed in failed.items():
      gLogger.error( "Errors when getting files metadata", 'at %s' % se )
      for lfn, reason in seFailed.items():
        gLogger.info( '%s: %s' % ( lfn, reason ) )
        if lfn in onlineLFNs:
          failed[se].pop( lfn )
      if not failed[se]:
        failed.pop( se )
    if failed:
      return S_ERROR( 'Could not get metadata for %d files' % \
                      len( set( [lfn for lfnList in failed.values() for lfn in lfnList] ) ) )
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
