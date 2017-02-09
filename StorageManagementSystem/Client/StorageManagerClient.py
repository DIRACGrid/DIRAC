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

  lfnListReplicas = dm.getReplicasForJobs( lfnList, getUrl = False )
  if not lfnListReplicas['OK']:
    return lfnListReplicas

  seToLFNs = dict()

  if lfnListReplicas['Value']['Failed']:
    return S_ERROR( "Failures in getting replicas" )

  lfnListReplicas = lfnListReplicas['Value']['Successful']
  # Check whether there is any file that is only at a tape SE
  # If a file is reported here at a tape SE, it is not at a disk SE as we use disk in priority
  for lfn, ld in lfnListReplicas.iteritems():
    for se in ld:
      status = StorageElement( se ).getStatus()
      if status.get( 'Value', {} ).get( 'DiskSE', False ):
        # File is not at a tape SE, no need to stage
        onlineLFNs.add( lfn )
        break
      else:
        seToLFNs.setdefault( se, list() ).append( lfn )

  if seToLFNs:
    # Only check on storage if it is a tape SE
    failed = {}
    for se, lfnsInSEList in seToLFNs.iteritems():
      fileMetadata = StorageElement( se ).getFileMetadata( lfnsInSEList )
      if not fileMetadata['OK']:
        failed[se] = dict.fromkeys( lfnsInSEList, fileMetadata['Message'] )
      else:
        if fileMetadata['Value']['Failed']:
          failed[se] = fileMetadata['Value']['Failed']
        # is there at least one online?
        for lfn, mDict in fileMetadata['Value']['Successful'].iteritems():
          if 'Cached' not in mDict:
            failed.setdefault( se, {} )[lfn] = 'No Cached item returned as metadata'
          elif mDict['Cached']:
            onlineLFNs.add( lfn )

    # If the file was found staged, ignore possible errors, but print out errors
    for se, failedLfns in failed.items():
      gLogger.error( "Errors when getting files metadata", 'at %s' % se )
      for lfn, reason in failedLfns.items():
        if lfn in onlineLFNs:
          gLogger.info( '%s: %s, but there is an online replica' % ( lfn, reason ) )
          failed[se].pop( lfn )
        else:
          gLogger.info( '%s: %s, no online replicas' % ( lfn, reason ) )
      if not failed[se]:
        failed.pop( se )
    if failed:
      gLogger.error( "Could not get metadata", "for %d files" % len( set( [lfn for lfnList in failed.itervalues() for lfn in lfnList] ) ) )
      return S_ERROR( "Could not get metadata for files" )
    offlineLFNs = set( lfnList ) - onlineLFNs

    for offlineLFN in offlineLFNs:
      ses = lfnListReplicas['Value']['Successful'][offlineLFN].keys()
      if ses:
        offlineLFNsDict.setdefault( random.choice( ses ), list() ).append( offlineLFN )

  return S_OK( {'onlineLFNs':list( onlineLFNs ), 'offlineLFNs': offlineLFNsDict} )


class StorageManagerClient( Client ):
  """ This is the client to the StorageManager service, so even if it is not seen, it exposes all its RPC calls
  """

  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( 'StorageManagement/StorageManager' )
