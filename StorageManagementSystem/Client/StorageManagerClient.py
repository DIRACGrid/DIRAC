""" Class that contains client access to the StorageManagerDB handler.
"""
__RCSID__ = "$Id$"

import random
import errno

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.Client                         import Client
from DIRAC.Core.Utilities.DErrno                    import cmpError
from DIRAC.Core.Utilities.Proxy                     import executeWithUserProxy
from DIRAC.DataManagementSystem.Client.DataManager  import DataManager
from DIRAC.Resources.Storage.StorageElement         import StorageElement

def getFilesToStage( lfnList, jobState = None, checkOnlyTapeSEs = None, jobLog = None ):
  """ Utility that returns out of a list of LFNs those files that are offline,
      and those for which at least one copy is online
  """
  if not lfnList:
    return S_OK( {'onlineLFNs':[], 'offlineLFNs': {}, 'failedLFNs':[], 'absentLFNs':{}} )

  dm = DataManager()

  lfnListReplicas = dm.getReplicasForJobs( lfnList, getUrl = False )
  if not lfnListReplicas['OK']:
    return lfnListReplicas

  if lfnListReplicas['Value']['Failed']:
    return S_ERROR( "Failures in getting replicas" )

  lfnListReplicas = lfnListReplicas['Value']['Successful']
  # If a file is reported here at a tape SE, it is not at a disk SE as we use disk in priority
  # We shall check all file anyway in order to make sure they exist
  seToLFNs = dict()
  for lfn, ses in lfnListReplicas.iteritems():
    for se in ses:
      seToLFNs.setdefault( se, list() ).append( lfn )

  offlineLFNsDict = {}
  onlineLFNs = set()
  offlineLFNs = {}
  absentLFNs = {}
  if seToLFNs:
    if jobState:
      # Get user name and group from the job state
      userName = jobState.getAttribute( 'Owner' )
      if not userName[ 'OK' ]:
        return userName
      userName = userName['Value']

      userGroup = jobState.getAttribute( 'OwnerGroup' )
      if not userGroup[ 'OK' ]:
        return userGroup
      userGroup = userGroup['Value']
    else:
      userName = None
      userGroup = None
    # Check whether files are Online or Offline, or missing at SE
    result = _checkFilesToStage( seToLFNs, onlineLFNs, offlineLFNs, absentLFNs,  # pylint: disable=unexpected-keyword-arg
                                 checkOnlyTapeSEs = checkOnlyTapeSEs, jobLog = jobLog,
                                 proxyUserName = userName,
                                 proxyUserGroup = userGroup,
                                 executionLock = True )

    if not result['OK']:
      return result
    failedLFNs = set( lfnList ) - onlineLFNs - set( offlineLFNs ) - set( absentLFNs )

    for lfn in offlineLFNs:
      ses = offlineLFNs[lfn]
      if ses:
        offlineLFNsDict.setdefault( random.choice( ses ), list() ).append( lfn )

  return S_OK( {'onlineLFNs':list( onlineLFNs ), 'offlineLFNs': offlineLFNsDict, 'failedLFNs':list( failedLFNs ), 'absentLFNs':absentLFNs} )

@executeWithUserProxy
def _checkFilesToStage( seToLFNs, onlineLFNs, offlineLFNs, absentLFNs, checkOnlyTapeSEs = None, jobLog = None ):
  """
  Checks on SEs whether the file is NEARLINE or ONLINE
  onlineLFNs is modified to contain the files found online
  """
  # Only check on storage if it is a tape SE
  if jobLog is None:
    logger = gLogger
  else:
    logger = jobLog
  if checkOnlyTapeSEs is None:
    # Default value is True
    checkOnlyTapeSEs = True

  failed = {}
  for se, lfnsInSEList in seToLFNs.iteritems():
    seObj = StorageElement( se )
    status = seObj.getStatus()
    if not status['OK']:
      logger.error( "Could not get SE status", "%s - %s" % ( se, status['Message'] ) )
      return status
    tapeSE = status['Value']['TapeSE']
    # If requested to check only Tape SEs and  the file is at a diskSE, we guess it is Online...
    if checkOnlyTapeSEs and status['Value']['DiskSE']:
      onlineLFNs.update( lfnsInSEList )
      continue

    fileMetadata = seObj.getFileMetadata( lfnsInSEList )
    if not fileMetadata['OK']:
      failed[se] = dict.fromkeys( lfnsInSEList, fileMetadata['Message'] )
    else:
      if fileMetadata['Value']['Failed']:
        failed[se] = fileMetadata['Value']['Failed']
      # is there at least one replica online?
      for lfn, mDict in fileMetadata['Value']['Successful'].iteritems():
        # SRM returns Cached, but others may only return Accessible
        if mDict.get( 'Cached', mDict['Accessible'] ):
          onlineLFNs.add( lfn )
        elif tapeSE:
          # A file can be staged only at Tape SE
          offlineLFNs.setdefault( lfn, [] ).append( se )
        else:
          # File not available at a diskSE... we shall retry later
          pass

  # Doesn't matter if some files are Offline if they are also online
  for lfn in set( offlineLFNs ) & onlineLFNs:
    offlineLFNs.pop( lfn )

  # If the file was found staged, ignore possible errors, but print out errors
  for se, failedLfns in failed.items():
    logger.error( "Errors when getting files metadata", 'at %s' % se )
    for lfn, reason in failedLfns.items():
      if lfn in onlineLFNs:
        logger.info( '%s: %s, but there is an online replica' % ( lfn, reason ) )
        failed[se].pop( lfn )
      else:
        logger.error( '%s: %s, no online replicas' % ( lfn, reason ) )
        if cmpError( reason, errno.ENOENT ):
          absentLFNs.setdefault( lfn, [] ).append( se )
          failed[se].pop( lfn )
    if not failed[se]:
      failed.pop( se )
  # Find the files that do not exist at SE
  if failed:
    logger.error( "Error getting metadata", "for %d files" % len( set( lfn for lfnList in failed.itervalues() for lfn in lfnList ) ) )

  return S_OK()


class StorageManagerClient( Client ):
  """ This is the client to the StorageManager service, so even if it is not seen, it exposes all its RPC calls
  """

  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( 'StorageManagement/StorageManager' )
