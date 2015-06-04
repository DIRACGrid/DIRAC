""" Class that contains client access to the StorageManagerDB handler.
"""
__RCSID__ = "$Id$"

import random

from DIRAC import S_OK, S_ERROR 
from DIRAC.Core.Base.Client import Client
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Storage.StorageElement import StorageElement

def getFilesToStage( lfnList ):
  """ Utility that returns out of a list of LFNs those files that are offline,
      and those for which at least one copy is online
  """
  dm = DataManager()

  lfnListReplicas = dm.getActiveReplicas( lfnList )
  if not lfnListReplicas['OK']:
    return lfnListReplicas

  seObjectsDict = dict()
  seToLFNs = dict()
  
  if lfnListReplicas['Value']['Failed']:
    return S_ERROR( "Failures in getting replicas" )
  for lfn, ld in lfnListReplicas['Value']['Successful'].iteritems():
    for se, _ in ld.iteritems():
      seObjectsDict.setdefault( se, StorageElement( se ) )
      seToLFNs.setdefault( se, list() ).append( lfn )

  onlineLFNs = set()

  for se, lfnsInSEList in seToLFNs.iteritems():
    fileMetadata = seObjectsDict[se].getFileMetadata( lfnsInSEList )
    if not fileMetadata['OK']:
      return fileMetadata

    if fileMetadata['Value']['Failed']:
      return S_ERROR( "Failures in getting file metadata" )
    # is there at least one online?
    for lfn, mDict in fileMetadata['Value']['Successful'].iteritems():
      if mDict['Cached']:
        onlineLFNs.add( lfn )

  offlineLFNs = set( lfnList ).difference( onlineLFNs )
  
  offlineLFNsDict = {}
  for offlineLFN in offlineLFNs:
    ses = lfnListReplicas['Value']['Successful'][offlineLFN].keys()
    random.shuffle( ses )
    se = ses[0]
    offlineLFNsDict.setdefault( se, list() ).append( offlineLFN )
  
  return S_OK( {'onlineLFNs':list( onlineLFNs ), 'offlineLFNs': offlineLFNsDict} )


class StorageManagerClient( Client ):

  def __init__( self, **kwargs ):
    Client.__init__( self, **kwargs )
    self.setServer( 'StorageManagement/StorageManager' )
