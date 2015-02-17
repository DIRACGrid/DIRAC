"""
  This module contains helper methods for accessing operational attributes or parameters of DMS objects

"""

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import gConfig, gLogger

def _resolveSEGroup( seGroupList ):
  seList = []
  for se in seGroupList:
    seConfig = gConfig.getValue( '/Resources/StorageElementGroups/%s' % se, se )
    if seConfig != se:
      seList += [se.strip() for se in seConfig.split( ',' )]
      # print seList
    else:
      seList.append( se )
    res = gConfig.getSections( '/Resources/StorageElements' )
    if not res['OK']:
      gLogger.fatal( 'Error getting list of SEs from CS', res['Message'] )
      return []
    for se in seList:
      if se not in res['Value']:
        gLogger.fatal( '%s is not a valid SE' % se )
        seList = []
        break

  return seList


def isSEFailover( storageElement ):
  seList = Operations().getValue( 'DataManagement/SEsUsedForFailover', [] )
  return storageElement in _resolveSEGroup( seList )

def isSEForJobs( storageElement ):
  seList = Operations().getValue( 'DataManagement/SEsNotToBeUsedForJobs', [] )
  return storageElement not in _resolveSEGroup( seList )
