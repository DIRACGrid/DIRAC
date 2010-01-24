# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/Utilities/ResolveCatalog.py $
__RCSID__ = "$Id: ResolveCatalog.py 18436 2009-11-20 14:49:10Z acsmith $"

"""  The ResolveCatalog module performs the necessary CS gymnastics to resolve the closest LFC instance for the current location """

from DIRAC                                              import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.CountryMapping                import getCountryMappingTier1
from DIRAC.Core.Utilities.List                          import randomize 

def getActiveCatalogs():
  res = gConfig.getSections("/Resources/FileCatalogs/LcgFileCatalogCombined")
  if not res['OK']:
    gLogger.error("Failed to get Active Catalogs","%s" % res['Message'] )
    return res
  readDict = {}
  for site in res['Value']:
    res = gConfig.getOptionsDict("/Resources/FileCatalogs/LcgFileCatalogCombined/%s" % site)
    if not res['OK']:
      gLogger.error("Failed to get Tier1 catalog options","%s %s" % (site,res['Message']))
      continue
    siteDict = res['Value']
    if siteDict['Status'] == 'Active':
      readDict[site] = siteDict['ReadOnly']
  return S_OK(readDict)
      
def getLocationOrderedCatalogs(siteName=''):
  # First get a list of the active catalogs and their location
  res = getActiveCatalogs()
  if not res['OK']:
    gLogger.error("Failed to get list of active catalogs",res['Message'])
    return res
  catalogDict = res['Value']
  # Get the tier1 associated to the current location
  if not siteName:
    import DIRAC
    siteName = DIRAC.siteName()
  countryCode = siteName.split('.')[-1]
  res = getCountryMappingTier1(countryCode)
  if not res['OK']:
    gLogger.error("Failed to resolve closest Tier1",res['Message'])
    return res
  tier1 = res['Value']
  # Create a sorted list of the active readonly catalogs
  catalogList = []    
  if catalogDict.has_key(tier1):
    catalogList.append(catalogDict[tier1])
    catalogDict.pop(tier1)
  for catalogURL in randomize(catalogDict.values()): 
    catalogList.append(catalogURL)    
  return S_OK(catalogList)
