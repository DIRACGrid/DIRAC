# $HeadURL$
"""  The SitesDIRACGOCDBmapping module performs the necessary CS gymnastics to
     resolve sites DIRAC-GOCDB names.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""
__RCSID__ = "$Id$"

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath

#############################################################################

def getGOCSiteName( diracSiteName ):
  """
  Get GOC DB site name, given the DIRAC site name, as it stored in the CS

  :params:
    :attr:`diracSiteName` - string: DIRAC site name (e.g. 'LCG.CERN.ch')
  """
  gocDBName = gConfig.getValue( '/Resources/Sites/%s/%s/Name' % ( diracSiteName.split( '.' )[0],
                                                          diracSiteName ) )
  if not gocDBName:
    return S_ERROR( "No GOC site name for %s in CS (Not a grid site ?)" % diracSiteName )
  else:
    return S_OK( gocDBName )
  
  
def getGOCFTSName( diracFTSName ):
  """
  Get GOC DB FTS server URL, given the DIRAC FTS server name, as it stored in the CS

  :params:
    :attr:`diracFTSName` - string: DIRAC FTS server name (e.g. 'CERN-FTS3')
  """
  
  csPath = "/Resources/FTSEndpoints/FTS3"
  gocFTSName = gConfig.getValue( "%s/%s" % (csPath, diracFTSName) )
  if not gocFTSName:
    return S_ERROR( "No GOC FTS server name for %s in CS (Not a grid site ?)" % diracFTSName )
  else:
    return S_OK( gocFTSName )
   

#############################################################################

def getDIRACSiteName( gocSiteName ):
  """
  Get DIRAC site name, given the GOC DB site name, as it stored in the CS

  :params:
    :attr:`gocSiteName` - string: GOC DB site name (e.g. 'CERN-PROD')
  """
  diracSites = []
  result = gConfig.getSections( "/Resources/Sites" )
  if not result['OK']:
    return result
  gridList = result['Value']
  for grid in gridList:
    result = gConfig.getSections( "/Resources/Sites/%s" % grid )
    if not result['OK']:
      return result
    sitesList = result['Value']    
    tmpList = [(site, gConfig.getValue( "/Resources/Sites/%s/%s/Name" % ( grid, site ) ) ) for site in sitesList]
    diracSites += [dirac for (dirac, goc) in tmpList if goc == gocSiteName]

  if diracSites:
    return S_OK( diracSites )

  return S_ERROR( "There's no site with GOCDB name = %s in DIRAC CS" % gocSiteName )

def getDIRACSesForSRM( srmService ):
    
  result = gConfig.getSections( "/Resources/StorageElements" )
  if not result['OK']:
    return result
  diracSEs = result['Value']

  resultDIRACSEs = []
  for se in diracSEs:
    seSection = "/Resources/StorageElements/%s" % se
    result = gConfig.getSections( seSection )
    if not result['OK']:\
      continue
    accesses = result['Value']
    for access in accesses:
      protocol = gConfig.getValue( cfgPath( seSection, access, 'Protocol'), 'Unknown' )
      if protocol == 'srm':
        seHost = gConfig.getValue( cfgPath( seSection, access, 'Host'), 'Unknown' )
        if seHost == srmService:
          resultDIRACSEs.append( se )
          
  return S_OK( resultDIRACSEs )         
  

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
