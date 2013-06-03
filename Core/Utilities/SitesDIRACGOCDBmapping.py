# $HeadURL$
"""  The SitesDIRACGOCDBmapping module performs the necessary CS gymnastics to
     resolve sites DIRAC-GOCDB names.

     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""
__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

#############################################################################

def getGOCSiteName( diracSiteName ):
  """
  Get GOC DB site name, given the DIRAC site name, as it stored in the CS

  :params:
    :attr:`diracSiteName` - string: DIRAC site name (e.g. 'LCG.CERN.ch')
  """
#  gocDBName = gConfig.getValue( '/Resources/Sites/%s/%s/Name' % ( diracSiteName.split( '.' )[0],
#                                                          diracSiteName ) )
  gocDBName = gConfig.getValue( '/Resources/Sites/%s/Name' % diracSiteName )
  
  if not gocDBName:
    return S_ERROR( "No GOC site name for %s in CS (Not a LCG site ?)" % diracSiteName )
  else:
    return S_OK( gocDBName )

#############################################################################

def getDIRACSiteName( gocSiteName ):
  """
  Get DIRAC site name, given the GOC DB site name, as it stored in the CS

  :params:
    :attr:`gocSiteName` - string: GOC DB site name (e.g. 'CERN-PROD')
  """
  sitesList = gConfig.getSections( "/Resources/Sites/LCG/" )
  if not sitesList['OK']:
    gLogger.warn( 'Problem retrieving sections in /Resources/Sites/LCG' )
    return sitesList

  sitesList = sitesList['Value']
  diracSites = [(site, gConfig.getValue( "/Resources/Sites/LCG/%s/Name" % site )) for site in sitesList]
  diracSites = [dirac for (dirac, goc) in diracSites if goc == gocSiteName]

  if diracSites:
    return S_OK( diracSites )

  return S_ERROR( "There's no site with GOCDB name = %s in DIRAC CS" % gocSiteName )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
