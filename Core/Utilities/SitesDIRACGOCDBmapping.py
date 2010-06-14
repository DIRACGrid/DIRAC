"""  The SitesDIRACGOCDBmapping module performs the necessary CS gymnastics to
     resolve sites DIRAC-GOCDB names.ù
     
     Assumes CS structure of: /Resources/Sites/<GRIDNAME>/<SITENAME>
"""

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

#############################################################################

def getGOCSiteName(diracSiteName):
  """
  Get GOC DB site name, given the DIRAC site name, as it stored in the CS
  
  :params:
    :attr:`diracSiteName` - string: DIRAC site name (e.g. 'LCG.CERN.ch')
  """
  GOCDBName = gConfig.getValue('/Resources/Sites/%s/%s/Name' %(diracSiteName.split('.')[0], 
                                                          diracSiteName))
  if GOCDBName == '':
    return S_ERROR("There's no site with DIRAC name = %s in DIRAC CS" %diracSiteName)
  else:
    return S_OK(GOCDBName)

#############################################################################

def getDIRACSiteName(GOCSiteName):
  """
  Get DIRAC site name, given the GOC DB site name, as it stored in the CS
  
  :params:
    :attr:`GOCSiteName` - string: GOC DB site name (e.g. 'CERN-PROD')
  """
  sitesList = gConfig.getSections("/Resources/Sites/LCG/")
  if not sitesList['OK']:
    gLogger.warn('Problem retrieving sections in /Resources/Sites/LCG')
    return sitesList
  
  sitesList = sitesList['Value']
  for site in sitesList:
    GOCName = gConfig.getValue("/Resources/Sites/LCG/%s/Name" %site)
    if GOCName == GOCSiteName:
      return S_OK(site)
  
  return S_ERROR("There's no site with GOCDB name = %s in DIRAC CS" %GOCSiteName)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#