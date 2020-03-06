#!/usr/bin/env python
########################################################################
# File :   dirac-admin-add-site
# Author : Andrew C. Smith
########################################################################
"""
  Add a new DIRAC SiteName to DIRAC Configuration, including one or more CEs.
  If site is already in the CS with another name, error message will be produced.
  If site is already in the CS with the right name, only new CEs will be added.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC import exit as DIRACExit, gLogger
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName

if __name__ == "__main__":

  Script.setUsageMessage(
      '\n'.join(
          [
              __doc__.split('\n')[1],
              'Usage:',
	      '  %s [option|cfgfile] ... DIRACSiteName GridSiteName ...' %
              Script.scriptName,
              'Arguments:',
              '  DIRACSiteName: Name of the site for DIRAC in the form GRID.LOCATION.COUNTRY (ie:LCG.CERN.ch)',
	      '  GridSiteName: Name of the site in the Grid (ie: CERN-PROD)']))
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  csAPI = CSAPI()

  if len(args) < 3:
    Script.showHelp()
    DIRACExit(-1)

  diracSiteName = args[0]
  gridSiteName = args[1]
  try:
    diracGridType, place, country = diracSiteName.split('.')
  except ValueError:
    gLogger.error("The DIRACSiteName should be of the form GRID.LOCATION.COUNTRY for example LCG.CERN.ch")
    DIRACExit(-1)

  result = getDIRACSiteName(gridSiteName)
  newSite = True
  if result['OK']:
    if result['Value']:
      if len(result['Value']) > 1:
        gLogger.notice('%s GOC site name is associated with several DIRAC sites:' % gridSiteName)
        for i, dSite in enumerate(result['Value']):
          gLogger.notice('%d: %s' % (i, dSite))
        inp = raw_input('Enter your choice number: ')
        try:
          inp = int(inp)
        except ValueError:
          gLogger.error('You should enter an integer number')
          DIRACExit(-1)
        if 0 <= inp < len(result['Value']):
          diracCSSite = result['Value'][inp]
        else:
          gLogger.error('Number out of range: %d' % inp)
          DIRACExit(-1)
      else:
        diracCSSite = result['Value'][0]
      if diracCSSite == diracSiteName:
        gLogger.notice('Site with GOC name %s is already defined as %s' % (gridSiteName, diracSiteName))
        newSite = False
      else:
        gLogger.error('ERROR: Site with GOC name %s is already defined as %s' % (gridSiteName, diracCSSite))
        DIRACExit(-1)

  cfgBase = "/Resources/Sites/%s/%s" % (diracGridType, diracSiteName)
  change = False
  if newSite:
    gLogger.notice("Adding new site to CS: %s" % diracSiteName)
    csAPI.setOption("%s/Name" % cfgBase, gridSiteName)
    change = True
  if change:
    res = csAPI.commitChanges()
    if not res['OK']:
      gLogger.error("Failed to commit changes to CS", res['Message'])
      DIRACExit(-1)
    gLogger.notice(
	"Successfully added site %s to the CS with name %s" %
	(diracSiteName, gridSiteName))
