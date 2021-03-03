#!/usr/bin/env python
########################################################################
# File :   dirac-admin-add-site
# Author : Federico Stagni
########################################################################
"""
Add a new DIRAC SiteName to DIRAC Configuration, including one or more CEs.
If site is already in the CS with another name, error message will be produced.
If site is already in the CS with the right name, only new CEs will be added.

Usage:
  dirac-admin-add-site [options] ... DIRACSiteName GridSiteName CE [CE] ...

Arguments:
  DIRACSiteName:  Name of the site for DIRAC in the form GRID.LOCATION.COUNTRY (ie:LCG.CERN.ch)
  GridSiteName:   Name of the site in the Grid (ie: CERN-PROD)
  CE:             Name of the CE to be included in the site (ie: ce111.cern.ch)

Example:
  $ dirac-admin-add-site LCG.IN2P3.fr IN2P3-Site
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import six

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC import exit as DIRACExit, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACSiteName
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if len(args) < 3:
    Script.showHelp(exitCode=1)

  diracSiteName = args[0]
  gridSiteName = args[1]
  ces = args[2:]
  try:
    diracGridType, place, country = diracSiteName.split('.')
  except ValueError:
    gLogger.error("The DIRACSiteName should be of the form GRID.LOCATION.COUNTRY for example LCG.CERN.ch")
    DIRACExit(-1)

  result = getDIRACSiteName(gridSiteName)
  newSite = True
  if result['OK'] and result['Value']:
    if len(result['Value']) > 1:
      gLogger.notice('%s GOC site name is associated with several DIRAC sites:' % gridSiteName)
      for i, dSite in enumerate(result['Value']):
        gLogger.notice('%d: %s' % (i, dSite))
      inp = six.moves.input('Enter your choice number: ')
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
  else:
    gLogger.error("ERROR getting DIRAC site name of %s" % gridSiteName, result.get('Message'))

  csAPI = CSAPI()

  if newSite:
    gLogger.notice("Site to CS: %s" % diracSiteName)
    res = csAPI.addSite(diracSiteName, {"Name": gridSiteName})
    if not res['OK']:
      gLogger.error("Failed adding site to CS", res['Message'])
      DIRACExit(1)
    res = csAPI.commit()
    if not res['OK']:
      gLogger.error("Failure committing to CS", res['Message'])
      DIRACExit(3)

  for ce in ces:
    gLogger.notice("Adding CE %s" % ce)
    res = csAPI.addCEtoSite(diracSiteName, ce)
    if not res['OK']:
      gLogger.error("Failed adding CE %s to CS" % ce, res['Message'])
      DIRACExit(2)
    res = csAPI.commit()
    if not res['OK']:
      gLogger.error("Failure committing to CS", res['Message'])
      DIRACExit(3)


if __name__ == "__main__":
  main()
