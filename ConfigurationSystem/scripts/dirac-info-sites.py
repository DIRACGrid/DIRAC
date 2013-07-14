#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-info-sites
# Author : A.T.
########################################################################
"""
  Combined command to look up various resources information
"""
__RCSID__   = "$Id$"

import sys
import DIRAC
from DIRAC.Core.Base import Script

vo = ""
def setVO( args ):
  global vo
  vo = args
  return DIRAC.S_OK()

seFlag = False
def setSEFlag( args ):
  global seFlag
  seFlag = True
  return DIRAC.S_OK()

ceFlag = False
def setCEFlag( args ):
  global ceFlag
  ceFlag = True
  return DIRAC.S_OK()


Script.registerSwitch( "V:", "vo=", "choose resources eligible for the given VO", setVO )
Script.registerSwitch( "S", "se", "display storage element information", setSEFlag )
Script.registerSwitch( "C", "ce", "display computing element information", setSEFlag )
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources, getSites

resources = Resources ( vo = 'biomed' )

result = resources.getEligibleSites()
if not result['OK']:
  print "ERROR:", result['Message']

siteList = [ resources.getSiteFullName( site )['Value'] for site in result['Value'] ]

print siteList


