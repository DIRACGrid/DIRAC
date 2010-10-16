#!/usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################
__RCSID__ = "$Id:  $"

from pprint import pprint
import DIRAC
from DIRAC import gConfig
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

infoDict = {}

infoDict['Setup'] = gConfig.getValue('/DIRAC/Setup','Unknown')
infoDict['ConfigurationServer'] = gConfig.getValue('/DIRAC/Configuration/Servers',[])
infoDict['VirtualOrganization'] = gConfig.getValue('/DIRAC/VirtualOrganization','Unknown')

print 'DIRAC version'.rjust(20),':',DIRAC.version

for k,v in infoDict.items():
  print k.rjust(20),':',str(v)
