#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/WorkloadManagementSystem/private/DIRACPilotDirector.py $
# File :   test_wrapperScript.py
# Author : Ricardo Graciani
########################################################################
#
#  Testing DIRAC.Resources.Computing.Pilot.writeScript
#  Example:
#   python test_wrapperScript.py | tee script.py && chmod +x script.py && ./script.py
#
__RCSID__ = "$Id: DIRACPilotDirector.py 28536 2010-09-23 06:08:40Z rgracian $"

from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyGeneration import CLIParams, generateProxy
from DIRAC.Core.Security.Locations import getProxyLocation
from DIRAC.Core.Security.X509Chain import X509Chain

Script.disableCS()
Script.parseCommandLine()

proxyFile = getProxyLocation()
if not proxyFile:
  retVal = generateProxy(CLIParams())
  if not retVal['OK']:
    proxy = None
  else:
    proxy = X509Chain()
    proxy.loadChainFromFile(retVal['Value'])
else:
  proxy = X509Chain()
  proxy.loadChainFromFile(proxyFile)

from DIRAC.Resources.Computing import Pilot
import os

pilotFile = Pilot.__file__

print Pilot.wrapperScript( 'python' ,
                           arguments=['-c','import Pilot,os;print Pilot.__file__;print os.getcwd();os.system("ls -la")'],
                           proxy = proxy,
                           sandboxDict = {'test.py':'test.py', os.path.basename( pilotFile ) : pilotFile },
                           environDict = {'HOME': '/tmp'},
                           execDir='$HOME' )
