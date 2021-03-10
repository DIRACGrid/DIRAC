"""
Do the initial configuration of a DIRAC component
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.registerArgument(" ComponentType:    Name of the ComponentType (ie: agent)")
Script.registerArgument(("System/Component: Full component name (ie: WorkloadManagement/Matcher)",
                         "System:           Name of the DIRAC system (ie: WorkloadManagement)"))
Script.registerArgument(" Component:        Name of the DIRAC service (ie: Matcher)", mandatory=False)

Script.parseCommandLine()
args = Script.getPositionalArgs()

componentType = args[0]

if len(args) == 2:
  system, component = args[1].split('/')
else:
  system = args[1]
  component = args[2]

# imports
from DIRAC import gConfig
from DIRAC import exit as DIRACexit

from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
#

gComponentInstaller.exitOnError = True

result = gComponentInstaller.addDefaultOptionsToCS(gConfig, componentType, system, component,
                                                   getCSExtensions(),
                                                   specialOptions={},
                                                   overwrite=False)
if not result['OK']:
  print("ERROR:", result['Message'])
else:
  DIRACexit()
