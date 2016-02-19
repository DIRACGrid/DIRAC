"""
Do the initial configuration of a DIRAC component
"""
__RCSID__ = "$Id$"
#
from DIRAC.Core.Utilities import InstallTools
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
#
from DIRAC import gConfig
from DIRAC import exit as DIRACexit
InstallTools.exitOnError = True
#
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... ComponentType System Component|System/Component' % Script.scriptName,
                                    'Arguments:',
                                    '  ComponentType:  Name of the ComponentType (ie: agent)',
                                    '  System:  Name of the DIRAC system (ie: WorkloadManagement)',
                                    '  component:   Name of the DIRAC component (ie: JobCleaningAgent)'] ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()

componentType = args[0]

if len( args ) == 2:
  system, component = args[1].split( '/' )
else:
  system = args[1]
  component = args[2]

result = InstallTools.addDefaultOptionsToCS( gConfig, componentType, system, component,
                                             getCSExtensions(),
                                             specialOptions = {},
                                             overwrite = False )
if not result['OK']:
  print "ERROR:", result['Message']
else:
  DIRACexit()
