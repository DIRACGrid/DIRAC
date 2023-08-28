"""
Do the initial configuration of a DIRAC component
"""
from DIRAC.Core.Base.Script import Script

Script.setUsageMessage(
    "\n".join(
        [
            __doc__.split("\n")[1],
            "Usage:",
            f"  {Script.scriptName} [options] ... ComponentType System Component|System/Component",
            "Arguments:",
            "  ComponentType:  Name of the ComponentType (ie: agent)",
            "  System:  Name of the DIRAC system (ie: WorkloadManagement)",
            "  component:   Name of the DIRAC component (ie: JobCleaningAgent)",
        ]
    )
)
Script.parseCommandLine()
args = Script.getPositionalArgs()

componentType = args[0]

if len(args) == 2:
    system, component = args[1].split("/")
else:
    system = args[1]
    component = args[2]

# imports
from DIRAC import gConfig
from DIRAC import exit as DIRACexit

from DIRAC.Core.Utilities.Extensions import extensionsByPriority
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

#

gComponentInstaller.exitOnError = True

result = gComponentInstaller.addDefaultOptionsToCS(
    gConfig, componentType, system, component, extensionsByPriority(), specialOptions={}, overwrite=False
)
if not result["OK"]:
    print("ERROR:", result["Message"])
else:
    DIRACexit()
