#!/usr/bin/env python
"""
Do the initial installation and configuration of a DIRAC component
"""
from DIRAC import S_OK
from DIRAC import exit as DIRACexit
from DIRAC import gConfig, gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.Extensions import extensionsByPriority
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities


class Params:
    """Class holding the parameters, and callbacks for their respective switches."""

    def __init__(self) -> None:
        """C'or"""
        self.overwrite = False
        self.module = ""
        self.specialOptions = {}

        self.switches = [
            ("w", "overwrite", "Overwrite the configuration in the global CS", self.setOverwrite),
            ("m:", "module=", "Python module name for the component code", self.setModule),
            ("p:", "parameter=", "Special component option ", self.setSpecialOption),
        ]

    def setOverwrite(self):
        self.overwrite = True
        return S_OK()

    def setModule(self, value):
        self.specialOptions["Module"] = value
        self.module = value
        return S_OK()

    def setSpecialOption(self, value):
        option, val = value.split("=")
        self.specialOptions[option] = val
        return S_OK()


@Script()
def main():
    params = Params()

    Script.registerSwitches(params.switches)
    Script.registerArgument(
        (
            "System/Component: Full component name (ie: WorkloadManagement/JobMonitoring)",
            "System:           Name of the DIRAC system (ie: WorkloadManagement)",
        )
    )
    Script.registerArgument(" Component:        Name of the DIRAC service (ie: JobMonitoring)", mandatory=False)

    Script.parseCommandLine(ignoreErrors=False)
    args = Script.getPositionalArgs()

    if not args or len(args) > 2:
        Script.showHelp(exitCode=1)

    # System/Component
    if len(args) == 1:
        args = args[0].split("/")

    system = args[0]
    component = args[1]
    compOrMod = params.module or component

    # Now actually doing things
    from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

    gComponentInstaller.exitOnError = True

    result = gComponentInstaller.getSoftwareComponents(extensionsByPriority())
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACexit(1)
    availableComponents = result["Value"]

    for compType in availableComponents:
        if system in availableComponents[compType] and compOrMod in availableComponents[compType][system]:
            cType = compType[:-1].lower()
            break
    else:
        gLogger.error(f"Component {system}/{component} is not available for installation")
        DIRACexit(1)

    if params.module:
        result = gComponentInstaller.addDefaultOptionsToCS(
            gConfig, cType, system, params.module, extensionsByPriority(), overwrite=params.overwrite
        )
        result = gComponentInstaller.addDefaultOptionsToCS(
            gConfig,
            cType,
            system,
            component,
            extensionsByPriority(),
            specialOptions=params.specialOptions,
            overwrite=params.overwrite,
            addDefaultOptions=False,
        )
    else:
        result = gComponentInstaller.addDefaultOptionsToCS(
            gConfig,
            cType,
            system,
            component,
            extensionsByPriority(),
            specialOptions=params.specialOptions,
            overwrite=params.overwrite,
        )

    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACexit(1)

    if component.startswith("Tornado"):
        result = gComponentInstaller.installTornado()
        if not result["OK"]:
            gLogger.error(result["Message"])
            DIRACexit(1)
    else:
        result = gComponentInstaller.installComponent(cType, system, component, extensionsByPriority(), params.module)
        if not result["OK"]:
            gLogger.error(result["Message"])
            DIRACexit(1)

    gLogger.notice(f"Successfully installed component {component} in {system} system, now setting it up")

    if component.startswith("Tornado"):
        result = gComponentInstaller.setupTornadoService(system, component)
        if not result["OK"]:
            gLogger.error(result["Message"])
            DIRACexit(1)
    else:
        result = gComponentInstaller.setupComponent(cType, system, component, extensionsByPriority(), params.module)
        if not result["OK"]:
            gLogger.error(result["Message"])
            DIRACexit(1)

    if "ComponentMonitoring" in component:
        result = MonitoringUtilities.monitorInstallation("DB", system, "InstalledComponentsDB")
        if not result["OK"]:
            gLogger.error(result["Message"])
            DIRACexit(1)

    result = MonitoringUtilities.monitorInstallation(cType, system, component, params.module)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACexit(1)

    gLogger.notice(f"Successfully completed the installation of {system}/{component}")
    DIRACexit()


if __name__ == "__main__":
    main()
