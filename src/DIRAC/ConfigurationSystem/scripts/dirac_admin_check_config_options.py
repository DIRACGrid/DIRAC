#!/usr/bin/env python
"""
Check configuration options against the defaults in the ConfigTemplate.cfg files.

This script can help to discover discrepancies in the configuration:

  - Typos in option names
  - Removed options
  - Missing authorization settings

This script should be run by dirac administrators after major updates.

Usage:
  dirac-admin-check-config-options [options] -[MAUO] [-S <system>]
"""
import os
from pprint import pformat

from diraccfg import CFG
from DIRAC import gLogger, S_ERROR, S_OK, gConfig
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.List import fromChar

LOG = gLogger


class CheckConfig:
    """Compare the ConfigTemplate with current configuration."""

    def __init__(self):
        self.systems = None
        self.showModified = False
        self.showAdded = False
        self.showMissingSections = False
        self.showMissingOptions = False

    def _setSystems(self, val):
        self.systems = fromChar(val)
        return S_OK()

    def _setShowModified(self, _):
        self.showModified = True
        return S_OK()

    def _setShowAdded(self, _):
        self.showAdded = True
        return S_OK()

    def _setShowMissingSections(self, _):
        self.showMissingSections = True
        return S_OK()

    def _setShowMissingOptions(self, _):
        self.showMissingOptions = True
        return S_OK()

    def _setSwitches(self):
        Script.registerSwitch("S:", "system=", "Systems to check, by default all of them are checked", self._setSystems)
        Script.registerSwitch("M", "modified", "Show entries which differ from the default", self._setShowModified)
        Script.registerSwitch("A", "added", "Show entries which do not exist in ConfigTemplate", self._setShowAdded)
        Script.registerSwitch(
            "U",
            "missingSection",
            "Show sections which do not exist in the current configuration",
            self._setShowMissingSections,
        )
        Script.registerSwitch(
            "O",
            "missingOption",
            "Show options which do not exist in the current configuration",
            self._setShowMissingOptions,
        )

        Script.parseCommandLine(ignoreErrors=True)
        if not any([self.showModified, self.showAdded, self.showMissingSections, self.showMissingOptions]):
            LOG.error("\nERROR: Set at least one of the flags M A U O")
            Script.showHelp()

    def _check(self):
        """Obtain default configuration and current configuration and print the diff."""
        cfg = CFG()
        templateLocations = self._findConfigTemplates()
        for templatePath in templateLocations:
            cfgRes = self._parseConfigTemplate(templatePath, cfg)
            if cfgRes["OK"]:
                cfg = cfgRes["Value"]

        currentCfg = self._getCurrentConfig()
        if not currentCfg["OK"]:
            return
        currentCfg = currentCfg["Value"]
        diff = currentCfg.getModifications(cfg, ignoreOrder=True, ignoreComments=True)

        LOG.debug("*" * 80)
        LOG.debug(f"Default Configuration: {str(cfg)}")
        LOG.debug("*" * 80)
        LOG.debug(f"Current Configuration: {str(currentCfg)} ")
        for entry in diff:
            self._printDiff(entry)

    def _parseConfigTemplate(self, templatePath, cfg=None):
        """Parse the ConfigTemplate.cfg files.

        :param str templatePath: path to the folder containing a ConfigTemplate.cfg file
        :param CFG cfg: cfg to merge with the systems config
        :returns: CFG object
        """
        cfg = CFG() if cfg is None else cfg

        system = os.path.split(templatePath.rstrip("/"))[1]
        if system.lower().endswith("system"):
            system = system[: -len("System")]

        if self.systems and system not in self.systems:
            return S_OK(cfg)

        templatePath = os.path.join(templatePath, "ConfigTemplate.cfg")
        if not os.path.exists(templatePath):
            return S_ERROR(f"File not found: {templatePath}")

        loadCfg = CFG()
        loadCfg.loadFromFile(templatePath)

        newCfg = CFG()
        newCfg.createNewSection(f"/{system}", contents=loadCfg)

        cfg = cfg.mergeWith(newCfg)

        return S_OK(cfg)

    @staticmethod
    def _findConfigTemplates():
        """Traverse folders in DIRAC and find ConfigTemplate.cfg files."""
        configTemplates = set()
        diracPath = os.environ.get("DIRAC")
        for baseDirectory, _subdirectories, files in os.walk(diracPath):
            if "ConfigTemplate.cfg" in files:
                configTemplates.add(baseDirectory)
        return configTemplates

    def _getCurrentConfig(self):
        """Return the current system configuration."""
        from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

        gConfig.forceRefresh()

        fullCfg = CFG()
        setup = gConfig.getValue("/DIRAC/Setup", "")
        setupList = gConfig.getSections("/DIRAC/Setups", [])
        if not setupList["OK"]:
            return S_ERROR("Could not get /DIRAC/Setups sections")
        setupList = setupList["Value"]
        if setup not in setupList:
            return S_ERROR(f"Setup {setup} is not in allowed list: {', '.join(setupList)}")
        serviceSetups = gConfig.getOptionsDict(f"/DIRAC/Setups/{setup}")
        if not serviceSetups["OK"]:
            return S_ERROR(f"Could not get /DIRAC/Setups/{setup} options")
        serviceSetups = serviceSetups["Value"]  # dict
        for system, setup in serviceSetups.items():
            if self.systems and system not in self.systems:
                continue
            systemCfg = gConfigurationData.remoteCFG.getAsCFG(f"/Systems/{system}/{setup}")
            for section in systemCfg.listSections():
                if section not in ("Agents", "Services", "Executors"):
                    systemCfg.deleteKey(section)

            fullCfg.createNewSection(f"/{system}", contents=systemCfg)

        return S_OK(fullCfg)

    def _printDiff(self, entry, level=""):
        """Format the changes."""
        if len(entry) == 5:
            diffType, entryName, _value, changes, _comment = entry
        elif len(entry) == 4:
            diffType, entryName, _value, changes = entry

        fullPath = os.path.join(level, entryName)

        if diffType == "modSec":
            for change in changes:
                self._printDiff(change, fullPath)
        elif diffType == "modOpt":
            if self.showModified:
                LOG.notice(f"Changed option {fullPath!r} from {changes!r}")
        elif diffType == "delOpt":
            if self.showAdded:
                LOG.notice(f"Option {fullPath!r} does not exist in template")
        elif diffType == "delSec":
            if self.showAdded:
                LOG.notice(f"Section {fullPath!r} does not exist in template")
        elif diffType == "addSec":
            if self.showMissingSections:
                LOG.notice(f"Section {fullPath!r} not found in current configuration: {pformat(changes)}")
        elif diffType == "addOpt":
            if self.showMissingOptions:
                LOG.notice(f"Option {fullPath!r} not found in current configuration. Default value is {changes!r}")
        else:
            LOG.error("Unknown DiffType", f"{diffType}, {fullPath}, {changes}")

    def run(self):
        """Run configuration comparison."""
        self._setSwitches()
        self._check()
        return S_OK()


@Script()
def main():
    CheckConfig().run()


if __name__ == "__main__":
    main()
