"""Configuration for the documentation scripts."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from __future__ import absolute_import

import argparse
import logging
import os
from pprint import pformat
import glob

from configparser import ConfigParser


LOG = logging.getLogger(__name__)

K_DOCS = "Docs"

K_CFG = "CFG"
DEF_CFG_BASEFILE = "../dirac.cfg"
DEF_CFG_TARGETFILE = "source/ExampleConfig.rst"

K_CODE = "Code"
DEF_CODE_TARGETPATH = "source/CodeDocumentation"
DEF_CODE_CUSTOMDOCS = "diracdoctools/CustomizedDocs"
DEF_CODE_COMMANDSECTION = "false"

K_COMMANDS = "Commands"
DEF_COMMANDS_FILENAME = "index.rst"
DEF_COMMANDS_SECTIONPATH = "source/CodeDocumentation/Commands"


def listify(values: str) -> list:
    """Listify string"""
    return [entry.strip() for entry in values.split(",") if entry]


class Configuration(object):
    """Provide configuration to the scripts."""

    def __init__(self, confFile, sections=None):
        if sections is None:
            sections = [K_CODE, K_COMMANDS, K_CFG]
        LOG.info("Reading configFile %r", os.path.join(os.getcwd(), confFile))
        self.config = ConfigParser(dict_type=dict)
        self.config.read(confFile)
        # self.config.optionxform = str  # do not transform options to lowercase

        self.docsPath = os.path.dirname(os.path.abspath(confFile))

        # Read Docs section
        self.moduleName = self.getOption(K_DOCS, "module_name", mandatory=True)
        self.sourcePath = self._fullPath(self.getOption(K_DOCS, "source_folder", f"../src/{self.moduleName}"))

        # Read Code section
        if K_CODE in sections:
            self.code_customDocsPath = self._fullPath(self.getOption(K_CODE, "customdocs_folder", DEF_CODE_CUSTOMDOCS))
            self.code_targetPath = self._fullPath(self.getOption(K_CODE, "docs_target_path", DEF_CODE_TARGETPATH))
            self.code_privateMembers = listify(self.getOption(K_CODE, "document_private_members"))
            self.code_noInherited = listify(self.getOption(K_CODE, "no_inherited_members"))
            self.code_dummyFiles = listify(self.getOption(K_CODE, "create_dummy_files"))
            self.code_ignoreFolders = listify(self.getOption(K_CODE, "ignore_folders"))
            self.code_ignoreFiles = listify(self.getOption(K_CODE, "ignore_files"))
            self.code_add_commands_section = self.getOption(
                K_CODE, "add_commands_section", DEF_CODE_COMMANDSECTION
            ).lower() in ["true", "yes", "y"]

        # Read Commands section
        if K_COMMANDS in sections:
            self.com_rst_path = os.path.join(
                self.getOption(K_COMMANDS, "sectionpath", DEF_COMMANDS_SECTIONPATH),
                self.getOption(K_COMMANDS, "filename", DEF_COMMANDS_FILENAME),
            )
            self.com_ignore_commands = listify(self.getOption(K_COMMANDS, "ignore_commands"))

            # List all scripts paths
            self.allScripts = glob.glob(os.path.join(self.sourcePath, "*", "scripts", "[!_]*.py"))
            self.allScripts += glob.glob(os.path.join(self.sourcePath, "*", "scripts", "[!_]*.sh"))
            self.allScripts.sort()

            self.scripts = {}  # Sorted by group/subgroup

            for section in [s for s in sorted(self.config.sections()) if s.startswith("commands.")]:
                # Identify group/subgroup names from the section name
                sp = section.split(".")
                group, subgroup = (sp[-1], None) if len(sp) == 2 else (sp[-2], sp[-1])

                LOG.info("Parsing config section: %r", section)

                # Read general group/subgroup settings
                title = self.getOption(section, "title", mandatory=True)
                pattern = listify(self.getOption(section, "pattern", mandatory=True))
                exclude = listify(self.getOption(section, "exclude"))
                prefix = self.getOption(section, "prefix")

                # Search scripts for group/subgroup pattern
                _scripts = []
                for sPath in self.allScripts:
                    path = sPath[len(self.sourcePath) :].replace("_", "-")
                    if any(p in path for p in pattern) and not any(p in path for p in exclude):
                        _scripts.append(sPath)

                if not subgroup:  # group case
                    # Path to RST file
                    fileName = self.getOption(section, "filename", "index.rst").strip()
                    sectionPath = self._fullPath(self.getOption(section, "sectionpath").replace(" ", ""))
                    # Collect scripts paths and metadata for group
                    self.scripts[group] = dict(
                        scripts=_scripts,
                        title=title,
                        prefix=prefix,
                        rstPath=os.path.join(sectionPath, fileName),
                        subgroups=[],
                    )
                else:  # subgroup case
                    # Collect scripts paths and metadata for subgroup
                    self.scripts[group]["subgroups"].append(subgroup)
                    # Sub group scripts is a subset of the group scripts
                    subgroupScripts = [s for s in _scripts if s in self.scripts[group]["scripts"]]
                    self.scripts[group][subgroup] = dict(title=title, prefix=prefix, scripts=subgroupScripts)
                    # Remove subgroup scripts from group
                    self.scripts[group]["scripts"] = [s for s in self.scripts[group]["scripts"] if s not in _scripts]

        # Read CFG section
        if K_CFG in sections:
            self.cfg_targetFile = self._fullPath(self.getOption(K_CFG, "target_file", DEF_CFG_TARGETFILE))
            self.cfg_baseFile = self._fullPath(self.getOption(K_CFG, "base_file", DEF_CFG_BASEFILE))

        for var, val in sorted(vars(self).items()):
            LOG.info("Parsed options: %s = %s", var, pformat(val))

    def _fullPath(self, path):
        """Return absolute path based on docsPath."""
        return os.path.abspath(os.path.join(self.docsPath, path))

    def getOption(self, section: str, option: str, default="", mandatory: bool = False) -> "option value":
        """Get option from TOML configuration

        :param section: section name
        :param option: option name
        :param default: default value
        :param mandatory: if option is mandatory"""
        if mandatory:
            return self.config.get(section, option)
        value = self.config.get(section, option) if self.config.has_option(section, option) else ""
        if not value and default:
            LOG.debug("Since the '{section}.{option}' is not specified, use default: {default}")
            return default
        return value

    def __str__(self):
        """Return string containing options and values."""
        theStr = ""
        for var, val in vars(self).items():
            theStr += "%s = %s\n" % (var, val)

        return theStr


class CLParser(object):
    def __init__(self):
        self.log = LOG.getChild("CLParser")
        self.parsed = None
        self.debug = False
        self.parser = argparse.ArgumentParser("DiracDocTool", formatter_class=argparse.RawTextHelpFormatter)

        self.parser.add_argument(
            "--configFile", action="store", default="docs.conf", dest="configFile", help="Name of the config file"
        )

        self.parser.add_argument("-d", "--debug", action="count", dest="debug", help="d, dd, ddd", default=0)

    def parse(self):
        self.log.info("Parsing common options")
        self.parsed = self.parser.parse_args()
        self.logLevel = self._parsePrintLevel(self.parsed.debug)
        self.configFile = self.parsed.configFile

    def optionDict(self):
        """Return dictionary of options."""
        if not self.parsed:
            self.parse()
        return dict(
            configFile=self.configFile,
            logLevel=self.logLevel,
            debug=self.debug,
        )

    def _parsePrintLevel(self, level):
        """Translate debug count to logging level."""
        level = level if level <= 2 else 2
        self.debug = level == 2
        return [
            logging.INFO,
            logging.INFO,
            logging.DEBUG,
        ][level]
