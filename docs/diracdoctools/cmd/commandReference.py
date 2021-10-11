#!/usr/bin/env python
"""Module to create command references for DIRAC."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ast
from concurrent.futures import ThreadPoolExecutor
import logging
import glob
import os
import shutil
import textwrap

from diracdoctools.Utilities import writeLinesToFile, mkdir, runCommand, makeLogger
from diracdoctools.Config import Configuration, CLParser as clparser

LOG = makeLogger("CommandReference")

TITLE = "title"
PATTERN = "pattern"
SCRIPTS = "scripts"
EXCLUDE = "exclude"
RST_PATH = "rstPath"
PREFIX = "prefix"


class CLParser(clparser):
    """Extension to CLParser to also parse buildType."""

    def __init__(self):
        super(CLParser, self).__init__()
        self.log = LOG.getChild("CLParser")
        self.clean = False

        self.parser.add_argument(
            "--clean",
            action="store_true",
            help="Remove rst files and exit",
        )

    def parse(self):
        super(CLParser, self).parse()
        self.log.info("Parsing options")
        self.clean = self.parsed.clean

    def optionDict(self):
        oDict = super(CLParser, self).optionDict()
        oDict["clean"] = self.clean
        return oDict


class CommandReference(object):
    def __init__(self, configFile="docs.conf", debug=False):
        self.config = Configuration(configFile, sections=["Commands"])
        self.exitcode = 0
        self.debug = debug

        self.scriptDocs = {}  # Scripts docs collection

        if not os.path.exists(self.config.sourcePath):
            LOG.error("%s does not exist" % self.config.sourcePath)
            raise RuntimeError("Package not found")

    def createSectionAndIndex(self, sectionDict):
        """Create the index file and folder where the RST files will go

        e.g.:
        source/UserGuide/CommandReference/DataManagement
        """
        systemName = sectionDict[TITLE]
        # Add reference
        sectionIndexRST = ".. _%s_cmd:\n\n" % sectionDict[PREFIX] if sectionDict[PREFIX] else ""
        # Add header
        systemHeader = systemName + " Command Reference"
        sectionIndexRST += "%s\n%s\n%s\n" % ("=" * len(systemHeader), systemHeader, "=" * len(systemHeader))
        # Add description
        sectionIndexRST += textwrap.dedent(
            """
                                       .. this page automatically is created in %s

                                       In this subsection the %s commands are collected

                                       """
            % (__name__, systemName)
        )

        # Write commands that were not included in the subgroups
        for com in sectionDict[SCRIPTS]:
            name = os.path.basename(com)
            name = name.replace("_", "-")[:-3] if name.endswith(".py") else name
            if self.scriptDocs[name]:
                sectionIndexRST += "- :ref:`%s<%s>`\n" % (name, name)

        # Write commands included in the subgroups
        for group in sectionDict["subgroups"]:
            groupDict = sectionDict[group]
            # Add subgroup reference
            sectionIndexRST += "\n.. _%s_cmd:\n\n" % groupDict[PREFIX] if groupDict[PREFIX] else ""
            # Add subgroup header
            sectionIndexRST += "%s\n%s\n%s\n" % (
                "-" * len(groupDict[TITLE]),
                groupDict[TITLE],
                "-" * len(groupDict[TITLE]),
            )
            for com in groupDict[SCRIPTS]:
                name = os.path.basename(com)
                name = name.replace("_", "-")[:-3] if name.endswith(".py") else name
                if self.scriptDocs[name]:
                    sectionIndexRST += "   - :ref:`%s<%s>`\n" % (name, name)

        writeLinesToFile(os.path.join(self.config.docsPath, sectionDict[RST_PATH]), sectionIndexRST)

    def createAllScriptsDocsAndWriteToRST(self):
        """Get all scripts and write it to RST file."""
        # Use `:orphan:` in case you do not need a reference to this document in doctree
        sectionIndexRST = textwrap.dedent(
            """
                                      :orphan:

                                      .. this page automatically is created in %s

                                      .. _cmd:

                                      Command Reference

                                      In this subsection all commands are collected:

                                      """
            % __name__
        )

        futures = []
        # Call all scripts help
        with ThreadPoolExecutor() as pool:
            for script in self.config.allScripts:
                futures.append(pool.submit(self.createScriptDoc, script))

        docs = {}
        # Collect all scripts help messages
        for future in futures:
            systemName, scriptName, createdScriptDocs = future.result()
            self.scriptDocs[scriptName] = createdScriptDocs
            if createdScriptDocs:
                if systemName not in docs:
                    # Set system name
                    head = "%s\n%s\n%s\n\n" % ("=" * len(systemName), systemName, "=" * len(systemName))
                    docs[systemName] = "\n\n.. _%s_cmd:\n\n" % systemName.lower().replace(" ", "") + head
                # Add script description
                docs[systemName] += createdScriptDocs

        # Write all commands in one RST for each system
        for system in sorted(docs):
            sectionIndexRST += docs[system]

        writeLinesToFile(os.path.join(self.config.docsPath, self.config.com_rst_path), sectionIndexRST)

    def createScriptDoc(self, script: str):
        """Create descriptions for all the scripts.

        Folders and indices already exist, just call the scripts and get the help messages. Format the help message.

        :return: tuple(str, str, str) -- system, script name, parsed help message
        """
        executor = "bash"
        scriptName = os.path.basename(script)
        systemName = script.split("/")[-3].replace("System", "")
        if scriptName.endswith(".py"):
            executor = "python"
            scriptName = scriptName.replace("_", "-")[:-3]

        if scriptName in self.config.com_ignore_commands:
            return systemName, scriptName, False

        LOG.info("Creating Doc for %r", scriptName)
        helpMessage = runCommand("%s %s -h" % (executor, script))
        if not helpMessage:
            LOG.warning("NO DOC for %s", scriptName)
            return systemName, scriptName, False

        # Script reference
        fileContent = f"\n\n.. _{scriptName}:\n\n"
        # Script title
        fileContent += f"{'-' * len(scriptName)}\n{scriptName}\n{'-' * len(scriptName)}"
        # Script description payload
        rstLines = []
        genOptions = False
        lineIndented = False
        for line in helpMessage.splitlines():
            line = line.rstrip()
            newLine = "\n" + ":".join(line.rsplit("::", 1)) + ":\n" if line.endswith(":") else line
            # ensure dedented lines are separated by newline from previous block
            if lineIndented and not newLine.startswith(" "):
                newLine = "\n" + newLine
            rstLines.append(newLine)
            lineIndented = newLine.startswith(" ")
        fileContent += "\n\n" + "\n".join(rstLines).strip() + "\n"

        # remove the standalone '-' when no short option exists
        fileContent = fileContent.replace("-   --", "    --")
        return systemName, scriptName, fileContent

    def cleanDoc(self):
        """Remove the code output folder."""
        LOG.info("Removing existing commands documentation")
        for fPath in [self.config.com_rst_path] + [self.config.scripts[p][RST_PATH] for p in self.config.scripts]:
            path = os.path.join(self.config.docsPath, fPath)
            if os.path.basename(path) == "index.rst":
                path = os.path.dirname(path)
            LOG.info("Removing: %r", path)
            if os.path.exists(path):
                shutil.rmtree(path)


def run(configFile="docs.conf", logLevel=logging.INFO, debug=False, clean=False):
    """Create the rst files for dirac commands, parsed form the --help message.

    :param str configFile: path to the configFile
    :param logLevel: logging level to use
    :param bool debug: if true even more debug information is printed
    :param bool clean: Remove rst files and exit
    :returns: return value 1 or 0
    """
    logging.getLogger().setLevel(logLevel)
    commands = CommandReference(configFile=configFile, debug=debug)
    if clean:
        commands.cleanDoc()
        return 0
    commands.createAllScriptsDocsAndWriteToRST()
    for section in commands.config.scripts:
        sectionDict = commands.config.scripts[section]
        commands.createSectionAndIndex(sectionDict)
    LOG.info("Done")
    return commands.exitcode


if __name__ == "__main__":
    exit(run(**(CLParser().optionDict())))
