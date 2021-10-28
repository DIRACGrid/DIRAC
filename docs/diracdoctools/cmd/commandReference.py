#!/usr/bin/env python
"""Module to create command references for DIRAC."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from concurrent.futures import ThreadPoolExecutor
import logging
import os
import shutil
import textwrap
from collections import namedtuple

from diracdoctools.Utilities import writeLinesToFile, runCommand, makeLogger
from diracdoctools.Config import Configuration, CLParser as clparser


LOG = makeLogger("CommandReference")


TITLE = "title"
PATTERN = "pattern"
SCRIPTS = "scripts"
EXCLUDE = "exclude"
RST_PATH = "rstPath"
PREFIX = "prefix"

Script = namedtuple("Script", "name system description")


class CLParser(clparser):
    """Extension to CLParser to also parse clean."""

    def __init__(self):
        self.clean = False
        super(CLParser, self).__init__()
        self.log = LOG.getChild("CLParser")
        self.parser.add_argument("--clean", action="store_true", help="Remove rst files and exit")

    def parse(self):
        super(CLParser, self).parse()
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

    def createSectionAndIndex(self, sectionDict: dict):
        """Create the index file and folder where the RST files will go.

        :param sectionDict: section description
        """
        reference = f".. _{sectionDict[PREFIX]}_cmd:" if sectionDict[PREFIX] else ""
        title = f"{sectionDict[TITLE]} Command Reference"
        # Add description
        sectionIndexRST = textwrap.dedent(
            f"""
                {reference}

                {"=" * len(title)}
                {title}
                {"=" * len(title)}

                .. this page automatically is created in {__name__}

                In this subsection the {title} commands are collected

            """
        )

        # Write commands that were not included in the subgroups
        for name in sectionDict[SCRIPTS]:
            if name in self.scriptDocs:
                sectionIndexRST += f"- :ref:`{name}<{name}>`\n"

        # Write commands included in the subgroups
        for group in sectionDict["subgroups"]:
            groupDict = sectionDict[group]
            # Add subgroup reference
            ref = f".. _{groupDict[PREFIX]}_cmd:" if groupDict[PREFIX] else ""
            # Add subgroup header
            sectionIndexRST += textwrap.dedent(
                f"""
                    {ref}

                    {"-" * len(groupDict[TITLE])}
                    {groupDict[TITLE]}
                    {"-" * len(groupDict[TITLE])}

                """
            )
            for name in groupDict[SCRIPTS]:
                if name in self.scriptDocs:
                    sectionIndexRST += f"   - :ref:`{name}<{name}>`\n"

        writeLinesToFile(os.path.join(self.config.docsPath, sectionDict[RST_PATH]), sectionIndexRST)

    def createAllScriptsDocsAndWriteToRST(self):
        """Get all scripts and write it to RST file."""
        # Use `:orphan:` in case you do not need a reference to this document in doctree
        sectionIndexRST = textwrap.dedent(
            f"""
                :orphan:

                .. this page automatically is created in {__name__}

                .. _cmd:

                Command Reference

                In this subsection all commands are collected:

            """
        )

        futures = []
        # Call all scripts help
        with ThreadPoolExecutor() as pool:
            for script in self.config.allScripts:
                futures.append(pool.submit(self.createScriptDoc, script))

        systems = []
        # Collect all scripts help messages
        for future in futures:
            script = future.result()
            if script:
                self.scriptDocs[script.name] = script
                script.system not in systems and systems.append(script.system)

        # Write all commands in one RST for each system
        for system in sorted(systems):
            # Write system head
            sectionIndexRST += textwrap.dedent(
                f"""
                    .. _{system}_cmd:

                    {"=" * len(system)}
                    {system}
                    {"=" * len(system)}

                """
            )
            # Write each system command description
            for script in sorted(self.scriptDocs):
                if self.scriptDocs[script].system == system:
                    sectionIndexRST += self.scriptDocs[script].description

        writeLinesToFile(os.path.join(self.config.docsPath, self.config.com_rst_path), sectionIndexRST)

    def createScriptDoc(self, script: str):
        """Create script description.

        Folders and indices already exist, just call the scripts and get the help messages. Format the help message.

        :return: Script -- system name, script name, parsed help message
        """
        executor = "bash"
        scriptName = os.path.basename(script)
        systemName = script.split("/")[-3].replace("System", "")
        if scriptName.endswith(".py"):
            executor = "python"
            scriptName = scriptName.replace("_", "-")[:-3]

        if scriptName in self.config.com_ignore_commands:
            return

        LOG.info("Creating Doc for %r", scriptName)
        helpMessage = runCommand("%s %s -h" % (executor, script))
        if not helpMessage:
            LOG.warning("NO DOC for %s", scriptName)
            helpMessage = "Oops, we couldn't generate a description for this command."

        # Script reference
        fileContent = textwrap.dedent(
            f"""

                .. _{scriptName}:

                {'-' * len(scriptName)}
                {scriptName}
                {'-' * len(scriptName)}

            """
        )

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
        return Script(scriptName, systemName, fileContent)

    def cleanDoc(self):
        """Remove the code output files."""
        LOG.info("Removing existing commands documentation")
        for fPath in [self.config.com_rst_path] + [self.config.scripts[p][RST_PATH] for p in self.config.scripts]:
            path = os.path.join(self.config.docsPath, fPath)
            if os.path.basename(path) == "index.rst":
                path = os.path.dirname(path)
            LOG.info("Removing: %r", path)
            if os.path.exists(path):
                shutil.rmtree(path)
        LOG.info("Done")


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

    # Clean the generated files
    if clean:
        return commands.cleanDoc()
    # Create a file with a description of all commands
    commands.createAllScriptsDocsAndWriteToRST()
    # Create dictionaries for the individual dresses described in the configuration
    for section in commands.config.scripts:
        sectionDict = commands.config.scripts[section]
        commands.createSectionAndIndex(sectionDict)
    LOG.info("Done")
    return commands.exitcode


if __name__ == "__main__":
    exit(run())
