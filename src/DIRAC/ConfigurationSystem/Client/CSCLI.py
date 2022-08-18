########################################################################
# File :   CSCLI.py
# Author : Adria Casajus
########################################################################
import sys
import atexit
import os
import readline

from DIRAC import gLogger

from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.Base.CLI import CLI, colorize
from DIRAC.ConfigurationSystem.private.Modificator import Modificator
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient


def _showTraceback():
    import traceback

    excepType, execpValue = sys.exc_info()[:2]
    print("________________________\n")
    print("Exception", excepType, ":", execpValue)
    traceback.print_tb(sys.exc_info()[2])
    print("________________________\n")


def _printComment(comment):
    commentList = comment.split("\n")
    for commentLine in commentList[:-1]:
        print("# %s" % commentLine.strip())


def _appendExtensionIfMissing(filename):
    dotPosition = filename.rfind(".")
    if dotPosition > -1:
        filename = filename[:dotPosition]
    return "%s.cfg" % filename


class CSCLI(CLI):
    def __init__(self):
        CLI.__init__(self)
        self.connected = False
        self.masterURL = "unset"
        self.writeEnabled = False
        self.modifiedData = False
        self.rpcClient = None
        self.do_connect()
        if self.connected:
            self.modificator = Modificator(self.rpcClient)
        else:
            self.modificator = Modificator()
        self.indentSpace = 20
        self.backupFilename = "dataChanges"
        # store history
        histfilename = os.path.basename(sys.argv[0])
        historyFile = os.path.expanduser("~/.dirac/%s.history" % histfilename[0:-3])
        mkDir(os.path.dirname(historyFile))
        if os.path.isfile(historyFile):
            readline.read_history_file(historyFile)
        readline.set_history_length(1000)
        atexit.register(readline.write_history_file, historyFile)

    def start(self):
        if self.connected:
            self.modificator.loadFromRemote()
            retVal = self.modificator.loadCredentials()
            if not retVal["OK"]:
                print("There was an error gathering your credentials")
                print(retVal["Message"])
                self._setStatus(False)
        try:
            self.cmdloop()
        except KeyboardInterrupt:
            gLogger.warn("Received a keyboard interrupt.")
            self.do_quit("")

    def _setConnected(self, connected, writeEnabled):
        self.connected = connected
        self.modifiedData = False
        self.writeEnabled = writeEnabled
        if connected:
            if writeEnabled:
                self.prompt = "({})-{}> ".format(self.masterURL, colorize("Connected", "green"))
            else:
                self.prompt = "({})-{}> ".format(self.masterURL, colorize("Connected (RO)", "yellow"))
        else:
            self.prompt = "({})-{}> ".format(self.masterURL, colorize("Disconnected", "red"))

    def do_quit(self, dummy):
        """
        Exits the application without sending changes to server

        Usage: quit
        """
        print()
        if self.modifiedData:
            print("Changes are about to be written to file for later use.")
            self.do_writeToFile(self.backupFilename)
            print("Changes written to %s.cfg" % self.backupFilename)
        sys.exit(0)

    def _setStatus(self, connected=True):
        if not connected:
            self.masterURL = "unset"
            self._setConnected(False, False)
        else:
            retVal = self.rpcClient.writeEnabled()
            if retVal["OK"]:
                if retVal["Value"]:
                    self._setConnected(True, True)
                else:
                    self._setConnected(True, False)
            else:
                print("Server returned an error: %s" % retVal["Message"])
                self._setConnected(True, False)

    def _tryConnection(self):
        print("Trying connection to %s" % self.masterURL)
        try:
            self.rpcClient = ConfigurationClient(url=self.masterURL)
            self._setStatus()
        except Exception as x:
            gLogger.error("Couldn't connect to master CS server", f"{self.masterURL} ({str(x)})")
            self._setStatus(False)

    def do_connect(self, args=""):
        """
        Connects to configuration master server (in specified url if provided).

        Usage: connect <url>
        """
        if not args or not isinstance(args, str):
            self.masterURL = gConfigurationData.getMasterServer()
            if self.masterURL != "unknown" and self.masterURL:
                self._tryConnection()
            else:
                self._setStatus(False)
        else:
            splitted = args.split()
            if len(splitted) == 0:
                print("Must specify witch url to connect")
                self._setStatus(False)
            else:
                self.masterURL = splitted[0].strip()
                self._tryConnection()

    def do_sections(self, args):
        """
        Shows all sections with their comments.
        If no section is specified, root is taken.

        Usage: sections <section>
        """
        try:
            argList = args.split()
            if argList:
                baseSection = argList[0].strip()
            else:
                baseSection = "/"
            if not self.modificator.existsSection(baseSection):
                print("Section %s does not exist" % baseSection)
                return
            sectionList = self.modificator.getSections(baseSection)
            if not sectionList:
                print("Section %s is empty" % baseSection)
                return
            for section in sectionList:
                section = f"{baseSection}/{section}"
                self.printPair(section, self.modificator.getComment(section), " #")
        except Exception:
            _showTraceback()

    def do_options(self, args):
        """
        Shows all options and values of a specified section

        Usage: options <section>
        """
        try:
            argList = args.split()
            if argList:
                section = argList[0].strip()
            else:
                print("Which section?")
                return
            if not self.modificator.existsSection(section):
                print("Section %s does not exist" % section)
                return
            optionsList = self.modificator.getOptions(section)
            if not optionsList:
                print("Section %s has no options" % section)
                return
            for option in optionsList:
                _printComment(self.modificator.getComment(f"{section}/{option}"))
                self.printPair(option, self.modificator.getValue(f"{section}/{option}"), "=")
        except Exception:
            _showTraceback()

    def do_get(self, args):
        """
        Shows value and comment for specified option in section

        Usage: get <path to option>
        """
        try:
            argList = args.split()
            if argList:
                optionPath = argList[0].strip()
            else:
                print("Which option?")
                return
            if self.modificator.existsOption(optionPath):
                option = optionPath.split("/")[-1]
                _printComment(self.modificator.getComment(optionPath))
                self.printPair(option, self.modificator.getValue(optionPath), "=")
            else:
                print("Option %s does not exist" % optionPath)
        except Exception:
            _showTraceback()

    def do_writeToServer(self, dummy):
        """
        Sends changes to server.

        Usage: writeToServer
        """
        if not self.connected:
            print("You are not connected!")
            return
        try:
            if not self.writeEnabled:
                print("This server can't receive data modifications")
                return
            if not self.modifiedData:
                while True:
                    choice = input("Data has not been modified, do you still want to upload changes? yes/no [no]: ")
                    choice = choice.lower()
                    if choice in ("yes", "y"):
                        break
                    else:
                        print("Commit aborted")
                        return

            choice = input("Do you really want to send changes to server? yes/no [no]: ")
            choice = choice.lower()
            if choice in ("yes", "y"):
                print("Uploading changes to %s (It may take some seconds)..." % self.masterURL)
                response = self.modificator.commit()
                if response["OK"]:
                    self.modifiedData = False
                    print("Data sent to server.")
                    self.modificator.loadFromRemote()
                else:
                    print("Error sending data, server said: %s" % response["Message"])
                return
            else:
                print("Commit aborted")
        except Exception as x:
            _showTraceback()
            print("Could not upload changes. ", str(x))

    def do_set(self, args):
        """
        Sets option's value

        Usage: set <optionPath> <value>...

        From second argument until the last one is considered option's value

        NOTE: If specified section does not exist it is created.
        """
        try:
            argsList = args.split()
            if len(argsList) < 2:
                print("Must specify option and value to use")
                return
            optionPath = argsList[0].strip()
            value = " ".join(argsList[1:]).strip()
            self.modificator.setOptionValue(optionPath, value)
            self.modifiedData = True
        except Exception as x:
            print("Cannot insert value: ", str(x))

    def do_removeOption(self, args):
        """
        Removes an option.

        Usage: removeOption <option>

        There can be empty sections.
        """
        try:
            argsList = args.split()
            if len(argsList) < 1:
                print("Must specify option to delete")
                return
            optionPath = argsList[0].strip()
            choice = input("Are you sure you want to delete %s? yes/no [no]: " % optionPath)
            choice = choice.lower()
            if choice in ("yes", "y", "true"):
                if self.modificator.removeOption(optionPath):
                    self.modifiedData = True
                else:
                    print("Can't be deleted")
            else:
                print("Aborting removal.")
        except Exception as x:
            print("Error removing option, %s" % str(x))

    def do_removeSection(self, args):
        """
        Removes a section.

        Usage: removeSection <section>
        """
        try:
            argsList = args.split()
            if len(argsList) < 1:
                print("Must specify section to delete")
                return
            section = argsList[0].strip()
            choice = input("Are you sure you want to delete %s? yes/no [no]: " % section)
            choice = choice.lower()
            if choice in ("yes", "y", "true"):
                if self.modificator.removeSection(section):
                    self.modifiedData = True
                else:
                    print("Can't be deleted")
            else:
                print("Aborting removal.")
        except Exception as x:
            print("Error removing section, %s" % str(x))

    def do_setComment(self, args):
        """
        Sets option or section's comment. Requested entry MUST exist.

        Usage: set <option/section> <comment>...

        From third argument until the last one is considered option's comment.
        """
        try:
            argsList = args.split()
            if len(argsList) < 2:
                print("Must specify option and value to use")
                return
            entryPath = argsList[0].strip()
            value = " ".join(argsList[1:]).strip()
            self.modificator.setComment(entryPath, value)
            self.modifiedData = True
        except Exception as x:
            print("Cannot insert comment: ", str(x))

    def do_writeToFile(self, args):
        """
        Writes modification to file for later use.

        Usage: writeToFile <filename>.cfg

        Note that if a file extension is specified, it is replaced by .cfg suffix.
        If not it is added automatically
        """
        try:
            if len(args) == 0:
                print("Filename to write must be specified!")
                return
            filename = args.split()[0].strip()
            filename = _appendExtensionIfMissing(filename)
            self.modificator.dumpToFile(filename)
        except Exception as x:
            print(f"Couldn't write to file {filename}: {str(x)}")

    def do_readFromFile(self, args):
        """
        Reads data from filename to be used. Actual data will be replaced!

        Usage: readFromFile <filename>.cfg

        Note that if a file extension is specified, it is replaced by .cfg suffix.
        If not it is added automatically
        """
        try:
            if len(args) == 0:
                print("Filename to read must be specified!")
                return
            filename = args.split()[0].strip()
            filename = _appendExtensionIfMissing(filename)
            self.modificator.loadFromFile(filename)
            self.modifiedData = True
        except Exception as x:
            print(f"Couldn't read from file {filename}: {str(x)}")

    def do_mergeFromFile(self, args):
        """
        Reads data from filename and merges it with current data.
        Data read from file has more precedence that current one.

        Usage: mergeFromFile <filename>.cfg

        Note that if a file extension is specified, it is replaced by .cfg suffix.
        If not it is added automatically
        """
        try:
            if len(args) == 0:
                print("Filename to read must be specified!")
                return
            filename = args.split()[0].strip()
            filename = _appendExtensionIfMissing(filename)
            self.modificator.mergeFromFile(filename)
            self.modifiedData = True
        except Exception as x:
            _showTraceback()
            print(f"Couldn't read from file {filename}: {str(x)}")

    def do_showData(self, dummy):
        """
        Shows the current modified configuration
        Usage: showData
        """
        print(self.modificator)

    def do_showHistory(self, args):
        """
        Shows the last commit history
        Usage: showHistory <update limit>
        """
        try:
            argsList = args.split()
            limit = 100
            if len(argsList) > 0:
                limit = int(argsList[0])
            history = self.modificator.getHistory(limit)
            print("%s recent commits:" % limit)
            for entry in history:
                self.printPair(entry[0], entry[1], "@")
        except Exception:
            _showTraceback()

    def do_showDiffWithServer(self, dummy):
        """
        Shows diff with lastest version in server
        Usage: showDiffWithServer
        """
        try:
            diffData = self.modificator.showCurrentDiff()
            print("Diff with latest from server ( + local - remote )")
            for line in diffData:
                if line[0] in ("-"):
                    print(colorize(line, "red"))
                elif line[0] in ("+"):
                    print(colorize(line, "green"))
                elif line[0] in ("?"):
                    print(colorize(line, "yellow"), end=" ")
        except Exception:
            _showTraceback()

    def do_showDiffBetweenVersions(self, args):
        """
        Shows diff between two versions
        Usage: showDiffBetweenVersions <version 1 with spaces> <version 2 with spaces>
        """
        try:
            argsList = args.split()
            if len(argsList) < 4:
                print("What are the two versions to compare?")
                return
            v1 = " ".join(argsList[0:2])
            v2 = " ".join(argsList[2:4])
            print(f"Comparing '{v1}' with '{v2}' ")
            diffData = self.modificator.getVersionDiff(v1, v2)
            print(f"Diff with latest from server ( + {v2} - {v1} )")
            for line in diffData:
                if line[0] in ("-"):
                    print(colorize(line, "red"))
                elif line[0] in ("+"):
                    print(colorize(line, "green"))
                elif line[0] in ("?"):
                    print(colorize(line, "yellow"), end=" ")
                else:
                    print(line)
        except Exception:
            _showTraceback()

    def do_rollbackToVersion(self, args):
        """
        rolls back to user selected version of the configuration
        Usage: rollbackToVersion <version with spaces>>
        """
        try:
            argsList = args.split()
            if len(argsList) < 2:
                print("What version to rollback?")
                return
            version = " ".join(argsList[0:2])
            choice = input("Do you really want to rollback to version %s? yes/no [no]: " % version)
            choice = choice.lower()
            if choice in ("yes", "y"):
                response = self.modificator.rollbackToVersion(version)
                if response["OK"]:
                    self.modifiedData = False
                    print("Rolled back.")
                    self.modificator.loadFromRemote()
                else:
                    print("Error sending data, server said: %s" % response["Message"])
        except Exception:
            _showTraceback()

    def do_mergeWithServer(self, dummy):
        """
        Shows diff with lastest version in server
        Usage: diffWithServer
        """
        try:
            choice = input("Do you want to merge with server configuration? yes/no [no]: ")
            choice = choice.lower()
            if choice in ("yes", "y"):
                retVal = self.modificator.mergeWithServer()
                if retVal["OK"]:
                    print("Merged")
                else:
                    print("There was an error: ", retVal["Message"])
            else:
                print("Merge aborted")
        except Exception:
            _showTraceback()
