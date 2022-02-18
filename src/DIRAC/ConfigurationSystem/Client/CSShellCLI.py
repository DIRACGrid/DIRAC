"""CSShellCmd class emulates the behaviour of a shell to edit the CS config.
"""

import cmd
import os

from DIRAC.Core.Base.CLI import CLI, colorize
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.Modificator import Modificator
from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient


class CSShellCLI(CLI):
    def __init__(self):
        CLI.__init__(self)
        self.serverURL = ""
        self.serverName = ""
        self.modificator = None
        self.connected = False
        self.dirty = False
        self.root = "/"

        self.do_connect("")

    def update_prompt(self):
        if self.connected:
            self.prompt = "[" + colorize(self.serverName, "green") + ":" + self.root + " ]% "
        else:
            self.prompt = "[" + colorize("disconnected", "red") + ":" + self.root + " ]% "

    def do_connect(self, line):
        """connect
        Connect to the CS
        Usage: connect <URL> (Connect to the CS at the specified URL)
               connect       (Connect to the default CS URL of your config)
        """
        if line == "":
            self.serverURL = gConfigurationData.getMasterServer()
            self.serverName = gConfigurationData.getName()
        else:
            self.serverURL = self.serverName = line

        if self.serverURL is None:
            print("Unable to connect to the default server. Maybe you don't have a proxy ?")
            return self.do_disconnect("")

        print("Trying to connect to " + self.serverURL + "...", end=" ")

        self.modificator = Modificator(ConfigurationClient(url=self.serverURL))
        rv = self.modificator.loadFromRemote()
        rv2 = self.modificator.loadCredentials()

        if rv["OK"] == False or rv2["OK"] == False:
            print("failed: ", end=" ")
            if rv["OK"] is False:
                print(rv["Message"])
            else:
                print(rv2["Message"])
            self.connected = False
            self.update_prompt()
        else:
            self.connected = True
            self.update_prompt()
            print("done.")

    def do_disconnect(self, _line):
        """Disconnect from CS"""
        if self.connected and self.dirty:
            res = input("Do you want to commit your changes into the CS ? [y/N] ")
            if res.lower() in ["y", "yes"]:
                self.do_commit("")

        self.serverURL = self.serverName = ""
        self.modificator = None
        self.connected = False
        self.update_prompt()

    def do_ls(self, line):
        """ls
        List the sections and options of CS of the current root"""
        if self.connected:
            secs = self.modificator.getSections(self.root)
            opts = self.modificator.getOptions(self.root)
            if line.startswith("-") and "l" in line:
                for i in secs:
                    print(colorize(i, "blue") + "  ")
                for i in opts:
                    print(i + " ")
            else:
                for i in secs:
                    print(colorize(i, "blue") + "  ", end=" ")
                for i in opts:
                    print(i + " ", end=" ")
                print("")

    def do_cd(self, line):
        """cd
        Go one directory deeper in the CS"""
        # Check if invariant holds
        if self.connected:
            assert self.root == "/" or not self.root.endswith("/")
            assert self.root.startswith("/")
            secs = self.modificator.getSections(self.root)
            if line == "..":
                self.root = os.path.dirname(self.root)
                self.update_prompt()
            else:
                if os.path.normpath(line) in secs:
                    if self.root == "/":
                        self.root = self.root + os.path.normpath(line)
                    else:
                        self.root = self.root + "/" + os.path.normpath(line)
                    self.update_prompt()
                else:
                    print("cd: no such section: " + line)

    def complete_cd(self, text, _line, _begidx, _endidx):
        secs = self.modificator.getSections(self.root)
        return [(s + "/") for s in secs if s.startswith(text)]

    def do_cat(self, line):
        """cat
        Read the content of an option in the CS"""
        if self.connected:
            opts = self.modificator.getOptionsDict(self.root)

            if line in opts:
                print(opts[line])
            else:
                print("cat: No such option")

    def complete_cat(self, text, _line, _begidx, _endidx):
        opts = self.modificator.getOptions(self.root)
        return [o for o in opts if o.startswith(text)]

    do_less = do_cat
    complete_less = complete_cat

    def do_mkdir(self, line):
        """mkdir
        Create a new section in the CS"""
        if self.connected:
            self.modificator.createSection(self.root + "/" + line)
            self.dirty = True

    complete_mkdir = complete_cd

    def do_rmdir(self, line):
        """rmdir
        Delete a section in the CS"""
        if self.connected:
            self.modificator.removeSection(self.root + "/" + line)
            self.dirty = True

    complete_rmdir = complete_cd

    def do_rm(self, line):
        """rm
        Delete an option in the CS"""
        if self.connected:
            self.modificator.removeOption(self.root + "/" + line)
            self.dirty = True

    complete_rm = complete_cat

    def do_set(self, line):
        """set
        Set an option in the CS (or create it if it does not exists)
        Usage: set <str> to set a string option (will be stored as a string in CS)
               set <str>,<str>,... to set a list option (will be stored as a list in CS)
        """
        if self.connected:
            line = line.split(" ", 2)
            if len(line) != 2:
                print("Usage: set <key> <value>")
            else:
                self.modificator.setOptionValue(self.root + "/" + line[0], line[1])
                self.dirty = True

    complete_set = complete_cat

    def do_unset(self, line):
        """unset
        Unset an option in the CS: Making the option equal to the
        empty string."""
        if self.connected:
            self.modificator.setOptionValue(self.root + "/" + line, "")
            self.dirty = True

    complete_unset = complete_cat

    def do_commit(self, _line):
        """commit
        Commit the modifications to the CS"""
        if self.connected and self.dirty:
            self.modificator.commit()

    def default(self, line):
        """Override [Cmd.default(line)] function."""
        if line == "EOF":
            if self.prompt:
                print()
            return self.do_quit(line)
        else:
            cmd.Cmd.default(self, line)

    def do_quit(self, _line):
        """quit
        Quit"""
        self.do_disconnect("")
        CLI.do_quit(self, _line)
