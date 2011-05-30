#!/usr/bin/env python

"""
CLI to edit RSS Config
"""
import sys
import os.path
import cmd

from DIRAC.Core.Base                                    import Script
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.DISET.RPCClient                         import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.BrowseConfig  import BrowseConfig

Script.parseCommandLine()

# Invariants:
# * root does not end with "/" or root is "/"
# * root starts with "/"

class RSSConfigCmd(cmd.Cmd):

  def __init__(self):
    cmd.Cmd.__init__(self)
    self.bc     = BrowseConfig(RPCClient(gConfigurationData.getMasterServer()))
    self.bc.loadFromRemote()
    self.bc.loadCredentials()
    self.connected = True
    self.root   = BrowseConfig.rssConfigRootPath
    self.prompt = "[cfgedit " + self.root + " ]% "
    self.dirty = False

  def update_prompt(self):
    self.prompt = "[cfgedit " + self.root + " ]% "

  def do_connect(self, line):
    """connect
    Connect to the CS
    Usage: connect <URL> (Connect to the CS at the specified URL)
           connect       (Connect to the default CS URL of your config)
    """
    if line == "":
      line = gConfigurationData.getMasterServer()
    try:
      self.bc = BrowseConfig(RPCClient(line))
      self.bc.loadFromRemote()
      self.bc.loadCredentials()
      self.connected = True
    except:
      self.do_disconnect("")

  def do_disconnect(self, _line):
    """Disconnect from CS"""
    self.bc = None
    self.connected = False

  def do_ls(self, line):
    """ls
    List the sections and options of CS of the current root"""
    secs = self.bc.getSections(self.root)
    opts = self.bc.getOptions(self.root)
    if line.startswith("-") and "l" in line:
      for i in secs:
        print '\033[94m' + i + '\033[0m' + "  "
      for i in opts:
        print i + " "
    else:
      for i in secs:
        print '\033[94m' + i + '\033[0m' + "  ",
      for i in opts:
        print i + " ",
      print ""

  def do_cd(self, line):
    """cd
    Go one directory deeper in the CS"""
    # Check if invariant holds
    assert(self.root == "/" or not self.root.endswith("/"))
    assert(self.root.startswith("/"))
    secs = self.bc.getSections(self.root)
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
        print "cd: no such section: " + line

  def complete_cd(self, text, _line, _begidx, _endidx):
    secs = self.bc.getSections(self.root)
    return [(s + "/") for s in secs if s.startswith(text)]

  def do_cat(self, line):
    """cat
    Read the content of an option in the CS"""
    opts = self.bc.getOptionsDict(self.root)

    if line in opts.keys():
      print opts[line]
    else:
      print "cat: No such option"

  def complete_cat(self, text, _line, _begidx, _endidx):
    opts = self.bc.getOptions(self.root)
    return [o for o in opts if o.startswith(text)]

  do_less = do_cat
  complete_less = complete_cat

  def do_mkdir(self, line):
    """mkdir
    Create a new section in the CS"""
    self.bc.createSection(self.root + "/" + line)
    self.dirty = True

  complete_mkdir = complete_cd

  def do_rmdir(self, line):
    """rmdir
    Delete a section in the CS"""
    self.bc.removeSection(self.root + "/" + line)
    self.dirty = True

  complete_rmdir = complete_cd

  def do_rm(self, line):
    """rm
    Delete an option in the CS"""
    self.bc.removeOption(self.root + "/" + line)
    self.dirty = True

  complete_rm = complete_cat

  def do_set(self, line):
    """set
    Set an option in the CS (or create it if it does not exists)
    Usage: set <str> to set a string option (will be stored as a string in CS)
           set <str>,<str>,... to set a list option (will be stored as a list in CS)
    """
    line = line.split(" ", 2)
    if len(line) != 2:
      print "Usage: set <key> <value>"
    else:
      self.bc.setOptionValue(self.root + "/" + line[0], line[1])
      self.dirty = True

  complete_set = complete_cat

  def do_unset(self, line):
    """unset
    Unset an option in the CS: Making the option equal to the
    empty string."""
    self.bc.setOptionValue(self.root + "/" + line, "")
    self.dirty = True

  complete_unset = complete_cat

  def do_commit(self, _line):
    """commit
    Commit the modifications to the CS"""
    if self.dirty:
      self.bc.commit()

  def default(self, line):
    """Override [Cmd.default(line)] function."""
    if line == "EOF":
      if self.prompt:
        print
      return self.do_quit(line)
    else:
      cmd.Cmd.default(self, line)

  def do_quit(self, _line):
    """quit
    Quit"""
    if self.dirty:
      res = raw_input("Do you want to commit your changes ? [Y/n] ")
      if res.lower() in ["", "y", "yes"]:
        self.do_commit()

    return True

  do_exit = do_quit

def main():
  rsscmd = RSSConfigCmd()
  rsscmd.cmdloop()

if __name__ == "__main__":
  sys.exit(main())
