#!/usr/bin/env python

"""
CLI to edit RSS Config
"""
import sys
import os.path
import cmd

from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors = True)

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException
from DIRAC.ResourceStatusSystem.Utilities.BrowseConfig import getSections, getOptions, createSection, \
    rssConfigRootPath, setOption

# Invariants:
# * root does not end with "/" or root is "/"
# * root starts with "/"

class RSSConfigCmd(cmd.Cmd):

  root = rssConfigRootPath
  prompt = "[cfgedit " + root + " ]% "

  def __init__(self):
    cmd.Cmd.__init__(self)

  def update_prompt(self):
    self.prompt = "[cfgedit " + self.root + " ]% "

  def do_ls(self, line):
    secs = getSections(self.root)
    opts = getOptions(self.root)
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
    # Check if invariant holds
    assert(self.root == "/" or not self.root.endswith("/"))
    assert(self.root.startswith("/"))
    secs = getSections(self.root)
    if line == "..":
      self.root = os.path.dirname(self.root)
      self.update_prompt()
    else:
      if os.path.dirname(line) in secs:
        if self.root == "/":
          self.root = os.path.normpath(self.root + os.path.dirname(line))
        else:
          self.root = os.path.normpath(self.root + "/" + os.path.dirname(line))
        self.update_prompt()
      else:
        print "cd: no such section: " + line

  def complete_cd(self, text, _line, _begidx, _endidx):
    secs = getSections(self.root)
    return [(s + "/") for s in secs if s.startswith(text)]

  def do_cat(self, line):
    opts = getOptions(self.root)
    if line in opts:
      print opts[line]
    else:
      print "cat: No such option"

  def complete_cat(self, text, _line, _begidx, _endidx):
    opts = getOptions(self.root)
    return [o for o in opts if o.startswith(text)]

  def do_less(self, line):
    return self.do_cat(line)

  def complete_less(self, text, _line, _begidx, _endidx):
    return self.complete_cat(text, _line, _begidx, _endidx)

  def do_mkdir(self, line):
    """Create a new section in the CS"""
    try:
      createSection(self.root + "/" + line)
    except RSSException:
      print "Unable to create section \"" + line + "\": Not allowed"

  def do_set(self, line):
    line = line.split(" ")
    if len(line) != 2:
      print "Usage: set <key> <value>"
    else:
      return setOption(self.root + "/" + line[0], line[1])

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
    return True

  def do_exit(self, line):
    """exit
    Quit"""
    return self.do_quit(line)

def main():
  rsscmd = RSSConfigCmd()
  rsscmd.cmdloop()

if __name__ == "__main__":
  sys.exit(main())
