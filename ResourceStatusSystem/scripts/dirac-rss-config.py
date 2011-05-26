#!/usr/bin/env python

"""
CLI to edit RSS Config
"""
import sys
import os.path
import cmd

from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors = True)

from DIRAC.ResourceStatusSystem.Utilities.BrowseConfig import getDictRootedAt, getSections, getOptions

class RSSConfigCmd(cmd.Cmd):

  root = "/"
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
    secs = getSections(self.root)
    if line == "..":
      self.root = os.path.split(self.root)[0]
      self.update_prompt()
    else:
      if line in secs:
        if self.root.endswith("/"):
          self.root = self.root + line
        else:
          self.root = self.root + "/" + line
        self.update_prompt()
      else:
        print "cd: no such section: " + line

  def complete_cd(self, text, line, _begidx, _endidx):
    secs = getSections(self.root)
    return [s for s in secs if s.startswith(text)]

  def do_cat(self, line):
    opts = getOptions(self.root)
    print opts[line]

  def complete_cat(self, text, line, _begidx, _endidx):
    opts = getOptions(self.root)
    return [o for o in opts if o.startswith(text)]

  def do_less(self, line):
    return self.do_cat(line)

  def complete_less(self, text, line, _begidx, _endidx):
    return self.complete_cat(text, line, _begidx, _endidx)

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
