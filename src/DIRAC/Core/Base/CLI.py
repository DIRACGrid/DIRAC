########################################################################
# File :   CLI.py
# Author : Andrei Tsaregorodtsev
########################################################################

""" CLI is the base class for all the DIRAC consoles ( CLIs ). It contains
    several utilities and signal handlers of general purpose.
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import six
import cmd
import sys
import os
import signal

from DIRAC import gLogger

gColors = {'red': 1, 'green': 2, 'yellow': 3, 'blue': 4}


def colorEnabled():
  return os.environ.get('TERM') in ('xterm', 'xterm-color')


def colorize(text, color):
  """Return colorized text"""
  if not colorEnabled():
    return text

  startCode = '\033[;3'
  endCode = '\033[0m'
  if isinstance(color, six.integer_types):
    return "%s%sm%s%s" % (startCode, color, text, endCode)
  try:
    return "%s%sm%s%s" % (startCode, gColors[color], text, endCode)
  except Exception:
    return text


class CLI(cmd.Cmd):

  def __init__(self):

    cmd.Cmd.__init__(self)
    self.indentSpace = 20
    self._initSignals()

  def _handleSignal(self, sig, frame):

    print("\nReceived signal", sig, ", exiting ...")
    self.do_quit(self)

  def _initSignals(self):
    """
    Registers signal handlers
    """
    for sigNum in (signal.SIGINT, signal.SIGQUIT, signal.SIGKILL, signal.SIGTERM):
      try:
        signal.signal(sigNum, self._handleSignal)
      except Exception:
        pass

  def _errMsg(self, errMsg):
    """
    Print out a colorized error log message

    :param str errMsg: error message string
    :return: nothing
    """
    gLogger.error("%s %s" % (colorize("[ERROR]", "red"), errMsg))

  def emptyline(self):
    pass

  def do_exit(self, args):
    """ Exit the shell.

    usage: exit
    """
    self.do_quit(self)

  def do_quit(self, args):
    """ Exit the shell.

    usage: quit
    """
    gLogger.notice('')
    sys.exit(0)

  def do_EOF(self, args):
    """ Handler for EOF ( Ctrl D ) signal - perform quit
    """
    self.do_quit(args)

  def do_execfile(self, args):
    """ Execute a series of CLI commands from a given file

        usage:

          execfile <filename>
    """

    argss = args.split()
    fname = argss[0]
    if not os.path.exists(fname):
      print("Error: File not found %s" % fname)
      return
    with open(fname, "r") as input_cmd:
      contents = input_cmd.readlines()
    for line in contents:
      try:
        gLogger.notice("\n--> Executing %s\n" % line)
        self.onecmd(line)
      except Exception as error:
        self._errMsg(str(error))
        break
    return

  def printPair(self, key, value, separator=":"):
    valueList = value.split("\n")
    print("%s%s%s %s" % (key, " " * (self.indentSpace - len(key)), separator, valueList[0].strip()))
    for valueLine in valueList[1:-1]:
      print("%s  %s" % (" " * self.indentSpace, valueLine.strip()))

  def do_help(self, args):
    """
    Shows help information
        Usage: help <command>
        If no command is specified all commands are shown
    """
    if len(args) == 0:
      print("\nAvailable commands:\n")
      attrList = sorted(dir(self))
      for attribute in attrList:
        if attribute.startswith("do_"):
          self.printPair(attribute[3:], getattr(self, attribute).__doc__[1:])
          print("")
    else:
      command = args.split()[0].strip()
      try:
        obj = getattr(self, "do_%s" % command)
      except Exception:
        print("There's no such %s command" % command)
        return
      self.printPair(command, obj.__doc__[1:])
