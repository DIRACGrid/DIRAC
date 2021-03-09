"""
ReportCLI class implementing command line interface to DIRAC Accounting
ReportGenerator Service. It is not complete yet

Once ready it could be used with a script as simple as:


from DIRAC.Core.Base import Script

Script.localCfg.addDefaultEntry("LogLevel", "info")
Script.parseCommandLine()

from DIRAC.AccountingSystem.Client.ReportCLI import ReportCLI

if __name__=="__main__":
    reli = ReportCLI()
    reli.start()


"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import sys
import datetime

from DIRAC.Core.Base.CLI import CLI, colorize
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC import gLogger


class ReportCLI(CLI):

  def __init__(self):
    CLI.__init__(self)
    self.do_connect(None)

  def start(self):
    """
    Start the command loop
    """
    if not self.connected:
      gLogger.error("Client is not connected")
    try:
      self.cmdloop()
    except KeyboardInterrupt as v:
      gLogger.warn("Received a keyboard interrupt.")
      self.do_quit("")

  def do_connect(self, args):
    """
    Tries to connect to the server
        Usage: connect
    """
    gLogger.info("Trying to connect to server")
    self.connected = False
    self.prompt = "(%s)> " % colorize("Not connected", "red")
    retVal = ReportsClient().ping()
    if retVal['OK']:
      self.prompt = "(%s)> " % colorize("Connected", "green")
      self.connected = True

  def printComment(self, comment):
    commentList = comment.split("\n")
    for commentLine in commentList[:-1]:
      print("# %s" % commentLine.strip())

  def showTraceback(self):
    import traceback
    type, value = sys.exc_info()[:2]
    print("________________________\n")
    print("Exception", type, ":", value)
    traceback.print_tb(sys.exc_info()[2])
    print("________________________\n")

  def __getDatetimeFromArg(self, dtString):
    if len(dtString) != 12:
      return False
    dt = datetime.datetime(year=int(dtString[0:4]),
                           month=int(dtString[4:6]),
                           day=int(dtString[6:8]))
    dt += datetime.timedelta(hours=int(dtString[8:10]),
                             minutes=int(dtString[10:12]))
    return dt
