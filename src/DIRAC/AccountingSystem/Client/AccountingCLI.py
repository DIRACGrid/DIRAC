"""
AccountingCLI class implementing command line administrative interface to
DIRAC Accounting DataStore Service
"""

import sys

from DIRAC import gLogger
from DIRAC.Core.Base.CLI import CLI, colorize
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


class AccountingCLI(CLI):
    def __init__(self):
        CLI.__init__(self)
        self.do_connect(None)
        self.objectLoader = ObjectLoader()

    def start(self):
        """
        Start the command loop
        """
        if not self.connected:
            gLogger.error("Client is not connected")
        try:
            self.cmdloop()
        except KeyboardInterrupt:
            gLogger.warn("Received a keyboard interrupt.")
            self.do_quit("")

    def do_connect(self, args):
        """
        Tries to connect to the server
            Usage: connect
        """
        gLogger.info("Trying to connect to server")
        self.connected = False
        self.prompt = f"({colorize('Not connected', 'red')})> "
        acClient = DataStoreClient()
        retVal = acClient.ping()
        if retVal["OK"]:
            self.prompt = f"({colorize('Connected', 'green')})> "
            self.connected = True

    def printComment(self, comment):
        commentList = comment.split("\n")
        for commentLine in commentList[:-1]:
            print(f"# {commentLine.strip()}")

    def showTraceback(self):
        import traceback

        type, value = sys.exc_info()[:2]
        print("________________________\n")
        print("Exception", type, ":", value)
        traceback.print_tb(sys.exc_info()[2])
        print("________________________\n")

    def do_showRegisteredTypes(self, args):
        """
        Get a list of registered types
          Usage : showRegisteredTypes
        """
        try:
            acClient = DataStoreClient()
            retVal = acClient.getRegisteredTypes()

            print(retVal)

            if not retVal["OK"]:
                gLogger.error(f"Error: {retVal['Message']}")
                return
            for typeList in retVal["Value"]:
                print(typeList[0])
                print(" Key fields:\n  %s" % "\n  ".join(typeList[1]))
                print(" Value fields:\n  %s" % "\n  ".join(typeList[2]))
        except Exception:
            self.showTraceback()
