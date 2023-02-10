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

    def do_registerType(self, args):
        """
        Registers a new accounting type
          Usage : registerType <typeName>
          <DIRACRoot>/DIRAC/AccountingSystem/Client/Types/<typeName>
           should exist and inherit the base type
        """
        try:
            argList = args.split()
            if argList:
                typeName = argList[0].strip()
            else:
                gLogger.error("No type name specified")
                return
            # Try to import the type
            result = self.objectLoader.loadObject(f"DIRAC.AccountingSystem.Client.Types.{typeName}")
            if not result["OK"]:
                return result
            typeClass = result["Value"]

            gLogger.info(f"Loaded type {typeClass.__name__}")
            typeDef = typeClass().getDefinition()
            acClient = DataStoreClient()
            retVal = acClient.registerType(*typeDef)
            if retVal["OK"]:
                gLogger.info("Type registered successfully")
            else:
                gLogger.error(f"Error: {retVal['Message']}")
        except Exception:
            self.showTraceback()

    def do_resetBucketLength(self, args):
        """
        Set the bucket Length. Will trigger a recalculation of buckets. Can take a while.
          Usage : resetBucketLength <typeName>
          <DIRACRoot>/DIRAC/AccountingSystem/Client/Types/<typeName>
           should exist and inherit the base type
        """
        try:
            argList = args.split()
            if argList:
                typeName = argList[0].strip()
            else:
                gLogger.error("No type name specified")
                return

            # Try to import the type
            result = self.objectLoader.loadObject(f"DIRAC.AccountingSystem.Client.Types.{typeName}")
            if not result["OK"]:
                return result
            typeClass = result["Value"]
            gLogger.info(f"Loaded type {typeClass.__name__}")
            typeDef = typeClass().getDefinition()
            acClient = DataStoreClient()
            retVal = acClient.setBucketsLength(typeDef[0], typeDef[3])
            if retVal["OK"]:
                gLogger.info("Type registered successfully")
            else:
                gLogger.error(f"Error: {retVal['Message']}")
        except Exception:
            self.showTraceback()

    def do_regenerateBuckets(self, args):
        """
        Regenerate buckets for type. Can take a while.
          Usage : regenerateBuckets <typeName>
          <DIRACRoot>/DIRAC/AccountingSystem/Client/Types/<typeName>
           should exist and inherit the base type
        """
        try:
            argList = args.split()
            if argList:
                typeName = argList[0].strip()
            else:
                gLogger.error("No type name specified")
                return

            # Try to import the type
            result = self.objectLoader.loadObject(f"DIRAC.AccountingSystem.Client.Types.{typeName}")
            if not result["OK"]:
                return result
            typeClass = result["Value"]
            gLogger.info(f"Loaded type {typeClass.__name__}")
            typeDef = typeClass().getDefinition()
            acClient = DataStoreClient()
            retVal = acClient.regenerateBuckets(typeDef[0])
            if retVal["OK"]:
                gLogger.info("Buckets recalculated!")
            else:
                gLogger.error(f"Error: {retVal['Message']}")
        except Exception:
            self.showTraceback()

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

    def do_deleteType(self, args):
        """
        Delete a registered accounting type.
          Usage : deleteType <typeName>
          WARN! It will delete all data associated to that type! VERY DANGEROUS!
          If you screw it, you'll discover a new dimension of pain and doom! :)
        """
        try:
            argList = args.split()
            if argList:
                typeName = argList[0].strip()
            else:
                gLogger.error("No type name specified")
                return

            choice = input(
                f"Are you completely sure you want to delete type {typeName} and all it's data? yes/no [no]: "
            )
            choice = choice.lower()
            if choice not in ("yes", "y"):
                print("Delete aborted")
                return

            acClient = DataStoreClient()
            retVal = acClient.deleteType(typeName)
            if not retVal["OK"]:
                gLogger.error(f"Error: {retVal['Message']}")
                return
            print("Hope you meant it, because it's done")
        except Exception:
            self.showTraceback()

    def do_compactBuckets(self, args):
        """
        Compact buckets table
          Usage : compactBuckets
        """
        try:
            acClient = DataStoreClient()
            retVal = acClient.compactDB()
            if not retVal["OK"]:
                gLogger.error(f"Error: {retVal['Message']}")
                return
            gLogger.info("Done")
        except Exception:
            self.showTraceback()
