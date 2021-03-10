"""
AccountingCLI class implementing command line administrative interface to
DIRAC Accounting DataStore Service
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import sys

import six

from DIRAC import gLogger
from DIRAC.Core.Base.CLI import CLI, colorize
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient


class AccountingCLI(CLI):

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
    self.prompt = "(%s)> " % colorize("Not connected", "red")
    acClient = DataStoreClient()
    retVal = acClient.ping()
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
      try:
        typeModule = __import__("DIRAC.AccountingSystem.Client.Types.%s" % typeName,
                                globals(),
                                locals(), typeName)
        typeClass = getattr(typeModule, typeName)
      except Exception as e:
        gLogger.error("Can't load type %s: %s" % (typeName, str(e)))
        return
      gLogger.info("Loaded type %s" % typeClass.__name__)
      typeDef = typeClass().getDefinition()
      acClient = DataStoreClient()
      retVal = acClient.registerType(*typeDef)
      if retVal['OK']:
        gLogger.info("Type registered successfully")
      else:
        gLogger.error("Error: %s" % retVal['Message'])
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
      try:
        typeModule = __import__("DIRAC.AccountingSystem.Client.Types.%s" % typeName,
                                globals(),
                                locals(), typeName)
        typeClass = getattr(typeModule, typeName)
      except Exception as e:
        gLogger.error("Can't load type %s: %s" % (typeName, str(e)))
        return
      gLogger.info("Loaded type %s" % typeClass.__name__)
      typeDef = typeClass().getDefinition()
      acClient = DataStoreClient()
      retVal = acClient.setBucketsLength(typeDef[0], typeDef[3])
      if retVal['OK']:
        gLogger.info("Type registered successfully")
      else:
        gLogger.error("Error: %s" % retVal['Message'])
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
      try:
        typeModule = __import__("DIRAC.AccountingSystem.Client.Types.%s" % typeName,
                                globals(),
                                locals(), typeName)
        typeClass = getattr(typeModule, typeName)
      except Exception as e:
        gLogger.error("Can't load type %s: %s" % (typeName, str(e)))
        return
      gLogger.info("Loaded type %s" % typeClass.__name__)
      typeDef = typeClass().getDefinition()
      acClient = DataStoreClient()
      retVal = acClient.regenerateBuckets(typeDef[0])
      if retVal['OK']:
        gLogger.info("Buckets recalculated!")
      else:
        gLogger.error("Error: %s" % retVal['Message'])
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

      if not retVal['OK']:
        gLogger.error("Error: %s" % retVal['Message'])
        return
      for typeList in retVal['Value']:
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
      while True:
        choice = six.moves.input(
            "Are you completely sure you want to delete type %s and all it's data? yes/no [no]: " %
            typeName)
        choice = choice.lower()
        if choice in ("yes", "y"):
          break
        else:
          print("Delete aborted")
          return
      acClient = DataStoreClient()
      retVal = acClient.deleteType(typeName)
      if not retVal['OK']:
        gLogger.error("Error: %s" % retVal['Message'])
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
      if not retVal['OK']:
        gLogger.error("Error: %s" % retVal['Message'])
        return
      gLogger.info("Done")
    except Exception:
      self.showTraceback()
