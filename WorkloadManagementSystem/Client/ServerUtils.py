########################################################################
# File :   ServerUtils.py
# Author : Ricardo Graciani
########################################################################
"""
  Provide uniform interface to backend for local and remote clients.return

  There's a pretty big assumption here: that DB and Handler expose the same calls, with identical signatures.
  This is not exactly the case for WMS DBs and services.
"""

__RCSID__ = "$Id$"


def getDBOrClient(DB, serverName):
  """ Tries to instantiate the DB object
      and returns it if we manage to connect to the DB,
      otherwise returns a Client of the server
  """
  from DIRAC import gLogger
  from DIRAC.Core.DISET.RPCClient import RPCClient
  try:
    myDB = DB()
    if myDB._connected:
      return myDB
  except BaseException:
    pass

  gLogger.info('Can not connect to DB will use %s' % serverName)
  return RPCClient(serverName)


def getPilotAgentsDB():
  serverName = 'WorkloadManagement/Pilots'
  PilotAgentsDB = None
  try:
    from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
  except BaseException:
    pass
  return getDBOrClient(PilotAgentsDB, serverName)


pilotAgentsDB = getPilotAgentsDB()
