########################################################################
# File :   ServerUtils.py
# Author : Ricardo Graciani
########################################################################
"""
  Provide uniform interface to backend for local and remote clients (ie Director Agents)
"""

__RCSID__ = "$Id$"

def getDBOrClient( DB, serverName ):
  """ Tries to instantiate the DB object and returns it if we manage to connect to the DB, otherwise returns a Client of the server
  """
  from DIRAC import gLogger
  from DIRAC.Core.DISET.RPCClient                            import RPCClient
  try:
    myDB = DB()
    if myDB._connected:
      return myDB
  except:
    pass

  gLogger.info( 'Can not connect to DB will use %s' % serverName )
  return RPCClient( serverName )

def getPilotAgentsDB():
  serverName = 'WorkloadManagement/WMSAdministrator'
  PilotAgentsDB = None
  try:
    from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB       import PilotAgentsDB
  except:
    pass
  return getDBOrClient( PilotAgentsDB, serverName )

def getTaskQueueDB():
  serverName = 'WorkloadManagement/Matcher'
  TaskQueueDB = None
  try:
    from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB         import TaskQueueDB
  except:
    pass
  return getDBOrClient( TaskQueueDB, serverName )

def getJobDB():
  serverName = 'WorkloadManagement/WMSAdministrator'
  JobDB = None
  try:
    from DIRAC.WorkloadManagementSystem.DB.JobDB               import JobDB
  except:
    pass
  return getDBOrClient( JobDB, serverName )

pilotAgentsDB = getPilotAgentsDB()
taskQueueDB = getTaskQueueDB()
jobDB = getJobDB()
