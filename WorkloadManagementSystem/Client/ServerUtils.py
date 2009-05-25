########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Client/ServerUtils.py,v 1.1 2009/05/25 07:20:20 rgracian Exp $
# File :   ServerUtils.py
# Author : Ricardo Graciani
########################################################################
"""
  Provide uniform interface to backend for local and remote clients (ie Director Agents)
"""

__RCSID__ = "$Id: ServerUtils.py,v 1.1 2009/05/25 07:20:20 rgracian Exp $"

def getDBOrClient( DB, serverName ):
  # Try to instantiate the DB object and return it we managed to connect to the DB
  # otherwise return a Client of the server
  from DIRAC.Core.DISET.RPCClient                            import RPCClient
  try:
    myDB = DB()
    if myDB._connected:
      return myDB
  except:
    pass

  gLogger.info('Can not connect to PilotAgentsDB will use %s' % serverName )
  return RPCClient( serverName )

def getPilotAgentsDB():
  serverName = 'WorkloadManagement/PilotAgents'
  PilotAgentsDB = None
  try:
    from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB       import PilotAgentsDB
  except:
    pass
  return getDBOrClient( PilotAgentsDB, serverName )

def getTaskQueueDB():
  serverName = 'WorkloadManagement/WMSAdministrator'
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

pilotAgentsDB     = getPilotAgentsDB()
taskQueueDB       = getTaskQueueDB()
jobDB             = getJobDB()
