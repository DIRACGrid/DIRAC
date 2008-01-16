########################################################################
# $Id: gLiteAgentMonitor.py,v 1.2 2008/01/16 14:50:00 paterson Exp $
# File :   gLiteAgentMonitor.py
# Author : Stuart Paterson
########################################################################

""" The gLite Agent Monitor performs the pilot job status tracking activity for LCG.
"""

__RCSID__ = "$Id: gLiteAgentMonitor.py,v 1.2 2008/01/16 14:50:00 paterson Exp $"

from DIRACEnvironment                                        import DIRAC
from DIRAC.Core.Utilities.Subprocess                         import shellCall
from DIRAC.Core.Utilities                                    import List
from DIRAC.WorkloadManagementSystem.PilotAgent.AgentMonitor  import AgentMonitor
from DIRAC                                                   import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time

class gLiteAgentMonitor(AgentMonitor):

  #############################################################################
  def __init__(self,configPath,mode):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger('%sAgentMonitor' %(mode))
    self.sectionPath = configPath
    self.cmd = gConfig.getValue(self.sectionPath+'/StatusCommand','glite-wms-job-status')
    self.cmdTimeout = gConfig.getValue(self.sectionPath+'/StatusCommandTimeout',60)
    self.agentMonConfig = '/%s/%s' % ( '/'.join( List.fromChar(configPath, '/' )[:-1] ), 'AgentMonitor')
    AgentMonitor.__init__(self,self.agentMonConfig,mode)

  #############################################################################
  def getPilotStatus(self,jobID,pilotID):
    """Get LCG job status information using the job's owner proxy and
       LCG job IDs. Returns for each JobID its status in the LCG WMS and
       its destination CE as a tuple of 2 elements
    """
    self.__checkProxy()
    cmd = "%s %s" % (self.cmd,pilotID)
    self.log.info( '--- Executing %s for %s' %(cmd,jobID) )
    result = self.__exeCommand(cmd)

    if not result['OK']:
      self.log.warn(result)
      return result

    status = result['Status']
    stdout = result['StdOut']
    queryTime = result['Time']
    timing = '>>> gLite status query time %.2fs' % queryTime
    self.log.info( timing )
    destination=None
    jobStatus=None
    if status == 0:
      jobStatus = ''
      lines = stdout.split('\n')
      for line in lines:
        if line.find('Current Status:') != -1 :
          jobStatus = re.search(':\s+(\w+)',line).group(1)
        if line.find('Destination:') != -1 :
          destination = line.split()[1].split(":")[0]

      self.log.info('JobID: %s, PilotStatus: %s, Destination: %s' %(jobID,jobStatus,destination))
      pilot = S_OK()
      pilot['JobID']=jobID
      pilot['PilotStatus']=jobStatus
      pilot['Destination']=destination
      if jobStatus == 'Aborted':
        pilot['Aborted']=True
      elif jobStatus == 'Waiting' or jobStatus == 'Ready' or jobStatus == 'Scheduled' or jobStatus == 'Submitted' or jobStatus == 'Running':
        pilot['Aborted']=False
      elif jobStatus=='Done':
        pilot['Aborted']=False
        self.log.verbose('Pilot %s has entered the Done status' %(pilotID))
      else:
        self.log.warn('Unknown status %s for pilot %s' %(jobStatus,pilotID))
        pilot['Aborted']=False

      return pilot
    else:
      return result

  #############################################################################
  def __checkProxy(self):
    """Print some debugging information for the current proxy.
    """
    proxyInfo = shellCall(self.cmdTimeout,'grid-proxy-info -debug')
    status = proxyInfo['Value'][0]
    stdout = proxyInfo['Value'][1]
    stderr = proxyInfo['Value'][2]
    self.log.verbose('Status %s' %status)
    self.log.verbose(stdout)
    self.log.verbose(stderr)

  #############################################################################
  def __exeCommand(self,cmd):
    """Runs a submit / list-match command and prints debugging information.
    """
    start = time.time()
    self.log.verbose( cmd )
    result = shellCall(60,cmd)

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]
    self.log.verbose('Status = %s' %status)
    self.log.verbose(stdout)
    if stderr:
      self.log.warn(stderr)
    result['Status']=status
    result['StdOut']=stdout
    result['StdErr']=stderr
    subtime = time.time() - start
    result['Time']=subtime
    return result

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#