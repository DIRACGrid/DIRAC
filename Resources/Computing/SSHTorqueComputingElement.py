########################################################################
# $HeadURL$
# File :   SSHTorqueComputingElement.py
# Author : A.T.
########################################################################

""" Torque Computing Element with remote job submission via ssh/scp and using site
    shared area for the job proxy placement
"""

__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.SSHComputingElement       import SSHComputingElement 
from DIRAC.Core.Utilities.Pfn                            import pfnparse
from DIRAC                                               import S_OK, S_ERROR
from DIRAC.Resources.Computing.SSHComputingElement       import SSH 
from DIRAC.Core.Utilities.List                           import breakListIntoChunks

import re

CE_NAME = 'SSHTorque'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHTorqueComputingElement( SSHComputingElement ):
""" Torque CE interface, via SSH
"""

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'torquece'
    self.mandatoryParameters = MANDATORY_PARAMETERS

  #############################################################################
  def __execRemoteSSH(self, ssh, cmd) :
    """ execute command via ssh connection and return stdout in case of success
    otherwise stderr"""

    ret = ssh.sshCall(10, cmd)

    if not ret['OK']:
      self.log.error( 'Timeout', ret['Message'] )
      return ret
    
    status = ret['Value'][0]
    stdout = ret['Value'][1]
    stderr = ret['Value'][2]

    self.log.debug( "status:", status )
    self.log.debug( "stdout:", stdout )
    self.log.debug( "stderr:", stderr )

    if status:
      self.log.error( 'Failed remote execution of command "%s": %s:' % (' '.join(cmd), stderr) )
      return S_ERROR( stderr )

    return S_OK(stdout)


  #############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """

    result = S_OK()
    result['SubmittedJobs'] = self.submittedJobs

    ssh = SSH( parameters = self.ceParameters )

    print 'STEFAN',self.ceParameters

    if self.ceParameters.has_key('batchUser') :

#      cmd = ["qstat", "-i", "-u", self.ceParameters['batchUser'], self.queue, "|", "grep", self.queue, "|", "wc", "-l"]
      cmd = ["qselect", "-u", self.ceParameters['batchUser'], "-s", "QW", "|", "wc", "-l"]

      ret = self.__execRemoteSSH( ssh, cmd )

      if not ret['OK'] :
        self.log.error( ret['Message'] )
        return ret

      waitingJobs = int(ret['Value'])

#      cmd = ["qstat", "-r", "-u", self.ceParameters['batchUser'], self.queue, "|", "grep", self.queue, "|", "wc", "-l"]
      cmd = ["qselect", "-u", self.ceParameters['batchUser'], "-s", "R", "|", "wc", "-l"]

      ret = self.__execRemoteSSH( ssh, cmd )

      if not ret['OK'] :
        self.log.error( ret['Message'] )
        return ret

      runningJobs = int(ret['Value'])

    else :

      cmd = ["qstat", "-Q" , self.execQueue ]

      ret = self.__execRemoteSSH( ssh, cmd )

      if not ret['OK']:
        self.log.error( ret['Message'] )
        return ret

      matched = re.search( self.queue + "\D+(\d+)\D+(\d+)\W+(\w+)\W+(\w+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\D+(\d+)\W+(\w+)", ret['Value'] )

      if matched.groups < 6:
        return S_ERROR( "Error retrieving information from qstat:" + ret['Value'] )

      try:
        waitingJobs = int( matched.group( 5 ) )
        runningJobs = int( matched.group( 6 ) )
      except:
        return S_ERROR( "Error retrieving information from qstat:" + ret['Value'] )

    result['WaitingJobs'] = waitingJobs
    result['RunningJobs'] = runningJobs

    self.log.verbose( 'Waiting Jobs: ', waitingJobs )
    self.log.verbose( 'Running Jobs: ', runningJobs )

    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """

    resultDict = {}
    ssh = SSH( parameters = self.ceParameters )

    for jobList in breakListIntoChunks( jobIDList, 100 ):
      
      jobDict = {}
      for job in jobList:
        result = pfnparse( job )
        if result['OK']:
          stamp = result['Value']['FileName'].split('.')[0] 
        else:
          self.log.error( 'Invalid job id', job )
          continue  
        jobDict[stamp] = job
      stampList = jobDict.keys() 

      cmd = [ 'qstat', ' '.join( stampList ) ]
      result = ssh.sshCall( 10, cmd )
      if not result['OK']:
        return result
      output = result['Value'][1].replace( '\r', '' )
      lines = output.split( '\n' )
      for job in jobDict:
        resultDict[jobDict[job]] = 'Unknown'
        for line in lines:
          if line.find( job ) != -1:
            if line.find( 'Unknown' ) != -1:
              resultDict[jobDict[job]] = 'Unknown'
            else:
              torqueStatus = line.split()[4]
              if torqueStatus in ['E', 'C']:
                resultDict[jobDict[job]] = 'Done'
              elif torqueStatus in ['R']:
                resultDict[jobDict[job]] = 'Running'
              elif torqueStatus in ['S', 'W', 'Q', 'H', 'T']:
                resultDict[jobDict[job]] = 'Waiting'

    return S_OK( resultDict )

  def _getJobOutputFiles( self, jobID ):
    """ Get output file names for the specific CE 
    """
    result = pfnparse( jobID )
    if not result['OK']:
      return result
    jobStamp = result['Value']['FileName']
    host = result['Value']['Host']

    output = '%s/DIRACPilot.o%s' % ( self.batchOutput, jobStamp )
    error = '%s/DIRACPilot.e%s' % ( self.batchError, jobStamp )

    return S_OK( (jobStamp, host, output, error) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
