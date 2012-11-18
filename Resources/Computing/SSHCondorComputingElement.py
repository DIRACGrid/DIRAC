########################################################################
# $HeadURL$
# File : CondorComputingElement.py
# Author : A.Tsaregorodtsev
########################################################################

import os, stat, tempfile, shutil

from DIRAC.Resources.Computing.SSHComputingElement  import SSH, SSHComputingElement 
from DIRAC.Resources.Computing.PilotBundle          import bundleProxy 
from DIRAC import rootPath, S_OK, S_ERROR

CE_NAME = 'SSHCondor'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHCondorComputingElement( SSHComputingElement ):
       
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'condorce'
    self.mandatoryParameters = MANDATORY_PARAMETERS         
       
  def submitJob_old( self, executableFile, proxy, numberOfJobs = 1 ):

#    self.log.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0755 )

    # if no proxy is supplied, the executable can be submitted directly
    # otherwise a wrapper script is needed to get the proxy to the execution node
    # The wrapper script makes debugging more complicated and thus it is
    # recommended to transfer a proxy inside the executable if possible.
    if proxy:
      self.log.verbose( 'Setting up proxy for payload' )
      wrapperContent = bundleProxy( executableFile, proxy )
      fd, name = tempfile.mkstemp( suffix = '_wrapper.py', prefix = 'DIRAC_', dir = os.getcwd() )
      wrapper = os.fdopen( fd, 'w' )
      wrapper.write( wrapperContent )
      wrapper.close()
      submitFile = name
    else: # no proxy
      submitFile = executableFile

    ssh = SSH( parameters = self.ceParameters )
    # Copy the executable
    os.chmod( submitFile, stat.S_IRUSR | stat.S_IXUSR )
    sFile = os.path.basename( submitFile )
    result = ssh.scpCall( 10, submitFile, '%s/%s' % ( self.executableArea, os.path.basename( submitFile ) ) )

    # Submit jobs
    cmd = "bash --login -c '%s/%s %s/%s %s %s'" % ( self.sharedArea, 
                                                    self.controlScript, 
                                                    self.executableArea, 
                                                    os.path.basename( submitFile ), 
                                                    numberOfJobs, 
                                                    self.batchOutput )
    self.log.verbose( 'CE submission command: %s\n' %  cmd )
    result = ssh.sshCall( 10, cmd )

    if not result['OK']:
      self.log.error( '%s CE job submission failed' % self.ceType, result['Message'] )
      return result
    else:
      self.log.debug( '%s CE job submission OK' % self.ceType )

    sshStatus = result['Value'][0]
    sshStdout = result['Value'][1]
    sshStderr = result['Value'][2]
    
    # Examine results of the job submission
    if sshStatus == 0:
      outputLines = sshStdout.strip().replace('\r','').split('\n')
      try:
        status = int( outputLines[0] )
      except:
        return S_ERROR( "Failed local batch job submission:" % outputLines[0] )
      if status != 0:
        message = "Unknown reason"
        if len( outputLines ) > 1:
          message = outputLines[1]
        return S_ERROR( 'Failed job submission, reason: %s' % message )   
      else:
        batchIDs = outputLines[1:]
        jobIDs = [ self.ceType.lower()+'://'+self.ceParameters['SSHHost']+'/'+id for id in batchIDs ]    
        return S_OK( jobIDs )
    else:
      return S_ERROR( '\n'.join( [sshStdout,sshStderr] ) )
   
  def getJobStatus_old( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    jobIDs = list ( jobIDList )
    resultDict = {}
    ssh = SSH( parameters = self.ceParameters )
    cmd = ["bash --login -c 'condor_q -submitter %s'" % self.ceParameters["SSHUser"] ]
    ret = ssh.sshCall( 10, cmd )

    if not ret['OK']:
      self.log.error( 'Timeout', ret['Message'] )
      return ret

    status_q = ret['Value'][0]
    stdout_q = ret['Value'][1]
    stderr_q = ret['Value'][2]

    if status_q:
      self.log.error( 'Failed condor_q execution:', stderr )
      return S_ERROR( stderr )
    
    cmd = ["bash --login -c 'condor_history | grep %s'" % self.ceParameters["SSHUser"] ]
    ret = ssh.sshCall( 10, cmd )

    if not ret['OK']:
      self.log.error( 'Timeout', ret['Message'] )
      return ret

    status_history = ret['Value'][0]
    stdout_history = ret['Value'][1]
    stderr_history = ret['Value'][2]
    
    stdout = stdout_q
    if status_history == 0:
      stdout = '\n'.join( [stdout_q,stdout_history] )

    if len( stdout ):
      lines = stdout.split( '\n' )
      for line in lines:
        l = line.strip()
        for job in jobIDList:
          if l.startswith( job ):
            if " I " in line:
              resultDict[job] = 'Waiting'
            elif " R " in line:
              resultDict[job] = 'Running'
            elif " C " in line:
              resultDict[job] = 'Done'
            elif " X " in line:
              resultDict[job] = 'Aborted'    

    if len( resultDict ) == len( jobIDList ):
      return S_OK( resultDict )

    for job in jobIDList:
      if not job in resultDict:
        resultDict[job] = 'Unknown'
    return S_OK( resultDict )    
                
  def getCEStatus_old( self ):  
    """ Method to return information on running and pending jobs.
    """
    result = S_OK()
    ssh = SSH( parameters = self.ceParameters )
    cmd = ["bash --login -c 'condor_q -submitter %s'" % self.ceParameters["SSHUser"] ]
    ret = ssh.sshCall( 10, cmd )

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
      self.log.error( 'Failed condor_q execution:', stderr )
      return S_ERROR( stderr )
    waitingJobs = 0
    runningJobs = 0

    if len( stdout ):
      lines = stdout.split( '\n' )
      for line in lines:
        if not line.strip():
          continue
        if " I " in line:
          waitingJobs += 1
        elif " R " in line:
          runningJobs += 1  

    result['SubmittedJobs'] = self.submittedJobs
    result['WaitingJobs'] = waitingJobs
    result['RunningJobs'] = runningJobs

    self.log.verbose( 'Waiting Jobs: ', waitingJobs )
    self.log.verbose( 'Running Jobs: ', runningJobs )

    return result
   
  def getJobOutput_old( self, jobID, localDir = None ):   
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    jobNumber = jobID

    if not localDir:
      tempDir = tempfile.mkdtemp()
    else:
      tempDir = localDir
    ssh = SSH( parameters = self.ceParameters )
    result = ssh.scpCall( 20, '%s/%s.out' % ( tempDir, jobID ), '%s/%s.out' % ( self.batchOutput, jobNumber ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.out' % ( tempDir, jobID ) ):
      os.system( 'touch %s/%s.out' % ( tempDir, jobID ) )
    result = ssh.scpCall( 20, '%s/%s.err' % ( tempDir, jobID ), '%s/%s.err' % ( self.batchOutput, jobNumber ), upload = False )
    if not result['OK']:
      return result
    if not os.path.exists( '%s/%s.err' % ( tempDir, jobID ) ):
      os.system( 'touch %s/%s.err' % ( tempDir, jobID ) )
    # The result is OK, we can remove the output
    if self.removeOutput:
      result = ssh.sshCall( 10, 'rm -f %s/%s*' % ( self.batchOutput, jobNumber ) )
    if localDir:
      return S_OK( ( '%s/%s.out' % ( tempDir, jobID ), '%s/%s.err' % ( tempDir, jobID ) ) )
    else:
      # Return the output as a string
      outputFile = open( '%s/%s.out' % ( tempDir, jobID ), 'r' )
      output = outputFile.read()
      outputFile.close()
      outputFile = open( '%s/%s.err' % ( tempDir, jobID ), 'r' )
      error = outputFile.read()
      outputFile.close()
      shutil.rmtree( tempDir )

      return S_OK( ( output, error ) )   
