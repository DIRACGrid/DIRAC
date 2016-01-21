#########################################################################################
# $HeadURL$
# SLURM.py
# 10.11.2014
# Author: A.T.
#########################################################################################

""" SLURM.py is a DIRAC independent class representing SLURM batch system.
    SLURM objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes 
"""

__RCSID__ = "$Id$"

import commands, os, re

class SLURM( object ):

  def submitJob( self, **kwargs ):
    """ Submit nJobs to the OAR batch system
    """
    
    resultDict = {}
    
    MANDATORY_PARAMETERS = [ 'Executable', 'OutputDir', 'ErrorDir',
                             'Queue', 'SubmitOptions' ]

    for argument in MANDATORY_PARAMETERS:
      if not argument in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict
    
    nJobs = kwargs.get( 'NJobs' )
    if not nJobs:
      nJobs = 1
      
    outputDir = kwargs['OutputDir']
    errorDir = kwargs['ErrorDir']  
    queue = kwargs['Queue']
    submitOptions = kwargs['SubmitOptions']
    executable = kwargs['Executable']
    
    outFile = os.path.join( outputDir , "%jobid%" )
    errFile = os.path.join( errorDir , "%jobid%" )
    outFile = os.path.expandvars( outFile )
    errFile = os.path.expandvars( errFile )
    executable = os.path.expandvars( executable ) 

    jobIDs = []   
    for _i in range( nJobs ):
      jid = ''
      cmd = "sbatch -o %s/%%j.out --cluster=%s %s %s" % ( outputDir, queue, submitOptions, executable )
      status, output = commands.getstatusoutput( cmd )

      if status != 0 or not output:
        break
  
      lines = output.split( '\n' )
      for line in lines:
        result = re.search( 'Submitted batch job (\d*)', line )
        if result:
          jid = result.groups()[0]
          break
  
      if not jid:
        break
  
      jid = jid.strip()
      jobIDs.append( jid )
  
    if jobIDs:
      resultDict['Status'] = 0
      resultDict['Jobs'] = jobIDs
    else:
      resultDict['Status'] = status
      resultDict['Message'] = output
    return resultDict
  
  
  def killJob( self, **kwargs ):
    """ Delete a job from OAR batch scheduler. Input: list of jobs output: int
    """
  
    resultDict = {}
    
    MANDATORY_PARAMETERS = [ 'JobIDList', 'Queue' ]
    for argument in MANDATORY_PARAMETERS:
      if not argument in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict   
      
    jobIDList = kwargs['JobIDList']  
    if not jobIDList:
      resultDict['Status'] = -1
      resultDict['Message'] = 'Empty job list'
      return resultDict
    
    queue = kwargs['Queue']
    
    successful = []
    failed = []
    for job in jobIDList:
      cmd = 'scancel --cluster=%s %s' % ( queue, job )
      status, output = commands.getstatusoutput( cmd )
       
      if status != 0:
        failed.append( job )
      else:
        successful.append( job )
        
    resultDict['Status'] = 0
    if failed:
      resultDict['Status'] = 1
      resultDict['Message'] = output
    resultDict['Successful'] = successful
    resultDict['Failed'] = failed
    return resultDict      
  
  def getJobStatus( self, **kwargs ):
    """ Get status of the jobs in the given list
    """
  
    resultDict = {}
    
    MANDATORY_PARAMETERS = [ 'JobIDList', 'Queue' ]
    for argument in MANDATORY_PARAMETERS:
      if not argument in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict   
      
    jobIDList = kwargs['JobIDList']  
    if not jobIDList:
      resultDict['Status'] = -1
      resultDict['Message'] = 'Empty job list'
      return resultDict
    
    user = kwargs.get( 'User' )
    if not user:
      user = os.environ.get( 'USER' )
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict
    
    queue = kwargs['Queue']  
  
    cmd = "squeue --cluster=%s --user=%s --format='%%j %%T' " % ( queue, user )
    status, output = commands.getstatusoutput( cmd )
    
    if status != 0:
      resultDict['Status'] = 1
      resultDict['Message'] = output
      
    statusDict = {}  
    lines = output.split( '\n' )
    jids = set()
    for line in lines[1:]:
      jid, status = line.split()
      jids.add( jid )
      if jid in jobIDList:
        if status in ['PENDING', 'SUSPENDED', 'CONFIGURING' ]:
          statusDict[jid] = 'Waiting'
        elif status in ['RUNNING', 'COMPLETING']:
          statusDict[jid] = 'Running'
        elif status in ['CANCELLED', 'PREEMPTED']:  
          statusDict[jid] = 'Aborted'
        elif status in ['COMPLETED' ]:
          statusDict[jid] = 'Done'
        elif status in ['FAILED', 'TIMEOUT', 'NODE_FAIL' ]:   
          statusDict[jid] = 'Failed'
        else:
          statusDict[jid] = 'Unknown'
          
    leftJobs = set( jobIDList ) - jids
    for jid in leftJobs:
      statusDict[jid] = 'Unknown'                       
         
    # Final output
    status = 0
    resultDict['Status'] = 0
    resultDict['Jobs'] = statusDict 
    return resultDict   
  
  def getCEStatus( self, **kwargs ):
    """  Get the overall status of the CE
    """
  
    resultDict = {}
    
    MANDATORY_PARAMETERS = [ 'Queue' ]
    for argument in MANDATORY_PARAMETERS:
      if not argument in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict   
    
    user = kwargs.get( 'User' )
    if not user:
      user = os.environ.get( 'USER' )
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict
    
    queue = kwargs['Queue']  
  
    cmd = "squeue --cluster=%s --user=%s --format='%%j %%T' " % ( queue, user )
    status, output = commands.getstatusoutput( cmd )
    
    if status != 0:
      resultDict['Status'] = 1
      resultDict['Message'] = output
      
    waitingJobs = 0
    runningJobs = 0
    lines = output.split( '\n' )
    for line in lines[1:]:
      _jid, status = line.split()
      if status in ['PENDING', 'SUSPENDED', 'CONFIGURING' ]:
        waitingJobs += 1
      elif status in ['RUNNING', 'COMPLETING']:
        runningJobs += 1
          
    # Final output
    resultDict['Status'] = 0
    resultDict["Waiting"] = waitingJobs
    resultDict["Running"] = runningJobs
    return resultDict
  