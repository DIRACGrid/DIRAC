#########################################################################################
# $HeadURL$
# Condor.py
# 10.11.2014
# Author: A.T.
#########################################################################################

""" Condor.py is a DIRAC independent class representing Condor batch system.
    Condor objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes 
"""

__RCSID__ = "$Id$"

import re, tempfile, commands, os

def parseCondorStatus( lines, jobID ):
  """parse the condor_q or condor_history output for the job status

  :param list lines: list of lines from the output of the condor commands, each line is a pair of jobID and statusID
  :param str jobID: jobID of condor job, e.g.: 123.53
  :returns: Status as known by DIRAC
  """
  jobID = str(jobID)
  for line in lines:
    l = line.strip().split()
    try:
      status = int( l[1] )
    except (ValueError, IndexError):
      continue
    if l[0] == jobID:
      return { 1: 'Waiting',
               2: 'Running',
               3: 'Aborted',
               4: 'Done',
               5: 'HELD'
             }.get( status, 'Unknown' )
  return 'Unknown'

def treatCondorHistory( condorHistCall, qList ):
  """concatenate clusterID and processID to get the same output as condor_q
  until we can expect condor version 8.5.3 everywhere

  :param str condorHistCall: condor_history command to run
  :param list qList: list of jobID and status from condor_q output, will be modified in this function
  :returns: None
  """
  status_history,stdout_history_temp = commands.getstatusoutput( condorHistCall )

  ## Join the ClusterId and the ProcId and add to existing list of statuses
  if status_history==0:
    for line in stdout_history_temp.split('\n'):
      values = line.strip().split()
      if len(values) == 3:
        qList.append( "%s.%s %s" % tuple(values) )


class Condor( object ):

  def submitJob( self, **kwargs ):
    """ Submit nJobs to the Condor batch system
    """
  
    resultDict = {}
    
    MANDATORY_PARAMETERS = [ 'Executable', 'OutputDir', 'SubmitOptions' ]

    for argument in MANDATORY_PARAMETERS:
      if not argument in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict
    
    nJobs = kwargs.get( 'NJobs' )
    if not nJobs:
      nJobs = 1
    outputDir = kwargs['OutputDir'] 
    executable = kwargs['Executable']  
    submitOptions = kwargs['SubmitOptions']
  
    jdlFile = tempfile.NamedTemporaryFile( dir=outputDir, suffix=".jdl" )
    jdlFile.write("""
    Executable = %s
    Universe = vanilla
    Requirements   = OpSys == "LINUX"
    Initialdir = %s
    Output = $(Cluster).$(Process).out
    Error = $(Cluster).$(Process).err
    Log = test.log
    Environment = CONDOR_JOBID=$(Cluster).$(Process)
    Getenv = True
    
    Queue %s
    
    """ % ( executable, outputDir, nJobs )
    )
    
    jdlFile.flush()
    
    status,output = commands.getstatusoutput( 'condor_submit %s %s' % ( submitOptions, jdlFile.name ) )
    
    jdlFile.close()
    
    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = output
      return resultDict
     
    submittedJobs = 0
    cluster = ''
    if status == 0:
      lines = output.split( '\n' )
      for line in lines:
        if 'cluster' in line:
          result = re.match( '(\d+) job.*cluster (\d+)\.', line )
          if result:
            submittedJobs, cluster = result.groups()
            try:
              submittedJobs = int( submittedJobs )
            except:
              submittedJobs = 0      
    
    if submittedJobs > 0 and cluster:
      resultDict['Status'] = 0
      resultDict['Jobs'] = []
      for i in range( submittedJobs ):
        resultDict['Jobs'].append( '.'.join( [cluster,str(i)] ) )
    else:
      resultDict['Status'] = status
      resultDict['Message'] = output
    return resultDict   

  def killJob( self, **kwargs ):
    """ Kill jobs in the given list
    """
    
    resultDict = {}
    
    MANDATORY_PARAMETERS = [ 'JobIDList' ]
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
    
    successful = []
    failed = []
    for job in jobIDList:
      status, output = commands.getstatusoutput( 'condor_rm %s' % job )
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
    
    MANDATORY_PARAMETERS = [ 'JobIDList' ]
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
    
    status,stdout_q = commands.getstatusoutput( 'condor_q -submitter %s -af:j JobStatus  ' % user )
    
    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = stdout_q
      return resultDict

    qList = stdout_q.strip().split('\n')

    ##FIXME: condor_history does only support j for autoformat from 8.5.3,
    ## format adds whitespace for each field This will return a list of 1245 75 3
    ## needs to cocatenate the first two with a dot
    condorHistCall = 'condor_history -af ClusterId ProcId JobStatus -submitter %s' % user
    treatCondorHistory( condorHistCall, qList )
  
    statusDict = {}
    if len( qList ):
      for job in jobIDList:
        job = str(job)
        statusDict[job] = parseCondorStatus( qList, job )
        if statusDict[job] == 'HELD':
          statusDict[job] = 'Unknown'

    # Final output
    status = 0
    resultDict['Status'] = 0
    resultDict['Jobs'] = statusDict 
    return resultDict     
  
  def getCEStatus( self, **kwargs ):
    """  Get the overall status of the CE 
    """ 
    resultDict = {}
  
    user = kwargs.get( 'User' )
    if not user:
      user = os.environ.get( 'USER' )
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict
    
    waitingJobs = 0
    runningJobs = 0
  
    status, output = commands.getstatusoutput( 'condor_q -submitter %s' % user )
    if status != 0:
      if "no record" in output:
        resultDict['Status'] = 0
        resultDict["Waiting"] = waitingJobs
        resultDict["Running"] = runningJobs
        return resultDict
      resultDict['Status'] = status
      resultDict['Message'] = output
      return resultDict
    
    if "no record" in output:
      resultDict['Status'] = 0
      resultDict["Waiting"] = waitingJobs
      resultDict["Running"] = runningJobs
      return resultDict
  
    if len( output ):
      lines = output.split( '\n' )
      for line in lines:
        if not line.strip():
          continue
        if " I " in line:
          waitingJobs += 1
        elif " R " in line:
          runningJobs += 1  
  
    # Final output
    resultDict['Status'] = 0
    resultDict["Waiting"] = waitingJobs
    resultDict["Running"] = runningJobs
    return resultDict
  