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
    
    status,stdout_q = commands.getstatusoutput( 'condor_q -submitter %s' % user )
    
    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = stdout_q
      return resultDict
      
    status_history, stdout_history = commands.getstatusoutput( 'condor_history | grep %s' % user )  
    
    stdout = stdout_q
    if status_history == 0:
      stdout = '\n'.join( [stdout_q,stdout_history] )
  
    statusDict = {}
    if len( stdout ):
      lines = stdout.split( '\n' )
      for line in lines:
        l = line.strip()
        for job in jobIDList:
          if l.startswith( job ):
            if " I " in line:
              statusDict[job] = 'Waiting'
            elif " R " in line:
              statusDict[job] = 'Running'
            elif " C " in line:
              statusDict[job] = 'Done'
            elif " X " in line:
              statusDict[job] = 'Aborted'    
  
    if len( statusDict ) != len( jobIDList ):
      for job in jobIDList:
        if not job in statusDict:
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
  