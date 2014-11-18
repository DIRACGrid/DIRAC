#########################################################################################
# $HeadURL$
# OAR.py
# 10.11.2014
# Author: Matvey Sapunov, A.T.
#########################################################################################

""" OAR.py is a DIRAC independent class representing OAR batch system.
    OAR objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes 
"""

__RCSID__ = "$Id$"

import commands, os, json

class OAR( object ):

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
    jobIDs = []
    for _i in range( nJobs ):
      cmd = "oarsub -O %s.out -E %s.err -q %s -n DIRACPilot %s %s" % ( outFile,
                                                                       errFile,
                                                                       queue,
                                                                       submitOptions,
                                                                       executable )
      status, output = commands.getstatusoutput(cmd)
  
      if status != 0 or not output:
        break
  
      lines = output.split( '\n' )
      if lines[ -1 ].find( "[JOB START]" ) >= 0:
        _prefix , jid = lines[ -1 ].split( ":" )
  
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
      status, output = commands.getstatusoutput( 'oardel %s' % job )
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
  
    status, output = commands.getstatusoutput( "oarstat --sql \"project = '%s'\" -J" % user )
    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = output
      return resultDict
  
    try:
      output = json.loads( output )
    except Exception , x:
      resultDict['Status'] = 2048
      resultDict['Message'] = str( x )
      return resultDict
  
    if not len( output ) > 0:
      resultDict['Status'] = 1024
      resultDict['Message'] = output
      return resultDict
  
    statusDict = {}
    for job in jobIDList:
  
      if not job in output:
        statusDict[ job ] = "Unknown"
        continue
  
      if not "state" in output[ job ]:
        statusDict[ job ] = "Unknown"
        continue
      state = output[ job ][ "state" ]
  
      if state in [ "Running", "Finishing" ]:
        statusDict[ job ] = "Running"
        continue
  
      if state in [ "Error", "toError" ]:
        statusDict[ job ] = "Aborted"
        continue
  
      if state in [ "Waiting", "Hold", "toAckReservation", "Suspended", "toLaunch", "Launching" ]:
        statusDict[ job ] = "Waiting"
        continue
  
      if state == "Terminated":
        statusDict[ job ] = "Done"
        continue
  
      statusDict[ job ] = "Unknown"
      continue
  
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
  
    status , output = commands.getstatusoutput( 'oarstat -u %s -J' % user )
    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = output
      return resultDict
  
    try:
      output = json.loads( output )
    except Exception , x:
      resultDict['Status'] = 2048
      resultDict['Message'] = str( x )
      return resultDict
  
    if output > 0:
      resultDict['Status'] = 0
      resultDict["Waiting"] = waitingJobs
      resultDict["Running"] = runningJobs
      return resultDict
  
    for value in output.values():
  
      if not "state" in value:
        continue
      state = value[ "state" ]
  
      if state in [ "Running", "Finishing" ]:
        runningJobs += 1
        continue
  
      if state in [ "Waiting", "Hold", "toAckReservation", "Suspended", "toLaunch", "Launching" ]:
        waitingJobs += 1
        continue
  
    # Final output
    resultDict['Status'] = 0
    resultDict["Waiting"] = waitingJobs
    resultDict["Running"] = runningJobs
    return resultDict
