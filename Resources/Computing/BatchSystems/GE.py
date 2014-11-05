############################################################################
#
#  SGEBatch class representing SGE batch system
#  Author: A.T.
#
############################################################################

""" The script relies on the SubmitOptions parameter to choose the right queue.
    This should be specified in the Queue description in the CS. e.g.
    
    SubmitOption = -l ct=6000
"""

import re, commands, os

MANDATORY_PARAMETERS = [ 'Executable', 'OutputDir', 'ErrorDir', 'NJobs', 'SubmitOptions' ]

class GE( object ):

  def submitJob( self, **kwargs ):
    """ Submit nJobs to the condor batch system
    """
    resultDict = {}
    
    for argument in MANDATORY_PARAMETERS:
      if not argument in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict   
      
    nJobs = kwargs.get( 'NJobs', 1 )  
    
    outputs = []
    output = ''
    for _i in range( int(nJobs) ):
      cmd = "qsub -o %(OutputDir)s -e %(ErrorDir)s -N DIRACPilot %(SubmitOptions)s %(Executable)s" % kwargs
      status,output = commands.getstatusoutput(cmd)
      if status == 0:
        outputs.append(output)
      else:
        break                                                         
  
    if outputs:
      resultDict['Status'] = 0
      resultDict['Jobs'] = []
      for output in outputs:
        match = re.match('Your job (\d*) ',output)
        if match:
          resultDict['Jobs'].append( match.groups()[0] )
    else:
      resultDict['Status'] = status
      resultDict['Message'] = output
      
    return resultDict
  
  def killJob( self, **kwargs ):
    """ Kill jobs in the given list
    """
    
    resultDict = {}
    
    jobIDList = kwargs.get( 'JobIDList' )
    if not jobIDList:
      resultDict['Status'] = -1
      resultDict['Message'] = 'Empty job list'
      return resultDict
    
    result = 0
    successful = []
    failed = []
    for job in jobIDList:
      status,output = commands.getstatusoutput( 'qdel %s' % job )
      if status != 0:
        result += 1
        failed.append( job )
      else:
        successful.append( job )  
    
    resultDict['Successful'] = successful
    resultDict['Failed'] = failed
    return resultDict
    
  def getJobStatus( self, **kwargs ):
    """ Get status of the jobs in the given list
    """
    resultDict = {}
    
    user = kwargs.get( 'User' )
    if not user:
      user = os.environ.get( 'USER' )
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict
    jobIDList = kwargs.get( 'JobIDList' )
    if not jobIDList:
      resultDict['Status'] = -1
      resultDict['Message'] = 'Empty job list'
      return resultDict
    
    status,output = commands.getstatusoutput( 'qstat -u %s' % user )
    
    if status != 0:
      resultDict['Status'] = status
      resultDict['Output'] = output
      return resultDict
      
    jobDict = {}
    if len( output ):
      lines = output.split( '\n' )
      for line in lines:
        l = line.strip()
        for job in jobIDList:
          if l.startswith( job ):
            jobStatus = l.split()[4]
            if jobStatus in ['Tt', 'Tr']:
              jobDict[job] = 'Done'
            elif jobStatus in ['Rr', 'r']:
              jobDict[job] = 'Running'
            elif jobStatus in ['qw', 'h']:
              jobDict[job] = 'Waiting'
  
    status,output = commands.getstatusoutput( 'qstat -u %s -s z' % user )
    
    if status == 0:    
      if len( output ):
        lines = output.split( '\n' )
        for line in lines:
          l = line.strip()
          for job in jobIDList:
            if l.startswith( job ):
              jobDict[job] = 'Done'
  
    if len( resultDict ) != len( jobIDList ):
      for job in jobIDList:
        if not job in jobDict:
          jobDict[job] = 'Unknown'
          
    # Final output
    status = 0
    resultDict['Status'] = 0
    resultDict['Jobs'] = jobDict 
    return resultDict    
    
  def getCEStatus( self, **kwargs ):
    """ Get the overall CE status
    """
    resultDict = {}
    
    user = kwargs.get( 'User' )
    if not user:
      user = os.environ.get( 'USER' )
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict

    cmd = 'qstat -u %s' % user
    status,output = commands.getstatusoutput( cmd )

    if status != 0:
      resultDict['Status'] = status
      resultDict['Output'] = output
      return resultDict

    waitingJobs = 0
    runningJobs = 0

    if len( output ):
      lines = output.split( '\n' )
      for line in lines:
        if not line.strip():
          continue
        if 'DIRACPilot %s' % user in line:
          jobStatus = line.split()[4]
          if jobStatus in ['Tt', 'Tr']:
            doneJobs = 'Done'
          elif jobStatus in ['Rr', 'r']:
            runningJobs = runningJobs + 1
          elif jobStatus in ['qw', 'h']:
            waitingJobs = waitingJobs + 1

    # Final output
    resultDict['Status'] = 0
    resultDict["Waiting"] = waitingJobs
    resultDict["Running"] = runningJobs
    return resultDict
