#########################################################################
#
#  $HeadURL$
#  Host.py
#  4.11.2014
#  Author: A.T.
#
#########################################################################
          
""" Host - class for managing jobs on a host, locally or through an SSH tunnel
"""                    
          
__RCSID__ = "$Id$"          
          
import commands, os, glob, shutil          
                                  
class Host( object ):
  
  def submitJob( self, **kwargs ):
    
    resultDict = {}
    
    args = dict( kwargs )
    
    MANDATORY_PARAMETERS = [ 'Executable', 'OutputDir', 'ErrorDir', 
                             'InfoDir', 'ExecutionContext', 'JobStamps' ]         
    
    for argument in MANDATORY_PARAMETERS:
      if not argument in args:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict   
      
    nJobs = args.get( 'NJobs', 1 )  
    stamps = args['JobStamps']
    context = args.get( 'ExecutionContext', 'Local' )
    jobidName = context.upper + '_JOBID'
    
    jobs = []
    output = ''
    for _i in range( int(nJobs) ):
      args['Stamp'] = stamps[_i]
      cmd = "export %s=%s; " % ( jobidName, stamps[_i] ) 
      cmd += "export %(Executable)s 1>%(OutputDir)s/%(Stamp)s.out 2>%(ErrorDir)s/%(Stamp)s.err &; " % args
      cmd += "echo $! > %(InfoDir)s/%(Stamp)s.pid; " % args
      status,output = commands.getstatusoutput(cmd)
      if status == 0:
        jobs.append( stamps[_i] )
      else:
        break                                                         
  
    if jobs:
      resultDict['Status'] = 0
      resultDict['Jobs'] = jobs
    else:
      resultDict['Status'] = status
      resultDict['Message'] = output
      
    return resultDict
  
  def __cleanJob( self, stamp, infoDir, workDir, outputDir = None, errorDir = None ):
    
    jobDir = os.path.join( workDir, stamp )
    if os.path.isdir( jobDir ):
      shutil.rmtree( jobDir )
    pidFile = os.path.join( infoDir, '%s.pid' % stamp )  
    if os.path.isfile( pidFile ):  
      os.unlink( pidFile )
    if outputDir:
      outFile = os.path.join( outputDir, '%s.out' % stamp )  
      if os.path.isfile( outFile ):
        os.unlink( outFile )
    if errorDir:
      errFile = os.path.join( errorDir, '%s.err' % stamp )  
      if os.path.isfile( errFile ):
        os.unlink( errFile )      
  
  def __getPid( self, infoDir, stamp ):
    
    pidFileName = os.path.join( infoDir, '%s.pid' % stamp )
    pidFile = open( pidFileName, 'r' )
    pid = pidFile.read().strip()
    return pid
  
  def getCEStatus( self, **kwargs ):
    
    """ Get the overall CE status
    """
    resultDict = { 'Running': 0, 'Waiting': 0 }
    
    MANDATORY_PARAMETERS = [ 'InfoDir', 'WorkDir', 'User' ]         
    
    for argument in MANDATORY_PARAMETERS:
      if not argument in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict   
    
    user = kwargs.get( 'User' )
    infoDir = kwargs.get( 'InfoDir' )
    workDir = kwargs.get( 'WorkDir' )
    
    running = 0
    pidFiles = glob.glob( '%s/*.pid' % infoDir )
    for pidFileName in pidFiles:
      pidFile = open( pidFileName, 'r' )
      pid = pidFile.read().strip()
      pidFile.close()
      cmd = 'ps -f -p %s | grep %s | wc -l' % ( pid, user )
      status,output = commands.getstatusoutput( cmd )
      if status == 0:
        if output.strip() == '1':
          running += 1
        else:
          stamp = os.path.basename( pidFileName ).replace( '.pid', '' )  
          self.__cleanJob( stamp, infoDir, workDir )    
      else:
        resultDict['Status'] = status
        return resultDict    
        
    resultDict['Status'] = 0
    resultDict['Running'] = running    
    return resultDict  

  def getJobStatus( self, **kwargs ):
    
    resultDict = {}
    
    MANDATORY_PARAMETERS = [ 'InfoDir', 'JobStamps', 'User' ]         
    
    for argument in MANDATORY_PARAMETERS:
      if not argument in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict   
      
    infoDir = kwargs.get( 'InfoDir' )
    jobStamps = kwargs.get( 'JobStamps' )  
    for stamp in jobStamps:
      pid = self.__getPid( infoDir, stamp )
      status, output = command( 'ps -p %s | grep wc -l' % pid )
        