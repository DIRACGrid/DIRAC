########################################################################
# $HeadURL$
# File :   ARCComputingElement.py
# Author : A.T.
########################################################################

""" ARC Computing Element 
"""

__RCSID__ = "58c42fc (2013-07-07 22:54:57 +0200) Andrei Tsaregorodtsev <atsareg@in2p3.fr>"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Grid                           import executeGridCommand
from DIRAC                                               import S_OK, S_ERROR

import os, re, tempfile
from types import StringTypes

CE_NAME = 'ARC'
MANDATORY_PARAMETERS = [ 'Queue' ]

class ARCComputingElement( ComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    ComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.submittedJobs = 0
    self.mandatoryParameters = MANDATORY_PARAMETERS
    self.pilotProxy = ''
    self.queue = ''
    self.outputURL = 'gsiftp://localhost'
    self.gridEnv = ''
    self.ceHost = self.ceName
    if 'Host' in self.ceParameters:
      self.ceHost = self.ceParameters['Host']
    if 'GridEnv' in self.ceParameters:
      self.gridEnv = self.ceParameters['GridEnv']

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )

  def __writeXRSL( self, executableFile ):
    """ Create the JDL for submission
    """

    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, name = tempfile.mkstemp( suffix = '.xrsl', prefix = 'ARC_', dir = workingDirectory )
    diracStamp = os.path.basename( name ).replace( '.xrsl', '' ).replace( 'ARC_', '' )
    xrslFile = os.fdopen( fd, 'w' )

    xrsl = """
&(executable="%(executable)s")
(inputFiles=(%(executable)s "%(executableFile)s"))
(stdout="%(diracStamp)s.out")
(stderr="%(diracStamp)s.err")
(outputFiles=("%(diracStamp)s.out" "") ("%(diracStamp)s.err" ""))
    """ % {
            'executableFile':executableFile,
            'executable':os.path.basename( executableFile ),
            'diracStamp':diracStamp
           }

    xrslFile.write( xrsl )
    xrslFile.close()
    return name, diracStamp

  def _reset( self ):
    self.queue = self.ceParameters['Queue']
    self.gridEnv = self.ceParameters['GridEnv']

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job
    """

    self.log.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, 0755 )

    batchIDList = []
    stampDict = {}

    i = 0
    while i < numberOfJobs:
      i += 1
      xrslName, diracStamp = self.__writeXRSL( executableFile )
      cmd = ['arcsub', '-j', self.ceParameters['JobListFile'],
             '-c', '%s' % self.ceHost, '%s' % xrslName ]
      result = executeGridCommand( self.proxy, cmd, self.gridEnv )
      os.unlink( xrslName )
      if not result['OK']:
        break
      if result['Value'][0] != 0:
        break
      pilotJobReference = result['Value'][1].strip()
      if pilotJobReference and pilotJobReference.startswith('Job submitted with jobid:'):
        pilotJobReference = pilotJobReference.replace('Job submitted with jobid:', '').strip()
        batchIDList.append( pilotJobReference )
        stampDict[pilotJobReference] = diracStamp
      else:
        break    

    #os.unlink( executableFile )
    if batchIDList:
      result = S_OK( batchIDList )
      result['PilotStampDict'] = stampDict
    else:
      result = S_ERROR('No pilot references obtained from the glite job submission')  
    return result

  def killJob( self, jobIDList ):
    """ Kill the specified jobs
    """
    
    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, name = tempfile.mkstemp( suffix = '.list', prefix = 'KillJobs_', dir = workingDirectory )
    jobListFile = os.fdopen( fd, 'w' )
    
    jobList = list( jobIDList )
    if type( jobIDList ) in StringTypes:
      jobList = [ jobIDList ]
    for job in jobList:
      jobListFile.write( job+'\n' )  
      
    cmd = ['arckill', '-c', self.ceHost, '-i', name]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    os.unlink( name )
    if not result['OK']:
      return result
    if result['Value'][0] != 0:
      return S_ERROR( 'Failed kill job: %s' % result['Value'][0][1] )   
      
    return S_OK()

#############################################################################
  def getCEStatus( self ):
    """ Method to return information on running and pending jobs.
    """
    cmd = ['arcstat', '-c', self.ceHost, '-j', self.ceParameters['JobListFile'] ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    resultDict = {}
    if not result['OK']:
      return result

    if result['Value'][0] == 1 and result['Value'][1] == "No jobs\n":
      result = S_OK()
      result['RunningJobs'] = 0
      result['WaitingJobs'] = 0
      result['SubmittedJobs'] = 0
      return result

    if result['Value'][0]:
      if result['Value'][2]:
        return S_ERROR(result['Value'][2])
      else:
        return S_ERROR('Error while interrogating CE status')
    if result['Value'][1]:
      resultDict = self.__parseJobStatus( result['Value'][1] )

    running = 0
    waiting = 0
    for ref in resultDict:
      status = resultDict[ref]
      if status == 'Scheduled':
        waiting += 1
      if status == 'Running':
        running += 1

    result = S_OK()
    result['RunningJobs'] = running
    result['WaitingJobs'] = waiting
    result['SubmittedJobs'] = 0
    return result

  def __parseJobStatus( self, commandOutput ):
    """ 
    """
    resultDict = {}
    lines = commandOutput.split('\n')
    ln = 0
    while ln < len( lines ):
      if lines[ln].startswith( 'Job:' ):
        jobRef = lines[ln].split()[1]
        ln += 1
        line = lines[ln].strip()
        stateARC = ''
        if line.startswith( 'State' ):
          result = re.match( 'State: \w+ \(([\w|:]+)\)', line )
          if result:
            stateARC = result.groups()[0]
          line = lines[ln+1].strip()
          exitCode = None 
          if line.startswith( 'Exit Code' ):
            line = line.replace( 'Exit Code:','' ).strip()
            exitCode = int( line )
          
          # Evaluate state now
          if stateARC in ['ACCEPTING', 'ACCEPTED', 'PREPARING', 'PREPARED', 'SUBMITTING',
                          'INLRMS:Q', 'INLRMS:S', 'INLRMS:O']:
            resultDict[jobRef] = "Scheduled"
          elif stateARC in ['INLRMS:R', 'INLRMS:E', 'EXECUTED', 'FINISHING']:
            resultDict[jobRef] = "Running"
          elif stateARC in ['KILLING', 'KILLED']:
            resultDict[jobRef] = "Killed"
          elif stateARC in ['FINISHED']:
            if exitCode is not None:
              if exitCode == 0:
                resultDict[jobRef] = "Done" 
              else:
                resultDict[jobRef] = "Failed"
            else:
              resultDict[jobRef] = "Failed"
          elif stateARC in ['FAILED']:
            resultDict[jobRef] = "Failed"
      elif lines[ln].startswith( "WARNING: Job information not found:" ):
        jobRef = lines[ln].replace( 'WARNING: Job information not found:', '' ).strip()
        resultDict[jobRef] = "Scheduled"
      ln += 1
          
    return resultDict                       

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """

    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, name = tempfile.mkstemp( suffix = '.list', prefix = 'StatJobs_', dir = workingDirectory )
    jobListFile = os.fdopen( fd, 'w' )
    
    jobTmpList = list( jobIDList )
    if type( jobIDList ) in StringTypes:
      jobTmpList = [ jobIDList ]


    jobList = []
    for j in jobTmpList:
      if ":::" in j:
        job = j.split(":::")[0] 
      else:
        job = j
      jobList.append( job )
      jobListFile.write( job+'\n' )  
      
    cmd = ['arcstat', '-c', self.ceHost, '-i', name, '-j', self.ceParameters['JobListFile']]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    os.unlink( name )
    
    resultDict = {}
    if not result['OK']:
      self.log.error( 'Failed to get job status', result['Message'] )
      return result
    if result['Value'][0]:
      if result['Value'][2]:
        return S_ERROR(result['Value'][2])
      else:
        return S_ERROR('Error while interrogating job statuses')
    if result['Value'][1]:
      resultDict = self.__parseJobStatus( result['Value'][1] )
     
    if not resultDict:
      return  S_ERROR('No job statuses returned')

    # If CE does not know about a job, set the status to Unknown
    for job in jobList:
      if not resultDict.has_key( job ):
        resultDict[job] = 'Unknown'
    return S_OK( resultDict )

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    if jobID.find( ':::' ) != -1:
      pilotRef, stamp = jobID.split( ':::' )
    else:
      pilotRef = jobID
      stamp = ''
    if not stamp:
      return S_ERROR( 'Pilot stamp not defined for %s' % pilotRef )

    arcID = os.path.basename(pilotRef)
    if "WorkingDirectory" in self.ceParameters:    
      workingDirectory = os.path.join( self.ceParameters['WorkingDirectory'], arcID )
    else:
      workingDirectory = arcID  
    outFileName = os.path.join( workingDirectory, '%s.out' % stamp )
    errFileName = os.path.join( workingDirectory, '%s.err' % stamp )

    cmd = ['arcget', '-j', self.ceParameters['JobListFile'], pilotRef ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    output = ''
    if result['OK']:
      if not result['Value'][0]:
        outFile = open( outFileName, 'r' )
        output = outFile.read()
        outFile.close()
        os.unlink( outFileName )
        errFile = open( errFileName, 'r' )
        error = errFile.read()
        errFile.close()
        os.unlink( errFileName )
      else:
        error = '\n'.join( result['Value'][1:] )
        return S_ERROR( error )  
    else:
      return S_ERROR( 'Failed to retrieve output for %s' % jobID )

    return S_OK( ( output, error ) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
