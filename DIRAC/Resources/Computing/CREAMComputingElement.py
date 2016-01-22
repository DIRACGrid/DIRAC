########################################################################
# File :   CREAMComputingElement.py
# Author : A.T.
########################################################################

""" CREAM Computing Element 
"""

__RCSID__ = "$Id$"

import os
import re
import tempfile
import stat
from types import StringTypes

from DIRAC                                               import S_OK, S_ERROR

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Grid                           import executeGridCommand
from DIRAC.Core.Utilities.File                           import makeGuid


CE_NAME = 'CREAM'
MANDATORY_PARAMETERS = [ 'Queue' ]

class CREAMComputingElement( ComputingElement ):

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
    self.proxyRenewal = 0

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )

  def __writeJDL( self, executableFile ):
    """ Create the JDL for submission
    """

    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, name = tempfile.mkstemp( suffix = '.jdl', prefix = 'CREAM_', dir = workingDirectory )
    diracStamp = os.path.basename( name ).replace( '.jdl', '' ).replace( 'CREAM_', '' )
    jdlFile = os.fdopen( fd, 'w' )

    jdl = """
[
  JobType = "Normal";
  Executable = "%(executable)s";
  StdOutput="%(diracStamp)s.out";
  StdError="%(diracStamp)s.err";
  InputSandbox={"%(executableFile)s"};
  OutputSandbox={"%(diracStamp)s.out", "%(diracStamp)s.err"};
  OutputSandboxBaseDestUri="%(outputURL)s";
]
    """ % {
            'executableFile':executableFile,
            'executable':os.path.basename( executableFile ),
            'outputURL':self.outputURL,
            'diracStamp':diracStamp
           }

    jdlFile.write( jdl )
    jdlFile.close()
    return name, diracStamp

  def _reset( self ):
    self.queue = self.ceParameters['Queue']
    self.outputURL = self.ceParameters.get( 'OutputURL', 'gsiftp://localhost' )
    self.gridEnv = self.ceParameters['GridEnv']

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job
    """

    self.log.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH )

    batchIDList = []
    stampDict = {}
    if numberOfJobs == 1:
      jdlName, diracStamp = self.__writeJDL( executableFile )
      cmd = ['glite-ce-job-submit', '-n', '-a', '-N', '-r',
             '%s/%s' % ( self.ceName, self.queue ),
             '%s' % jdlName ]
      result = executeGridCommand( self.proxy, cmd, self.gridEnv )
      os.unlink( jdlName )
      if result['OK']:
        if result['Value'][0]:
          # We have got a non-zero status code
          return S_ERROR( 'Pilot submission failed with error: %s ' % result['Value'][2].strip() )
        pilotJobReference = result['Value'][1].strip()
        if not pilotJobReference:
          return S_ERROR( 'No pilot reference returned from the glite job submission command' )
        if not pilotJobReference.startswith( 'https' ):
          return S_ERROR( 'Invalid pilot reference %s' % pilotJobReference )
        batchIDList.append( pilotJobReference )
        stampDict[pilotJobReference] = diracStamp
    else:
      delegationID = makeGuid()
      cmd = [ 'glite-ce-delegate-proxy', '-e', '%s' % self.ceName, '%s' % delegationID ]
      result = executeGridCommand( self.proxy, cmd, self.gridEnv )
      if not result['OK']:
        self.log.error( 'Failed to delegate proxy', result['Message'] )
        return result
      for _i in range( numberOfJobs ):
        jdlName, diracStamp = self.__writeJDL( executableFile )
        cmd = ['glite-ce-job-submit', '-n', '-N', '-r',
               '%s/%s' % ( self.ceName, self.queue ),
               '-D', '%s' % delegationID, '%s' % jdlName ]
        result = executeGridCommand( self.proxy, cmd, self.gridEnv )
        os.unlink( jdlName )
        if not result['OK']:
          break
        if result['Value'][0] != 0:
          break
        pilotJobReference = result['Value'][1].strip()
        if pilotJobReference and pilotJobReference.startswith( 'https' ):
          batchIDList.append( pilotJobReference )
          stampDict[pilotJobReference] = diracStamp
        else:
          break
    if batchIDList:
      result = S_OK( batchIDList )
      result['PilotStampDict'] = stampDict
    else:
      result = S_ERROR( 'No pilot references obtained from the glite job submission' )
    return result

  def killJob( self, jobIDList ):
    """ Kill the specified jobs
    """
    jobList = list( jobIDList )
    if type( jobIDList ) in StringTypes:
      jobList = [ jobIDList ]

    cmd = ['glite-ce-job-cancel', '-n', '-N'] + jobList
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    if not result['OK']:
      return result
    if result['Value'][0] != 0:
      return S_ERROR( 'Failed kill job: %s' % result['Value'][0][1] )

    return S_OK()

#############################################################################
  def getCEStatus( self, jobIDList = None ):
    """ Method to return information on running and pending jobs.
    
        :param list jobIDList: list of job IDs to be considered
    """
    statusList = ['REGISTERED', 'PENDING', 'IDLE', 'RUNNING', 'REALLY-RUNNING']
    cmd = ['glite-ce-job-status', '-n', '-a', '-e',
           '%s' % self.ceName, '-s',
           '%s' % ':'.join( statusList ) ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    resultDict = {}
    if not result['OK']:
      return result
    if result['Value'][0]:
      if result['Value'][0] == 11:
        return S_ERROR( 'Segmentation fault while calling glite-ce-job-status' )
      elif result['Value'][2]:
        return S_ERROR( result['Value'][2] )
      elif "Authorization error" in result['Value'][1]:
        return S_ERROR( "Authorization error" )
      elif "FaultString" in result['Value'][1]:
        res = re.search( 'FaultString=\[([\w\s]+)\]', result['Value'][1] )
        fault = ''
        if res:
          fault = res.group( 1 )
        detail = ''
        res = re.search( 'FaultDetail=\[([\w\s]+)\]', result['Value'][1] )  
        if res:
          detail = res.group( 1 )
          return S_ERROR( "Error: %s:%s" % (fault,detail) )
      else:
        return S_ERROR( 'Error while interrogating CE status' )
    if result['Value'][1]:
      resultDict = self.__parseJobStatus( result['Value'][1] )

    running = 0
    waiting = 0
    statusDict = {}
    for ref, status in resultDict.items():
      if jobIDList is not None and not ref in jobIDList:
        continue
      if status == 'Scheduled':
        waiting += 1
      if status == 'Running':
        running += 1
      statusDict[ref] = status  

    result = S_OK()
    result['RunningJobs'] = running
    result['WaitingJobs'] = waiting
    result['SubmittedJobs'] = 0
    result['JobStatusDict'] = statusDict
    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """
    if self.proxyRenewal % 60 == 0:
      self.proxyRenewal += 1
      statusList = ['REGISTERED', 'PENDING', 'IDLE', 'RUNNING', 'REALLY-RUNNING']
      cmd = ['glite-ce-job-status', '-L', '2', '--all', '-e',
             '%s' % self.ceName, '-s',
             '%s' % ':'.join( statusList ) ]
      result = executeGridCommand( self.proxy, cmd, self.gridEnv )
      if result['OK']:
        delegationIDs = []
        for line in result['Value'][1].split( '\n' ):
          if line.find( 'Deleg Proxy ID' ) != -1:
            delegationID = line.split()[-1].replace( '[', '' ).replace( ']', '' )
            if delegationID not in delegationIDs:
              delegationIDs.append( delegationID )
        if delegationIDs:
          cmd = ['glite-ce-proxy-renew', '-e', self.ceName ]
          cmd.extend( delegationIDs )
          self.log.info( 'Refreshing proxy for:', ' '.join( delegationIDs ) )
          result = executeGridCommand( self.proxy, cmd, self.gridEnv )
          if result['OK']:
            status, output, error = result['Value']
            if status:
              self.log.error( "Failed to renew proxy delegation",
                              'Output:\n' + output + '\nError:\n' + error )

    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, idFileName = tempfile.mkstemp( suffix = '.ids', prefix = 'CREAM_', dir = workingDirectory )
    idFile = os.fdopen( fd, 'w' )
    idFile.write( '##CREAMJOBS##' )
    for id_ in jobIDList:
      if ":::" in id_:
        ref, _stamp = id_.split( ':::' )
      else:
        ref = id_
      idFile.write( '\n' + ref )
    idFile.close()

    cmd = ['glite-ce-job-status', '-n', '-i', '%s' % idFileName ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    os.unlink( idFileName )
    resultDict = {}
    if not result['OK']:
      self.log.error( 'Failed to get job status', result['Message'] )
      return result
    if result['Value'][0]:
      if result['Value'][2]:
        return S_ERROR( result['Value'][2] )
      else:
        return S_ERROR( 'Error while interrogating job statuses' )
    if result['Value'][1]:
      resultDict = self.__parseJobStatus( result['Value'][1] )

    if not resultDict:
      return  S_ERROR( 'No job statuses returned' )

    # If CE does not know about a job, set the status to Unknown
    for job in jobIDList:
      if not resultDict.has_key( job ):
        resultDict[job] = 'Unknown'

    return S_OK( resultDict )

  def __parseJobStatus( self, output ):
    """ Parse the output of the glite-ce-job-status
    """
    resultDict = {}
    ref = ''
    for line in output.split( '\n' ):
      if not line:
        continue
      match = re.search( 'JobID=\[(.*)\]', line )
      if match and len( match.groups() ) == 1:
        ref = match.group( 1 )
      match = re.search( 'Status.*\[(.*)\]', line )
      if match and len( match.groups() ) == 1:
        creamStatus = match.group( 1 )
        if creamStatus in ['DONE-OK']:
          resultDict[ref] = 'Done'
        elif creamStatus in ['DONE-FAILED']:
          resultDict[ref] = 'Failed'
        elif creamStatus in ['REGISTERED', 'PENDING', 'IDLE']:
          resultDict[ref] = 'Scheduled'
        elif creamStatus in ['ABORTED']:
          resultDict[ref] = 'Aborted'
        elif creamStatus in ['CANCELLED']:
          resultDict[ref] = 'Killed'
        elif creamStatus in ['RUNNING', 'REALLY-RUNNING']:
          resultDict[ref] = 'Running'
        elif creamStatus == 'N/A':
          resultDict[ref] = 'Unknown'
        else:
          resultDict[ref] = creamStatus.capitalize()

    return resultDict

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

    outURL = self.ceParameters.get( 'OutputURL', 'gsiftp://localhost' )
    if outURL == 'gsiftp://localhost':
      result = self.__resolveOutputURL( pilotRef )
      if not result['OK']:
        return result
      outURL = result['Value']

    outputURL = os.path.join( outURL, '%s.out' % stamp )
    errorURL = os.path.join( outURL, '%s.err' % stamp )
    workingDirectory = self.ceParameters['WorkingDirectory']
    outFileName = os.path.join( workingDirectory, os.path.basename( outputURL ) )
    errFileName = os.path.join( workingDirectory, os.path.basename( errorURL ) )

    cmd = ['globus-url-copy', '%s' % outputURL, 'file://%s' % outFileName ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    output = ''
    if result['OK']:
      if not result['Value'][0]:
        outFile = open( outFileName, 'r' )
        output = outFile.read()
        outFile.close()
        os.unlink( outFileName )
      elif result['Value'][0] == 1 and "No such file or directory" in result['Value'][2]:
        output = "Standard Output is not available on the CREAM service"
        if os.path.exists( outFileName ):
          os.unlink( outFileName )
      else:
        error = '\n'.join( result['Value'][1:] )
        return S_ERROR( error )
    else:
      return S_ERROR( 'Failed to retrieve output for %s' % jobID )

    cmd = ['globus-url-copy', '%s' % errorURL, '%s' % errFileName ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    error = ''
    if result['OK']:
      if not result['Value'][0]:
        errFile = open( errFileName, 'r' )
        error = errFile.read()
        errFile.close()
        os.unlink( errFileName )
    elif result['Value'][0] == 1 and "No such file or directory" in result['Value'][2]:
        error = "Standard Error is not available on the CREAM service"
        if os.path.exists( errFileName ):
          os.unlink( errFileName )
    else:
      return S_ERROR( 'Failed to retrieve error for %s' % jobID )

    return S_OK( ( output, error ) )

  def __resolveOutputURL( self, pilotRef ):
    """ Resolve the URL of the pilot output files
    """

    cmd = [ 'glite-ce-job-status', '-L', '2', '%s' % pilotRef,
            '| grep -i osb' ]
    result = executeGridCommand( self.proxy, cmd, self.gridEnv )
    url = ''
    if result['OK']:
      if not result['Value'][0]:
        output = result['Value'][1]
        for line in output.split( '\n' ):
          line = line.strip()
          if line.find( 'OSB' ) != -1:
            match = re.search( '\[(.*)\]', line )
            if match:
              url = match.group( 1 )
      if url:
        return S_OK( url )
      else:
        return S_ERROR( 'output URL not found for %s' % pilotRef )
    else:
      return S_ERROR( 'Failed to retrieve long status for %s' % pilotRef )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

