########################################################################
# File :   ARCComputingElement.py
# Author : A.T.
########################################################################

""" ARC Computing Element 
    Using the ARC API now
"""

__RCSID__ = "58c42fc (2013-07-07 22:54:57 +0200) Andrei Tsaregorodtsev <atsareg@in2p3.fr>"

import os
import stat
import tempfile
from types import StringTypes

import arc # Has to work if this module is called
from DIRAC                                               import S_OK, S_ERROR
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Grid                           import executeGridCommand
from DIRAC.ConfigurationSystem.Client.Helpers.Path       import cfgPath
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import theARCJob
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.Core.Utilities.DictCache                      import DictCache
from DIRAC.WorkloadManagementSystem.private.ConfigHelper import findGenericPilotCredentials

import sys
logstdout = arc.LogStream(sys.stdout)
logstdout.setFormat(arc.ShortFormat)
arc.Logger_getRootLogger().addDestination(logstdout)
arc.Logger_getRootLogger().setThreshold(arc.VERBOSE)

CE_NAME = 'ARC'
MANDATORY_PARAMETERS = [ 'Queue' ] #Probably not mandatory for ARC CEs

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
    # Used in getJobStatus
    self.mapStates = { 'Accepted'   : 'Scheduled',
                       'Preparing'  : 'Scheduled',
                       'Submitting' : 'Scheduled',
                       'Queuing'    : 'Scheduled',
                       'Hold'       : 'Scheduled',
                       'Undefined'  : 'Unknown',
                       'Running'    : 'Running',
                       'Finishing'  : 'Running',
                       'Deleted' : 'Killed',
                       'Killed'  : 'Killed',
                       'Failed'  : 'Failed',
                       'Finished': 'Done',
                       'Other'   : 'Done'
      }

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )

  def _doTheProxyBit( self ):
    """ Set the environment variable X509_USER_PROXY and let the ARC CE pick it up from there.
    """
    result = findGenericPilotCredentials( vo = self.ceParameters['VO'] )
    if not result[ 'OK' ]:
      os.environ['X509_USER_PROXY'] = ''
      return
    self.pilotDN, self.pilotGroup = result[ 'Value' ]
    self.pilotProxy = gProxyManager.getPilotProxyFromDIRACGroup( self.pilotDN, self.pilotGroup )['Value']
    try :
      ret = gProxyManager.dumpProxyToFile( self.pilotProxy )
      os.environ['X509_USER_PROXY'] = ret['Value']
    except AttributeError :
      ret = getProxyInfo()
      os.environ['X509_USER_PROXY'] = ret['Value']['path']
    
  def __writeXRSL( self, executableFile ):
    """ Create the JDL for submission
    """

    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, name = tempfile.mkstemp( suffix = '.xrsl', prefix = 'ARC_', dir = workingDirectory )
    diracStamp = os.path.basename( name ).replace( '.xrsl', '' ).replace( 'ARC_', '' )
    #xrslFile = os.fdopen( fd, 'w' )

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

    #xrslFile.write( xrsl.strip() )
    #xrslFile.close()
    #return name, diracStamp
    return xrsl, diracStamp

  def _reset( self ):
    self.queue = self.ceParameters['Queue']
    if 'GridEnv' in self.ceParameters:
      self.gridEnv = self.ceParameters['GridEnv']

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job
    """

    self._doTheProxyBit()
    self.log.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH )

    batchIDList = []
    stampDict = {}

    usercfg = arc.UserConfig()
    endpoint = arc.Endpoint( self.ceHost + ":2811/jobs", arc.Endpoint.JOBSUBMIT,
                            "org.nordugrid.gridftpjob")

    i = 0
    for __i in range(numberOfJobs):
#    while i < numberOfJobs:
#      i += 1
      # The basic job description
      job = arc.Job()
      jobdescs = arc.JobDescriptionList()
      # Get the job into the ARC way
      # xrslName, diracStamp = self.__writeXRSL( executableFile )
      xrslString, diracStamp = self.__writeXRSL( executableFile )
      # if not arc.JobDescription_ParseFromFile(xrslName, jobdescs):
      if not arc.JobDescription_Parse(xrslString, jobdescs):
        self.log.error("Invalid job description")
        break
      # Submit the job
      jobs = arc.JobList() # filled by the submit process
      submitter = arc.Submitter(usercfg)
      result = submitter.Submit(endpoint, jobdescs, jobs)
      # Remove clutter
      # os.unlink( xrslName )
      # Save info or else ...
      if ( result == arc.SubmissionStatus.NONE ):
        # Job successfully submitted
        pilotJobReference = jobs[0].JobID
        batchIDList.append( pilotJobReference )
        stampDict[pilotJobReference] = diracStamp
      else:
        break

    if batchIDList:
      result = S_OK( batchIDList )
      result['PilotStampDict'] = stampDict
    else:
      result = S_ERROR('No pilot references obtained from the ARC job submission')
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

    self._doTheProxyBit()
    usercfg = arc.UserConfig()

    endpoints = [arc.Endpoint( "ldap://" + self.ceHost + "/MDS-Vo-name=local,o=grid",
                               arc.Endpoint.COMPUTINGINFO, 'org.nordugrid.ldapng')]
    retriever = arc.ComputingServiceRetriever(usercfg, endpoints)
    retriever.wait()
    targets = retriever.GetExecutionTargets()
    ceStats = targets[0].ComputingShare

    result = S_OK()
    result['RunningJobs'] = ceStats.RunningJobs
    result['WaitingJobs'] = ceStats.WaitingJobs
    result['SubmittedJobs'] = 0
    return result

  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """

    self._doTheProxyBit()
    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, name = tempfile.mkstemp( suffix = '.list', prefix = 'StatJobs_', dir = workingDirectory )
    jobListFile = os.fdopen( fd, 'w' )
    
    jobTmpList = list( jobIDList )
    if type( jobIDList ) in StringTypes:
      jobTmpList = [ jobIDList ]

    # Not sure if this is needed but assume it is ....
    jobList = []
    for j in jobTmpList:
      if ":::" in j:
        job = j.split(":::")[0] 
      else:
        job = j
      jobList.append( job )
      jobListFile.write( job+'\n' )  

    resultDict = {}
    for jobID in jobList:
      job = theARCJob(self.ceHost, jobID)
      job.Update()
      arcState = job.State.GetGeneralState()
      if ( arcState ): # Meaning arcState is filled. Is this good python?
        resultDict[jobID] = self.mapStates[arcState]
      else:
        resultDict[jobID] = 'Unknown'
      # If done - is it really done? Check the exit code
      if (resultDict[jobID] == "Done"):
        exitCode = jobID.ExitCode
        if ( exitCode != 0 ):
          resultDict[jobID] == "Failed"

    if not resultDict:
      return  S_ERROR('No job statuses returned')

    return S_OK( resultDict )

  def getJobOutput( self, jobID, localDir = None ):
    """ Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned 
        as strings. 
    """
    self._doTheProxyBit()
    if jobID.find( ':::' ) != -1:
      pilotRef, stamp = jobID.split( ':::' )
    else:
      pilotRef = jobID
      stamp = ''
    if not stamp:
      return S_ERROR( 'Pilot stamp not defined for %s' % pilotRef )

    job = theARCJob(self.ceHost, pilotRef)

    arcID = os.path.basename(pilotRef)
    if "WorkingDirectory" in self.ceParameters:
      workingDirectory = os.path.join( self.ceParameters['WorkingDirectory'], arcID )
    else:
      workingDirectory = arcID  
    outFileName = os.path.join( workingDirectory, '%s.out' % stamp )
    errFileName = os.path.join( workingDirectory, '%s.err' % stamp )

    usercfg = arc.UserConfig()
    isItOkay = job.Retrieve(usercfg, arc.URL(workingDirectory), False) 
    output = ''
    if ( isItOkay ):
      outFile = open( outFileName, 'r' )
      output = outFile.read()
      outFile.close()
      os.unlink( outFileName )
      errFile = open( errFileName, 'r' )
      error = errFile.read()
      errFile.close()
      os.unlink( errFileName )
    else:
      return S_ERROR( 'Failed to retrieve output for %s' % jobID )

    return S_OK( ( output, error ) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
