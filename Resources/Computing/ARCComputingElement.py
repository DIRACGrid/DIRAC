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

from DIRAC                                               import S_OK, S_ERROR

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Grid                           import executeGridCommand

import arc # Has to work if this module is called
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
    # Used in getJobStatus
    self.mapStates = { 'ACCEPTED' : 'Scheduled',
                       'PREPARING' : 'Scheduled',
                       'SUBMITTING' : 'Scheduled',
                       'QUEUING' : 'Scheduled',
                       'HOLD' : 'Scheduled',
                       'UNDEFINED' : 'Unknown',
                       'RUNNING' : 'Running',
                       'FINISHING' : 'Running',
                       'DELETED' : 'Killed',
                       'KILLED' : 'Killed',
                       'FAILED' : 'Failed',
                       'FINISHED' : 'Done',
                       'OTHER' : 'Done'
      }

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
    if 'GridEnv' in self.ceParameters:
      self.gridEnv = self.ceParameters['GridEnv']

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job
    """

    self.log.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH )

    batchIDList = []
    stampDict = {}

    # Do not need to loop over these ... the pilot proxy is the same for everyone
    usercfg = arc.UserConfig()
    usercfg.CredentialString(proxy)
    endpoint = arc.Endpoint(cfgPath("gsiftp://", self.ceHost, ":2811/jobs"), arc.Endpoint.JOBSUBMIT,
                            "org.nordugrid.gridftpjob")

    i = 0
    while i < numberOfJobs:
      i += 1
      # The basic job description
      job = arc.Job()
      jobdescs = arc.JobDescriptionList()
      # Get the job into the ARC way
      xrslName, diracStamp = self.__writeXRSL( executableFile )
      if not arc.JobDescription_Parse(xrslName, jobdescs):
        logger.msg(arc.ERROR, "Invalid job description")
        sys.exit(1) 
      # Submit the job
      jobs = arc.JobList() # filled by the submit process
      submitter = arc.Submitter(usercfg)
      result = submitter.Submit(endpoint, jobdescs, jobs)
      # Remove clutter
      os.unlink( xrslName )
      # Save info or else ...
      if ( result == arc.SubmissionStatus.NONE ) :
        # Job successfully submitted
        pilotJobReference = jobs[0].JobID
        batchIDList.append( pilotJobReference )
        stampDict[pilotJobReference] = diracStamp
      else :
        break

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

    usercfg = arc.UserConfig()
    usercfg.CredentialString(self.proxy)

    endpoints = [arc.Endpoint( cfgPath("ldap://", self.ceHost, "/MDS-Vo-name=local,o=grid"),
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

    usercfg = arc.UserConfig()
    usercfg.CredentialString(self.proxy)
    resultDict = {}
    for job in jobList:
      job = arc.Job()
      job.JobID = job
      job.JobStatusURL = arc.URL(cfgPath("ldap://", self.ceHost, ":2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=", job.JobID, ")"))
      job.JobManagementURL = arc.URL(cfgPath("gsiftp://", self.ceHost, ":2811/jobs/"))
      job.JobManagementInterfaceName = "org.nordugrid.gridftpjob"
      job.PrepareHandler(usercfg)
      job.Update()
      arcState = job.State.GetSpecificState()
      resultDict[job] = self.mapStates[arcState]
      # If done - is it really done? Check the exit code
      if (self.mapStates[arcState] == "Done") :
        exitCode = job.ExitCode
        if ( exitCode != 0 ) :
          resultDict[job] == "Failed"
      
    if not resultDict:
      return  S_ERROR('No job statuses returned')

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

    usercfg = arc.UserConfig()
    usercfg.CredentialString(self.proxy)
    job = arc.Job()
    job.jobID = os.path.basename(pilotRef)
    job.JobStatusURL = arc.URL(cfgPath("ldap://", self.ceHost, ":2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=", job.JobID, ")"))
    job.JobManagementURL = arc.URL(cfgPath("gsiftp://", self.ceHost, ":2811/jobs/"))
    job.JobManagementInterfaceName = "org.nordugrid.gridftpjob"

    # arcID = os.path.basename(pilotRef)
    if "WorkingDirectory" in self.ceParameters:    
      workingDirectory = os.path.join( self.ceParameters['WorkingDirectory'], arcID )
    else:
      workingDirectory = arcID  
    outFileName = os.path.join( workingDirectory, '%s.out' % stamp )
    errFileName = os.path.join( workingDirectory, '%s.err' % stamp )

    job.Retrieve(usercfg, arc.URL("./"), False) 
    #cmd = ['arcget', '-j', self.ceParameters['JobListFile'], pilotRef ]
    #result = executeGridCommand( self.proxy, cmd, self.gridEnv )
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
