########################################################################
# File :   ARCComputingElement.py
# Author : A.T.
# Update to use ARC API : Raja Nandakumar
########################################################################

""" ARC Computing Element 
    Using the ARC API now
"""

__RCSID__ = "58c42fc (2013-07-07 22:54:57 +0200) Andrei Tsaregorodtsev <atsareg@in2p3.fr>"

import os
import stat
import tempfile # Need it for the Dirac stamp
from types import StringTypes

import arc # Has to work if this module is called
from DIRAC                                               import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo, getVOfromProxyGroup
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import theARCJob
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.WorkloadManagementSystem.private.ConfigHelper import findGenericPilotCredentials
from DIRAC.Core.Utilities.SiteCEMapping                  import getSiteForCE

# Uncomment the following 5 lines for getting verbose ARC api output (debugging)
# import sys
# logstdout = arc.LogStream(sys.stdout)
# logstdout.setFormat(arc.ShortFormat)
# arc.Logger_getRootLogger().addDestination(logstdout)
# arc.Logger_getRootLogger().setThreshold(arc.VERBOSE)

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
    self.__getXRSLExtraString() # Do this after all other initialisations, in case something barks
    
  #############################################################################
  def __getXRSLExtraString( self ):
    # For the XRSL additional string from configuration - only done at initialisation time
    # If this string changes, the corresponding (ARC) site directors have to be restarted
    #
    # Variable = XRSLExtraString
    # Default value = ''
    #   If you give a value, I think it should be of the form
    #          (aaa = "xxx")
    #   Otherwise the ARC job description parser will have a fit
    # Locations searched in order :
    # Top priority    : Resources/Sites/<Grid>/<Site>/CEs/<CE>/XRSLExtraString
    # Second priority : Resources/Sites/<Grid>/<Site>/XRSLExtraString
    # Default         : Resources/Computing/CEDefaults/XRSLExtraString
    #
    self.xrslExtraString = '' # Start with the default value
    result = getSiteForCE(self.ceHost)
    self.site = ''
    if ( result['OK'] ):
      self.site = result['Value']
    else :
      gLogger.error("Unknown Site ...")
      return
    # Now we know the site. Get the grid
    grid = self.site.split(".")[0]
    # The different possibilities that we have agreed upon
    xtraVariable = "XRSLExtraString"
    firstOption = "Resources/Sites/%s/%s/CEs/%s/%s" % (grid, self.site, self.ceHost, xtraVariable)
    secondOption = "Resources/Sites/%s/%s/%s" % (grid, self.site, xtraVariable)
    defaultOption = "Resources/Computing/CEDefaults/%s" % xtraVariable
    # Now go about getting the string in the agreed order
    gLogger.debug("Trying to get xrslExtra string : first option %s" % firstOption)
    result = gConfig.getValue(firstOption, defaultValue='')
    if ( result != '' ):
      self.xrslExtraString = result
      gLogger.debug("Found xrslExtra string : %s" % self.xrslExtraString)
    else:
      gLogger.debug("Trying to get xrslExtra string : second option %s" % secondOption)
      result = gConfig.getValue(secondOption, defaultValue='')
      if ( result != '' ):
        self.xrslExtraString = result
        gLogger.debug("Found xrslExtra string : %s" % self.xrslExtraString)
      else:
        gLogger.debug("Trying to get xrslExtra string : default option %s" % defaultOption)
        result = gConfig.getValue(defaultOption, defaultValue='')
        if ( result != '' ):
          self.xrslExtraString = result
          gLogger.debug("Found xrslExtra string : %s" % self.xrslExtraString)
    if ( self.xrslExtraString == '' ):
      gLogger.always("No XRSLExtra string found in configuration for %s" % self.ceHost)
    else :
      gLogger.always("XRSLExtra string : %s" % self.xrslExtraString)
      gLogger.always(" --- to be added to pilots going to CE : %s" % self.ceHost)

  #############################################################################
  def _addCEConfigDefaults( self ):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    # First assure that any global parameters are loaded
    ComputingElement._addCEConfigDefaults( self )

  #############################################################################
  def _doTheProxyBit( self ):
    """ Set the environment variable X509_USER_PROXY and let the ARC CE pick it up from there.
    Unfortunately I cannot trust the proxies that are floating around here. So, explicitly hunt
    for the pilot proxy which is the only one I care about. Also, try to be safe about finding
    out the VO, given that the self.ceParameters['VO'] is not always filled.
    """
    vo = ''
    try: # First get the VO
      result = getVOfromProxyGroup()
      if result['OK']:
        vo = result['Value']
      else: # A backup solution which may work
        vo = self.ceParameters['VO']
    except:
      gLogger.error("Could not get the VO we are in ...")
    result = findGenericPilotCredentials( vo ) # Second find out who is the pilot for this vo
    if not result[ 'OK' ]:
      os.environ['X509_USER_PROXY'] = ''
      gLogger.error("Could not set proxy correctly. You (or maybe I) should worry about this.")
      return
    self.pilotDN, self.pilotGroup = result[ 'Value' ]
    # Third - get the actual proxy into a temp file and set the environment to point to this file.
    result = gProxyManager.getPilotProxyFromDIRACGroup( self.pilotDN, self.pilotGroup )
    if not result[ 'OK' ]:
      os.environ['X509_USER_PROXY'] = ''
      gLogger.error("Why did I crash here? Likely Dirac bug (report please) - or try restarting the siteDirector.")
      return
    self.pilotProxy = result['Value']
    try:
      ret = gProxyManager.dumpProxyToFile( self.pilotProxy )
      os.environ['X509_USER_PROXY'] = ret['Value']
    except AttributeError:
      ret = getProxyInfo()
      os.environ['X509_USER_PROXY'] = ret['Value']['path']
    gLogger.debug("Set proxy variable X509_USER_PROXY to %s" % os.environ['X509_USER_PROXY'])
    
  #############################################################################
  def __writeXRSL( self, executableFile ):
    """ Create the JDL for submission
    """

    workingDirectory = self.ceParameters['WorkingDirectory']
    fd, name = tempfile.mkstemp( suffix = '.xrsl', prefix = 'ARC_', dir = workingDirectory )
    diracStamp = os.path.basename( name ).replace( '.xrsl', '' ).replace( 'ARC_', '' )

    xrsl = """
&(executable="%(executable)s")
(inputFiles=(%(executable)s "%(executableFile)s"))
(stdout="%(diracStamp)s.out")
(stderr="%(diracStamp)s.err")
(outputFiles=("%(diracStamp)s.out" "") ("%(diracStamp)s.err" ""))
%(xrslExtraString)s
    """ % {
            'executableFile':executableFile,
            'executable':os.path.basename( executableFile ),
            'diracStamp':diracStamp,
            'xrslExtraString':self.xrslExtraString
           }

    return xrsl, diracStamp

  #############################################################################
  def _reset( self ):
    self.queue = self.ceParameters['Queue']
    if 'GridEnv' in self.ceParameters:
      self.gridEnv = self.ceParameters['GridEnv']

  #############################################################################
  def submitJob( self, executableFile, proxy, numberOfJobs = 1 ):
    """ Method to submit job
    """

    self._doTheProxyBit()
    gLogger.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH )

    batchIDList = []
    stampDict = {}

    usercfg = arc.UserConfig()
    endpoint = arc.Endpoint( self.ceHost + ":2811/jobs", arc.Endpoint.JOBSUBMIT,
                            "org.nordugrid.gridftpjob")

    # Submit jobs iteratively for now. Tentatively easier than mucking around with the JobSupervisor class
    for __i in range(numberOfJobs):
      # The basic job description
      job = arc.Job()
      jobdescs = arc.JobDescriptionList()
      # Get the job into the ARC way
      xrslString, diracStamp = self.__writeXRSL( executableFile )
      if not arc.JobDescription_Parse(xrslString, jobdescs):
        gLogger.error("Invalid job description")
        break
      # Submit the job
      jobs = arc.JobList() # filled by the submit process
      submitter = arc.Submitter(usercfg)
      result = submitter.Submit(endpoint, jobdescs, jobs)
      # Save info or else ...
      if ( result == arc.SubmissionStatus.NONE ):
        # Job successfully submitted
        pilotJobReference = jobs[0].JobID
        batchIDList.append( pilotJobReference )
        stampDict[pilotJobReference] = diracStamp
        gLogger.debug("Successfully submitted job %s to CE %s" % (pilotJobReference, self.ceHost))
      else:
        message = "Failed to submit job because "
        if (result == arc.SubmissionStatus.NOT_IMPLEMENTED ):
          gLogger.warn( "%s feature not implemented on CE? (weird I know - complain to site admins" % message )
        elif ( result == arc.SubmissionStatus.NO_SERVICES ):
          gLogger.warn( "%s no services are running on CE? (open GGUS ticket to site admins" % message )
        elif ( result == arc.SubmissionStatus.ENDPOINT_NOT_QUERIED ):
          gLogger.warn( "%s endpoint was not even queried. (network ..?)" % message )
        elif ( result == arc.SubmissionStatus.BROKER_PLUGIN_NOT_LOADED ):
          gLogger.warn( "%s BROKER_PLUGIN_NOT_LOADED : ARC library installation problem?" % message )
        elif ( result == arc.SubmissionStatus.DESCRIPTION_NOT_SUBMITTED ):
          gLogger.warn( "%s no job description was there (Should not happen, but horses can fly (in a plane))" % message )
        elif ( result == arc.SubmissionStatus.SUBMITTER_PLUGIN_NOT_LOADED ):
          gLogger.warn( "%s SUBMITTER_PLUGIN_NOT_LOADED : ARC library installation problem?" % message )
        elif ( result == arc.SubmissionStatus.AUTHENTICATION_ERROR ):
          gLogger.warn( "%s authentication error - screwed up / expired proxy? Renew / upload pilot proxy on machine?" % message )
        elif ( result == arc.SubmissionStatus.ERROR_FROM_ENDPOINT ):
          gLogger.warn( "%s some error from the CE - ask site admins for more information ..." % message )
        else:
          gLogger.warn( "%s I do not know why. Check everything." % message )
        break # Boo hoo *sniff*

    if batchIDList:
      result = S_OK( batchIDList )
      result['PilotStampDict'] = stampDict
    else:
      result = S_ERROR('No pilot references obtained from the ARC job submission')
    return result

  #############################################################################
  def killJob( self, jobIDList ):
    """ Kill the specified jobs
    """
    
    self._doTheProxyBit()
    usercfg = arc.UserConfig()
    js = arc.compute.JobSupervisor(usercfg)

    jobList = list( jobIDList )
    if type( jobIDList ) in StringTypes:
      jobList = [ jobIDList ]

    for jobID in jobList:
      job = theARCJob(self.ceHost, jobID)
      js.AddJob(job)

    result = js.Cancel() # Cancel all jobs at once

    if not result:
      gLogger.debug("Failed to kill jobs %s. CE(?) not reachable?" % jobIDList)
      return S_ERROR( 'Failed to kill the job(s)' )
    else:
      gLogger.debug("Killed jobs %s" % jobIDList)

      
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
    retriever.wait() # Takes a bit of time to get and parse the ldap information
    targets = retriever.GetExecutionTargets()
    ceStats = targets[0].ComputingShare
    gLogger.debug("Running jobs for CE %s : %s" % (self.ceHost, ceStats.RunningJobs))
    gLogger.debug("Waiting jobs for CE %s : %s" % (self.ceHost, ceStats.WaitingJobs))

    result = S_OK()
    result['RunningJobs'] = ceStats.RunningJobs
    result['WaitingJobs'] = ceStats.WaitingJobs
    result['SubmittedJobs'] = 0
    return result

  #############################################################################
  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """

    self._doTheProxyBit()
    workingDirectory = self.ceParameters['WorkingDirectory']
    #fd, name = tempfile.mkstemp( suffix = '.list', prefix = 'StatJobs_', dir = workingDirectory )
    #jobListFile = os.fdopen( fd, 'w' )

    jobTmpList = list( jobIDList )
    if type( jobIDList ) in StringTypes:
      jobTmpList = [ jobIDList ]

    # Pilots are stored with a DIRAC stamp (":::XXXXX") appended
    jobList = []
    for j in jobTmpList:
      if ":::" in j:
        job = j.split(":::")[0] 
      else:
        job = j
      jobList.append( job )
      #jobListFile.write( job+'\n' )  

    resultDict = {}
    for jobID in jobList:
      gLogger.debug("Retrieving status for job %s" % jobID)
      job = theARCJob(self.ceHost, jobID)
      job.Update()
      arcState = job.State.GetGeneralState()
      gLogger.debug("ARC status for job %s is %s" % (jobID, arcState))
      if ( arcState ): # Meaning arcState is filled. Is this good python?
        resultDict[jobID] = self.mapStates[arcState]
      else:
        resultDict[jobID] = 'Unknown'
      # If done - is it really done? Check the exit code
      if (resultDict[jobID] == "Done"):
        exitCode = jobID.ExitCode
        if ( exitCode != 0 ):
          resultDict[jobID] == "Failed"
      gLogger.debug("DIRAC status for job %s is %s" % (jobID, resultDict[jobID]))

    if not resultDict:
      return  S_ERROR('No job statuses returned')

    return S_OK( resultDict )

  #############################################################################
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
    gLogger.debug("Retrieving pilot logs for %s" % pilotRef)
    if "WorkingDirectory" in self.ceParameters:
      workingDirectory = os.path.join( self.ceParameters['WorkingDirectory'], arcID )
    else:
      workingDirectory = arcID  
    outFileName = os.path.join( workingDirectory, '%s.out' % stamp )
    errFileName = os.path.join( workingDirectory, '%s.err' % stamp )
    gLogger.debug("Working directory for pilot output %s" % workingDirectory)

    usercfg = arc.UserConfig()
    isItOkay = job.Retrieve(usercfg, arc.URL(workingDirectory), False) 
    output = ''
    error = ''
    if ( isItOkay ):
      outFile = open( outFileName, 'r' )
      output = outFile.read()
      outFile.close()
      os.unlink( outFileName )
      errFile = open( errFileName, 'r' )
      error = errFile.read()
      errFile.close()
      os.unlink( errFileName )
      gLogger.debug("Pilot output = %s" % output)
      gLogger.debug("Pilot error = %s" % error)
    else:
      job.Update()
      arcState = job.State.GetGeneralState()
      if (arcState != "Undefined"):
        return S_ERROR( 'Failed to retrieve output for %s as job is not finished (maybe not started yet)' % jobID )
      gLogger.debug("Could not retrieve pilot output for %s - either permission / proxy error or could not connect to CE" % pilotRef)
      return S_ERROR( 'Failed to retrieve output for %s' % jobID )

    return S_OK( ( output, error ) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
