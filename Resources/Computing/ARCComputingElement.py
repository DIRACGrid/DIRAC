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

import arc # Has to work if this module is called
from DIRAC                                               import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.SiteCEMapping                  import getSiteForCE
from DIRAC.Core.Utilities.File                           import makeGuid
from DIRAC.Core.Security.ProxyInfo                       import getVOfromProxyGroup

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
    self.usercfg = arc.common.UserConfig()
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

  def __getARCJob( self, jobID ):
    """ Create an ARC Job with all the needed / possible parameters defined.
        By the time we come here, the environment variable X509_USER_PROXY should already be set
    """
    j = arc.Job()
    j.JobID = jobID
    statURL = "ldap://%s:2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=%s)" % ( self.ceHost, jobID )
    j.JobStatusURL = arc.URL( statURL )
    j.JobStatusInterfaceName = "org.nordugrid.ldapng"
    mangURL = "gsiftp://%s:2811/jobs/" % ( self.ceHost )
    j.JobManagementURL = arc.URL( mangURL )
    j.JobManagementInterfaceName = "org.nordugrid.gridftpjob"
    j.ServiceInformationURL = j.JobManagementURL
    j.ServiceInformationInterfaceName = "org.nordugrid.ldapng"
    j.PrepareHandler( self.usercfg )
    return j

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
  def __writeXRSL( self, executableFile ):
    """ Create the JDL for submission
    """
    diracStamp = makeGuid()[:8]

    xrsl = """
&(executable="%(executable)s")
(inputFiles=(%(executable)s "%(executableFile)s"))
(stdout="%(diracStamp)s.out")
(stderr="%(diracStamp)s.err")
(outputFiles=("%(diracStamp)s.out" "") ("%(diracStamp)s.err" ""))
(queue=%(queue)s)
%(xrslExtraString)s
    """ % {
            'executableFile':executableFile,
            'executable':os.path.basename( executableFile ),
            'diracStamp':diracStamp,
            'queue':self.arcQueue,
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

    # Assume that the ARC queues are always of the format nordugrid-<batchSystem>-<queue>
    # And none of our supported batch systems have a "-" in their name
    self.arcQueue = self.queue.split("-",2)[2]
    result = self._prepareProxy()
    self.usercfg.ProxyPath(os.environ['X509_USER_PROXY'])
    if not result['OK']:
      gLogger.error( 'ARCComputingElement: failed to set up proxy', result['Message'] )
      return result

    gLogger.verbose( "Executable file path: %s" % executableFile )
    if not os.access( executableFile, 5 ):
      os.chmod( executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH )

    batchIDList = []
    stampDict = {}

    endpoint = arc.Endpoint( self.ceHost + ":2811/jobs", arc.Endpoint.JOBSUBMIT,
                            "org.nordugrid.gridftpjob")

    # Submit jobs iteratively for now. Tentatively easier than mucking around with the JobSupervisor class
    for __i in range(numberOfJobs):
      # The basic job description
      jobdescs = arc.JobDescriptionList()
      # Get the job into the ARC way
      xrslString, diracStamp = self.__writeXRSL( executableFile )
      gLogger.debug("XRSL string submitted : %s" %xrslString)
      gLogger.debug("DIRAC stamp for job : %s" %diracStamp)
      if not arc.JobDescription_Parse(xrslString, jobdescs):
        gLogger.error("Invalid job description")
        break
      # Submit the job
      jobs = arc.JobList() # filled by the submit process
      submitter = arc.Submitter(self.usercfg)
      result = submitter.Submit(endpoint, jobdescs, jobs)
      # Save info or else ..else.
      if ( result == arc.SubmissionStatus.NONE ):
        # Job successfully submitted
        pilotJobReference = jobs[0].JobID
        batchIDList.append( pilotJobReference )
        stampDict[pilotJobReference] = diracStamp
        gLogger.debug("Successfully submitted job %s to CE %s" % (pilotJobReference, self.ceHost))
      else:
        message = "Failed to submit job because "
        if (result.isSet(arc.SubmissionStatus.NOT_IMPLEMENTED) ): #pylint: disable=E1101
          gLogger.warn( "%s feature not implemented on CE? (weird I know - complain to site admins" % message )
        if ( result.isSet(arc.SubmissionStatus.NO_SERVICES) ): #pylint: disable=E1101
          gLogger.warn( "%s no services are running on CE? (open GGUS ticket to site admins" % message )
        if ( result.isSet(arc.SubmissionStatus.ENDPOINT_NOT_QUERIED) ): #pylint: disable=E1101
          gLogger.warn( "%s endpoint was not even queried. (network ..?)" % message )
        if ( result.isSet(arc.SubmissionStatus.BROKER_PLUGIN_NOT_LOADED) ): #pylint: disable=E1101
          gLogger.warn( "%s BROKER_PLUGIN_NOT_LOADED : ARC library installation problem?" % message )
        if ( result.isSet(arc.SubmissionStatus.DESCRIPTION_NOT_SUBMITTED) ): #pylint: disable=E1101
          gLogger.warn( "%s Job not submitted - incorrect job description? (missing field in XRSL string?)" % message )
        if ( result.isSet(arc.SubmissionStatus.SUBMITTER_PLUGIN_NOT_LOADED) ): #pylint: disable=E1101
          gLogger.warn( "%s SUBMITTER_PLUGIN_NOT_LOADED : ARC library installation problem?" % message )
        if ( result.isSet(arc.SubmissionStatus.AUTHENTICATION_ERROR) ): #pylint: disable=E1101
          gLogger.warn( "%s authentication error - screwed up / expired proxy? Renew / upload pilot proxy on machine?" % message )
        if ( result.isSet(arc.SubmissionStatus.ERROR_FROM_ENDPOINT) ): #pylint: disable=E1101
          gLogger.warn( "%s some error from the CE - possibly CE problems?" % message )
        gLogger.warn( "%s ... maybe above messages will give a hint." % message )
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

    result = self._prepareProxy()
    self.usercfg.ProxyPath(os.environ['X509_USER_PROXY'])
    if not result['OK']:
      gLogger.error( 'ARCComputingElement: failed to set up proxy', result['Message'] )
      return result

    js = arc.compute.JobSupervisor(self.usercfg)

    jobList = list( jobIDList )
    if isinstance( jobIDList, basestring ):
      jobList = [ jobIDList ]

    for jobID in jobList:
      job = self.__getARCJob( jobID )
      js.AddJob( job )

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
        We hope to satisfy both instances that use robot proxies and those which use proper configurations.
    """

    result = self._prepareProxy()
    self.usercfg.ProxyPath(os.environ['X509_USER_PROXY'])
    if not result['OK']:
      gLogger.error( 'ARCComputingElement: failed to set up proxy', result['Message'] )
      return result

    # Try to find out which VO we are running for.
    vo = ''
    res = getVOfromProxyGroup()
    if res['OK']:
      vo = res['Value']

    result = S_OK()
    result['SubmittedJobs'] = 0
    if not vo:
      # Presumably the really proper way forward once the infosys-discuss WG comes up with a solution
      # and it is implemented. Needed for DIRAC instances which use robot certificates for pilots.
      endpoints = [arc.Endpoint( "ldap://" + self.ceHost + "/MDS-Vo-name=local,o=grid",
                               arc.Endpoint.COMPUTINGINFO, 'org.nordugrid.ldapng')]
      retriever = arc.ComputingServiceRetriever(self.usercfg, endpoints)
      retriever.wait() # Takes a bit of time to get and parse the ldap information
      targets = retriever.GetExecutionTargets()
      ceStats = targets[0].ComputingShare
      gLogger.debug("Running jobs for CE %s : %s" % (self.ceHost, ceStats.RunningJobs))
      gLogger.debug("Waiting jobs for CE %s : %s" % (self.ceHost, ceStats.WaitingJobs))
      result['RunningJobs'] = ceStats.RunningJobs
      result['WaitingJobs'] = ceStats.WaitingJobs
    else:
      # The system which works properly at present for ARC CEs that are configured correctly.
      # But for this we need the VO to be known - ask me (Raja) for the whole story if interested.
      cmd = 'ldapsearch -x -LLL -H ldap://%s:2135 -b mds-vo-name=resource,o=grid "(GlueVOViewLocalID=%s)"' %(self.ceHost, vo.lower())
      res = shellCall( 0, cmd )
      if not res['OK']:
        gLogger.debug("Could not query CE %s - is it down?" % self.ceHost)
        return res
      try:
        ldapValues = res['Value'][1].split("\n")
        running = [lValue for lValue in ldapValues if 'GlueCEStateRunningJobs' in lValue]
        waiting = [lValue for lValue in ldapValues if 'GlueCEStateWaitingJobs' in lValue]
        result['RunningJobs'] = int(running[0].split(":")[1])
        result['WaitingJobs'] = int(waiting[0].split(":")[1])
      except IndexError:
        res = S_ERROR('Unknown ldap failure for site %s' % self.ceHost)
        return res

    return result

  #############################################################################
  def getJobStatus( self, jobIDList ):
    """ Get the status information for the given list of jobs
    """

    result = self._prepareProxy()
    self.usercfg.ProxyPath(os.environ['X509_USER_PROXY'])
    if not result['OK']:
      gLogger.error( 'ARCComputingElement: failed to set up proxy', result['Message'] )
      return result

    jobTmpList = list( jobIDList )
    if isinstance( jobIDList, basestring ):
      jobTmpList = [ jobIDList ]

    # Pilots are stored with a DIRAC stamp (":::XXXXX") appended
    jobList = []
    for j in jobTmpList:
      if ":::" in j:
        job = j.split(":::")[0]
      else:
        job = j
      jobList.append( job )

    resultDict = {}
    for jobID in jobList:
      gLogger.debug("Retrieving status for job %s" % jobID)
      job = self.__getARCJob( jobID )
      job.Update()
      arcState = job.State.GetGeneralState()
      gLogger.debug("ARC status for job %s is %s" % (jobID, arcState))
      if arcState: # Meaning arcState is filled. Is this good python?
        resultDict[jobID] = self.mapStates[arcState]
        # Renew proxy only of jobs which are running - and will expire within the next hour
        if self.mapStates[arcState] == "Running":
          nextHour = arc.Time()+arc.Period(10000) # 2 hours, 46 minutes and 40 seconds
          if job.ProxyExpirationTime < nextHour:
            job.Renew()
            gLogger.debug("Renewing proxy for job %s whose proxy expires at %s" % (jobID, job.ProxyExpirationTime))
      else:
        resultDict[jobID] = 'Unknown'
      # If done - is it really done? Check the exit code
      if (resultDict[jobID] == "Done"):
        exitCode = int( job.ExitCode )
        if exitCode:
          resultDict[jobID] = "Failed"
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
    result = self._prepareProxy()
    self.usercfg.ProxyPath(os.environ['X509_USER_PROXY'])
    if not result['OK']:
      gLogger.error( 'ARCComputingElement: failed to set up proxy', result['Message'] )
      return result

    if jobID.find( ':::' ) != -1:
      pilotRef, stamp = jobID.split( ':::' )
    else:
      pilotRef = jobID
      stamp = ''
    if not stamp:
      return S_ERROR( 'Pilot stamp not defined for %s' % pilotRef )

    job = self.__getARCJob( pilotRef )

    arcID = os.path.basename(pilotRef)
    gLogger.debug("Retrieving pilot logs for %s" % pilotRef)
    if "WorkingDirectory" in self.ceParameters:
      workingDirectory = os.path.join( self.ceParameters['WorkingDirectory'], arcID )
    else:
      workingDirectory = arcID
    outFileName = os.path.join( workingDirectory, '%s.out' % stamp )
    errFileName = os.path.join( workingDirectory, '%s.err' % stamp )
    gLogger.debug("Working directory for pilot output %s" % workingDirectory)

    isItOkay = job.Retrieve( self.usercfg, arc.URL( workingDirectory ), False )
    if isItOkay:
      output = None
      error  = None
      try:
        with open(outFileName, 'r') as outFile:
          output = outFile.read()
        os.unlink( outFileName )
        with open(errFileName, 'r') as errFile:
          error = errFile.read()
        os.unlink( errFileName )
      except IOError as e:
        return S_ERROR("Error downloading outputs", repr(e))
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
