""" Encapsulate here the logic for matching jobs

    Utilities and classes here are used by MatcherHandler
"""

__RCSID__ = "$Id"

import time
from types import StringTypes

from DIRAC import gLogger, gMonitor

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client.Limiter import Limiter

from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB, singleValueDefFields, multiValueMatchFields
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB


class Matcher( object ):
  """ Logic for matching
  """

  def __init__( self, pilotAgentsDB = None, jobDB = None, tqDB = None, jlDB = None, opsHelper = None ):
    """ c'tor
    """
    if pilotAgentsDB:
      self.pilotAgentsDB = pilotAgentsDB
    else:
      self.pilotAgentsDB = PilotAgentsDB()
    if jobDB:
      self.jobDB = jobDB
    else:
      self.jobDB = JobDB()
    if tqDB:
      self.tqDB = tqDB
    else:
      self.tqDB = TaskQueueDB()
    if jlDB:
      self.jlDB = jlDB
    else:
      self.jlDB = JobLoggingDB()

    if opsHelper:
      self.opsHelper = opsHelper
    else:
      self.opsHelper = Operations()

    self.log = gLogger.getSubLogger( "Matcher" )

    self.limiter = Limiter( jobDB = self.jobDB, opsHelper = self.opsHelper )


  def selectJob( self, resourceDescription, credDict ):
    """ Main job selection function to find the highest priority job matching the resource capacity
    """

    startTime = time.time()

    resourceDict = self._getResourceDict( resourceDescription, credDict )

    negativeCond = self.limiter.getNegativeCondForSite( resourceDict['Site'] )
    result = self.tqDB.matchAndGetJob( resourceDict, negativeCond = negativeCond )

    if not result['OK']:
      return result
    result = result['Value']
    if not result['matchFound']:
      self.log.info( "No match found" )
      raise RuntimeError( "No match found" )

    jobID = result['jobId']
    resAtt = self.jobDB.getJobAttributes( jobID, ['OwnerDN', 'OwnerGroup', 'Status'] )
    if not resAtt['OK']:
      raise RuntimeError( 'Could not retrieve job attributes' )
    if not resAtt['Value']:
      raise RuntimeError( "No attributes returned for job" )
    if not resAtt['Value']['Status'] == 'Waiting':
      self.log.error( 'Job matched by the TQ is not in Waiting state', str( jobID ) )
      result = self.tqDB.deleteJob( jobID )
      if not result[ 'OK' ]:
        return result
      raise RuntimeError( "Job %s is not in Waiting state" % str( jobID ) )

    self._reportStatus( resourceDict, jobID )

    result = self.jobDB.getJobJDL( jobID )
    if not result['OK']:
      raise RuntimeError( "Failed to get the job JDL" )

    resultDict = {}
    resultDict['JDL'] = result['Value']
    resultDict['JobID'] = jobID

    matchTime = time.time() - startTime
    self.log.info( "Match time: [%s]" % str( matchTime ) )
    gMonitor.addMark( "matchTime", matchTime )

    # Get some extra stuff into the response returned
    resOpt = self.jobDB.getJobOptParameters( jobID )
    if resOpt['OK']:
      for key, value in resOpt['Value'].items():
        resultDict[key] = value
    resAtt = self.jobDB.getJobAttributes( jobID, ['OwnerDN', 'OwnerGroup'] )
    if not resAtt['OK']:
      raise RuntimeError( 'Could not retrieve job attributes' )
    if not resAtt['Value']:
      raise RuntimeError( 'No attributes returned for job' )

    if self.opsHelper.getValue( "JobScheduling/CheckMatchingDelay", True ):
      self.limiter.updateDelayCounters( resourceDict['Site'], jobID )

    pilotInfoReportedFlag = resourceDict.get( 'PilotInfoReportedFlag', False )
    if not pilotInfoReportedFlag:
      self._updatePilotInfo( resourceDict )
    self._updatePilotJobMapping( resourceDict, jobID )

    resultDict['DN'] = resAtt['Value']['OwnerDN']
    resultDict['Group'] = resAtt['Value']['OwnerGroup']
    resultDict['PilotInfoReportedFlag'] = True

    return resultDict


  def _getResourceDict( self, resourceDescription, credDict ):
    """ from resourceDescription to resourceDict (just various mods)
    """
    resourceDict = self._processResourceDescription( resourceDescription )
    resourceDict = self._checkCredentials( resourceDict, credDict )
    self._checkPilotVersion( resourceDict )
    if not self._checkMask( resourceDict ):
      # Banned destinations can only take Test jobs
      resourceDict['JobType'] = 'Test'

    self.log.verbose( "Resource description:" )
    for key in resourceDict:
      self.log.verbose( "%s : %s" % ( key.rjust( 20 ), resourceDict[ key ] ) )

    return resourceDict

  def _processResourceDescription( self, resourceDescription ):
    """ Check and form the resource description dictionary

        resourceDescription is a ceDict coming from a JobAgent, for example.
    """

    resourceDict = {}
    if type( resourceDescription ) in StringTypes:
      classAdAgent = ClassAd( resourceDescription )
      if not classAdAgent.isOK():
        raise ValueError( 'Illegal Resource JDL' )
      self.log.verbose( classAdAgent.asJDL() )

      for name in singleValueDefFields:
        if classAdAgent.lookupAttribute( name ):
          if name == 'CPUTime':
            resourceDict[name] = classAdAgent.getAttributeInt( name )
          else:
            resourceDict[name] = classAdAgent.getAttributeString( name )

      for name in multiValueMatchFields:
        if classAdAgent.lookupAttribute( name ):
          if name == 'SubmitPool':
            resourceDict[name] = classAdAgent.getListFromExpression( name )
          else:
            resourceDict[name] = classAdAgent.getAttributeString( name )

      # Check if a JobID is requested
      if classAdAgent.lookupAttribute( 'JobID' ):
        resourceDict['JobID'] = classAdAgent.getAttributeInt( 'JobID' )

      for k in ( 'DIRACVersion', 'ReleaseVersion', 'ReleaseProject', 'VirtualOrganization' ):
        if classAdAgent.lookupAttribute( k ):
          resourceDict[ k ] = classAdAgent.getAttributeString( k )

    else:
      for name in singleValueDefFields:
        if resourceDescription.has_key( name ):
          resourceDict[name] = resourceDescription[name]

      for name in multiValueMatchFields:
        if resourceDescription.has_key( name ):
          resourceDict[name] = resourceDescription[name]

      if resourceDescription.has_key( 'JobID' ):
        resourceDict['JobID'] = resourceDescription['JobID']

      for k in ( 'DIRACVersion', 'ReleaseVersion', 'ReleaseProject', 'VirtualOrganization',
                 'PilotReference', 'PilotBenchmark', 'PilotInfoReportedFlag' ):
        if k in resourceDescription:
          resourceDict[ k ] = resourceDescription[ k ]

    return resourceDict



  def _reportStatus( self, resourceDict, jobID ):
    """ Reports the status of the matched job in jobDB and jobLoggingDB

        Do not fail if errors happen here
    """
    attNames = ['Status', 'MinorStatus', 'ApplicationStatus', 'Site']
    attValues = ['Matched', 'Assigned', 'Unknown', resourceDict['Site']]
    result = self.jobDB.setJobAttributes( jobID, attNames, attValues )
    if not result['OK']:
      self.log.error( "Problem reporting job status", "setJobAttributes, jobID = %s: %s" % ( jobID, result['Message'] ) )
    else:
      self.log.verbose( "Set job attributes for jobID %s" % jobID )

    result = self.jlDB.addLoggingRecord( jobID,
                                         status = 'Matched',
                                         minor = 'Assigned',
                                         source = 'Matcher' )
    if not result['OK']:
      self.log.error( "Problem reporting job status", "addLoggingRecord, jobID = %s: %s" % ( jobID, result['Message'] ) )
    else:
      self.log.verbose( "Added logging record for jobID %s" % jobID )


  def _checkMask( self, resourceDict ):
    """ Check the mask: are we allowed to run normal jobs?

        FIXME: should we move to site OR SE?
    """
    if not 'Site' in resourceDict:
      self.log.error( "Missing Site Name in Resource JDL" )
      raise RuntimeError( "Missing Site Name in Resource JDL" )

    # Get common site mask and check the agent site
    result = self.jobDB.getSiteMask( siteState = 'Active' )
    if not result['OK']:
      self.log.error( "Internal error", "getSiteMask: %s" % result['Message'] )
      raise RuntimeError( "Internal error" )
    maskList = result['Value']

    if resourceDict['Site'] not in maskList:
      return False

    return True

  def _updatePilotInfo( self, resourceDict ):
    """ Update pilot information - do not fail if we don't manage to do it
    """
    pilotReference = resourceDict.get( 'PilotReference', '' )
    if pilotReference:
      gridCE = resourceDict.get( 'GridCE', 'Unknown' )
      site = resourceDict.get( 'Site', 'Unknown' )
      benchmark = resourceDict.get( 'PilotBenchmark', 0.0 )
      self.log.verbose( 'Reporting pilot info for %s: gridCE=%s, site=%s, benchmark=%f' % ( pilotReference, gridCE, site, benchmark ) )

      result = self.pilotAgentsDB.setPilotStatus( pilotReference, status = 'Running', gridSite = site,
                                                  destination = gridCE, benchmark = benchmark )
      if not result['OK']:
        self.log.error( "Problem updating pilot information",
                        "; setPilotStatus. pilotReference: %s; %s" % ( pilotReference, result['Message'] ) )

  def _updatePilotJobMapping( self, resourceDict, jobID ):
    """ Update pilot to job mapping information
    """
    pilotReference = resourceDict.get( 'PilotReference', '' )
    if pilotReference:
      result = self.pilotAgentsDB.setCurrentJobID( pilotReference, jobID )
      if not result['OK']:
        self.log.error( "Problem updating pilot information",
                        ";setCurrentJobID. pilotReference: %s; %s" % ( pilotReference, result['Message'] ) )
      result = self.pilotAgentsDB.setJobForPilot( jobID, pilotReference, updateStatus = False )
      if not result['OK']:
        self.log.error( "Problem updating pilot information",
                        "; setJobForPilot. pilotReference: %s; %s" % ( pilotReference, result['Message'] ) )

  def _checkCredentials( self, resourceDict, credDict ):
    """ Check if we can get a job given the passed credentials
    """
    # Check credentials if not generic pilot
    if Properties.GENERIC_PILOT in credDict[ 'properties' ]:
      # You can only match groups in the same VO
      vo = Registry.getVOForGroup( credDict[ 'group' ] )
      result = Registry.getGroupsForVO( vo )
      if result[ 'OK' ]:
        resourceDict[ 'OwnerGroup' ] = result[ 'Value' ]
      else:
        raise RuntimeError( result['Message'] )
    else:
      # If it's a private pilot, the DN has to be the same
      if Properties.PILOT in credDict[ 'properties' ]:
        self.log.notice( "Setting the resource DN to the credentials DN" )
        resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
      # If it's a job sharing. The group has to be the same and just check that the DN (if any)
      # belongs to the same group
      elif Properties.JOB_SHARING in credDict[ 'properties' ]:
        resourceDict[ 'OwnerGroup' ] = credDict[ 'group' ]
        self.log.notice( "Setting the resource group to the credentials group" )
        if 'OwnerDN'  in resourceDict and resourceDict[ 'OwnerDN' ] != credDict[ 'DN' ]:
          ownerDN = resourceDict[ 'OwnerDN' ]
          result = Registry.getGroupsForDN( resourceDict[ 'OwnerDN' ] )
          if not result[ 'OK' ]:
            raise RuntimeError( result['Message'] )
          if credDict[ 'group' ] not in result[ 'Value' ]:
            # DN is not in the same group! bad boy.
            self.log.notice( "You cannot request jobs from DN %s. It does not belong to your group!" % ownerDN )
            resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
      # Nothing special, group and DN have to be the same
      else:
        resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
        resourceDict[ 'OwnerGroup' ] = credDict[ 'group' ]

    return resourceDict

  def _checkPilotVersion( self, resourceDict ):
    """ Check the pilot DIRAC version
    """
    if self.opsHelper.getValue( "Pilot/CheckVersion", True ):
      if 'ReleaseVersion' not in resourceDict:
        if not 'DIRACVersion' in resourceDict:
          raise RuntimeError( 'Version check requested and not provided by Pilot' )
        else:
          pilotVersion = resourceDict['DIRACVersion']
      else:
        pilotVersion = resourceDict['ReleaseVersion']

      validVersions = self.opsHelper.getValue( "Pilot/Version", [] )
      if validVersions and pilotVersion not in validVersions:
        raise RuntimeError( 'Pilot version does not match the production version %s not in ( %s )' % \
                            ( pilotVersion, ",".join( validVersions ) ) )
      # Check project if requested
      validProject = self.opsHelper.getValue( "Pilot/Project", "" )
      if validProject:
        if 'ReleaseProject' not in resourceDict:
          raise RuntimeError( "Version check requested but expected project %s not received" % validProject )
        if resourceDict[ 'ReleaseProject' ] != validProject:
          raise RuntimeError( "Version check requested but expected project %s != received %s" % ( validProject,
                                                                                                   resourceDict[ 'ReleaseProject' ] ) )
