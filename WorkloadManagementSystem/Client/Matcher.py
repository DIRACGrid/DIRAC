""" Encapsulate here the logic for matching jobs

    Utilities and classes here are used by MatcherHandler
"""

__RCSID__ = "$Id"

import time
from types import StringTypes

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger, gMonitor

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers import Registry, Operations
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import singleValueDefFields, multiValueDefFields
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

class Matcher(object):
  """ Logic for matching
  """

  def __init__( self ):
    """ c'tor
    """
    self.pilotAgentsDB = PilotAgentsDB()
    self.jobDB = JobDB()
    self.tqDB = TaskQueueDB()
    self.jlDB = JobLoggingDB()
    
  # this can be a function
  def _processResourceDescription( self, resourceDescription ):
    """ Check and form the resource description dictionary

        resourceDescription is a ceDict coming from a JobAgent, for example.
    """

    resourceDict = {}
    if type( resourceDescription ) in StringTypes:
      classAdAgent = ClassAd( resourceDescription )
      if not classAdAgent.isOK():
        raise ValueError( 'Illegal Resource JDL' )
      gLogger.verbose( classAdAgent.asJDL() )

      for name in singleValueDefFields:
        if classAdAgent.lookupAttribute( name ):
          if name == 'CPUTime':
            resourceDict[name] = classAdAgent.getAttributeInt( name )
          else:
            resourceDict[name] = classAdAgent.getAttributeString( name )

      for name in multiValueDefFields:
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

      for name in multiValueDefFields:
        if resourceDescription.has_key( name ):
          resourceDict[name] = resourceDescription[name]

      if resourceDescription.has_key( 'JobID' ):
        resourceDict['JobID'] = resourceDescription['JobID']

      for k in ( 'DIRACVersion', 'ReleaseVersion', 'ReleaseProject', 'VirtualOrganization',
                 'PilotReference', 'PilotInfoReportedFlag', 'PilotBenchmark' ):
        if k in resourceDescription:
          resourceDict[ k ] = resourceDescription[ k ]

    return resourceDict

  def _checkCredentials( self, resourceDict ):
    credDict = self.getRemoteCredentials()
    # Check credentials if not generic pilot
    if Properties.GENERIC_PILOT in credDict[ 'properties' ]:
      # You can only match groups in the same VO
      vo = Registry.getVOForGroup( credDict[ 'group' ] )
      result = Registry.getGroupsForVO( vo )
      if result[ 'OK' ]:
        resourceDict[ 'OwnerGroup' ] = result[ 'Value' ]
      else:
        return result
    else:
      # If it's a private pilot, the DN has to be the same
      if Properties.PILOT in credDict[ 'properties' ]:
        gLogger.notice( "Setting the resource DN to the credentials DN" )
        resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
      # If it's a job sharing. The group has to be the same and just check that the DN (if any)
      # belongs to the same group
      elif Properties.JOB_SHARING in credDict[ 'properties' ]:
        resourceDict[ 'OwnerGroup' ] = credDict[ 'group' ]
        gLogger.notice( "Setting the resource group to the credentials group" )
        if 'OwnerDN'  in resourceDict and resourceDict[ 'OwnerDN' ] != credDict[ 'DN' ]:
          ownerDN = resourceDict[ 'OwnerDN' ]
          result = Registry.getGroupsForDN( resourceDict[ 'OwnerDN' ] )
          if not result[ 'OK' ] or credDict[ 'group' ] not in result[ 'Value' ]:
            # DN is not in the same group! bad boy.
            gLogger.notice( "You cannot request jobs from DN %s. It does not belong to your group!" % ownerDN )
            resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
      # Nothing special, group and DN have to be the same
      else:
        resourceDict[ 'OwnerDN' ] = credDict[ 'DN' ]
        resourceDict[ 'OwnerGroup' ] = credDict[ 'group' ]

    return resourceDict

  def _checkPilotVersion( self, resourceDict ):
    # Check the pilot DIRAC version
    if Operations().getValue( "Pilot/CheckVersion", True ):
      if 'ReleaseVersion' not in resourceDict:
        if not 'DIRACVersion' in resourceDict:
          return S_ERROR( 'Version check requested and not provided by Pilot' )
        else:
          pilotVersion = resourceDict['DIRACVersion']
      else:
        pilotVersion = resourceDict['ReleaseVersion']

      validVersions = Operations().getValue( "Pilot/Version", [] )
      if validVersions and pilotVersion not in validVersions:
        return S_ERROR( 'Pilot version does not match the production version %s not in ( %s )' % \
                       ( pilotVersion, ",".join( validVersions ) ) )
      # Check project if requested
      validProject = Operations().getValue( "Pilot/Project", "" )
      if validProject:
        if 'ReleaseProject' not in resourceDict:
          return S_ERROR( "Version check requested but expected project %s not received" % validProject )
        if resourceDict[ 'ReleaseProject' ] != validProject:
          return S_ERROR( "Version check requested but expected project %s != received %s" % ( validProject,
                                                                                               resourceDict[ 'ReleaseProject' ] ) )


  def selectJob( self, resourceDescription ):
    """ Main job selection function to find the highest priority job matching the resource capacity
    """

    startTime = time.time()
    resourceDict = self._processResourceDescription( resourceDescription )

    resourceDict = self._checkCredentials( resourceDict )

    self._checkPilotVersion( resourceDict )

    # Update pilot information
    pilotInfoReported = resourceDict.get( 'PilotInfoReportedFlag', False )
    pilotReference = resourceDict.get( 'PilotReference', '' )
    if pilotReference and not pilotInfoReported:
      gridCE = resourceDict.get( 'GridCE', 'Unknown' )
      site = resourceDict.get( 'Site', 'Unknown' )
      benchmark = resourceDict.get( 'PilotBenchmark', 0.0 )
      gLogger.verbose( 'Reporting pilot info for %s: gridCE=%s, site=%s, benchmark=%f' % ( pilotReference, gridCE, site, benchmark ) )
      result = self.pilotAgentsDB.setPilotStatus( pilotReference, status = 'Running',
                                              gridSite = site,
                                              destination = gridCE,
                                              benchmark = benchmark )
      if result['OK']:
        pilotInfoReported = True

    # Check the site mask
    if not 'Site' in resourceDict:
      return S_ERROR( 'Missing Site Name in Resource JDL' )

    # Get common site mask and check the agent site
    result = self.jobDB.getSiteMask( siteState = 'Active' )
    if not result['OK']:
      return S_ERROR( 'Internal error: can not get site mask' )
    maskList = result['Value']

    siteName = resourceDict['Site']
    if siteName not in maskList:

      # if 'GridCE' not in resourceDict:
      #  return S_ERROR( 'Site not in mask and GridCE not specified' )
      # Even if the site is banned, if it defines a CE, it must be able to check it
      # del resourceDict['Site']

      # Banned site can only take Test jobs
      resourceDict['JobType'] = 'Test'

    resourceDict['Setup'] = self.serviceInfoDict['clientSetup']

    gLogger.verbose( "Resource description:" )
    for key in resourceDict:
      gLogger.verbose( "%s : %s" % ( key.rjust( 20 ), resourceDict[ key ] ) )

    negativeCond = self.__limiter.getNegativeCondForSite( siteName )
    result = self.tqDB.matchAndGetJob( resourceDict, negativeCond = negativeCond )

    if not result['OK']:
      return result
    result = result['Value']
    if not result['matchFound']:
      return S_ERROR( 'No match found' )

    jobID = result['jobId']
    resAtt = self.jobDB.getJobAttributes( jobID, ['OwnerDN', 'OwnerGroup', 'Status'] )
    if not resAtt['OK']:
      return S_ERROR( 'Could not retrieve job attributes' )
    if not resAtt['Value']:
      return S_ERROR( 'No attributes returned for job' )
    if not resAtt['Value']['Status'] == 'Waiting':
      gLogger.error( 'Job matched by the TQ is not in Waiting state', str( jobID ) )
      result = self.tqDB.deleteJob( jobID )
      if not result[ 'OK' ]:
        return result
      return S_ERROR( "Job %s is not in Waiting state" % str( jobID ) )

    attNames = ['Status', 'MinorStatus', 'ApplicationStatus', 'Site']
    attValues = ['Matched', 'Assigned', 'Unknown', siteName]
    result = self.jobDB.setJobAttributes( jobID, attNames, attValues )
    # result = self.jobDB.setJobStatus( jobID, status = 'Matched', minor = 'Assigned' )
    result = self.jlDB.addLoggingRecord( jobID,
                                         status = 'Matched',
                                         minor = 'Assigned',
                                         source = 'Matcher' )

    result = self.jobDB.getJobJDL( jobID )
    if not result['OK']:
      return S_ERROR( 'Failed to get the job JDL' )

    resultDict = {}
    resultDict['JDL'] = result['Value']
    resultDict['JobID'] = jobID

    matchTime = time.time() - startTime
    gLogger.info( "Match time: [%s]" % str( matchTime ) )
    gMonitor.addMark( "matchTime", matchTime )

    # Get some extra stuff into the response returned
    resOpt = self.jobDB.getJobOptParameters( jobID )
    if resOpt['OK']:
      for key, value in resOpt['Value'].items():
        resultDict[key] = value
    resAtt = self.jobDB.getJobAttributes( jobID, ['OwnerDN', 'OwnerGroup'] )
    if not resAtt['OK']:
      return S_ERROR( 'Could not retrieve job attributes' )
    if not resAtt['Value']:
      return S_ERROR( 'No attributes returned for job' )

    if self.__opsHelper.getValue( "JobScheduling/CheckMatchingDelay", True ):
      self.__limiter.updateDelayCounters( siteName, jobID )

    # Report pilot-job association
    if pilotReference:
      result = self.pilotAgentsDB.setCurrentJobID( pilotReference, jobID )
      result = self.pilotAgentsDB.setJobForPilot( jobID, pilotReference, updateStatus = False )

    resultDict['DN'] = resAtt['Value']['OwnerDN']
    resultDict['Group'] = resAtt['Value']['OwnerGroup']
    resultDict['PilotInfoReportedFlag'] = pilotInfoReported
    return S_OK( resultDict )

