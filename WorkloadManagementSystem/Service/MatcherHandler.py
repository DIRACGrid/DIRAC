########################################################################
# $Id$
########################################################################
"""
Matcher class. It matches Agent Site capabilities to job requirements.
It also provides an XMLRPC interface to the Matcher

"""

__RCSID__ = "$Id$"

import time
from   types import StringType, DictType, StringTypes
import threading

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.DISET.RequestHandler                   import RequestHandler
from DIRAC.Core.Utilities.ClassAd.ClassAdLight         import ClassAd
from DIRAC                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB           import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB    import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import TaskQueueDB
from DIRAC                                             import gMonitor
from DIRAC.Core.Utilities.ThreadScheduler              import gThreadScheduler

DEBUG = 0

gMutex = threading.Semaphore()
gTaskQueues = {}
gJobDB = False
gJobLoggingDB = False
gTaskQueueDB = False

def initializeMatcherHandler( serviceInfo ):
  """  Matcher Service initialization
  """

  global gJobDB
  global gJobLoggingDB
  global gTaskQueueDB

  gJobDB = JobDB()
  gJobLoggingDB = JobLoggingDB()
  gTaskQueueDB = TaskQueueDB()

  gMonitor.registerActivity( 'matchTime', "Job matching time",
                             'Matching', "secs" , gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'matchTaskQueues', "Task queues checked per job",
                             'Matching', "task queues" , gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'matchesDone', "Job Matches",
                             'Matching', "matches" , gMonitor.OP_MEAN, 300 )
  gMonitor.registerActivity( 'numTQs', "Number of Task Queues",
                             'Matching', "tqsk queues" , gMonitor.OP_MEAN, 300 )

  gTaskQueueDB.recalculateTQSharesForAll()
  gThreadScheduler.addPeriodicTask( 120, gTaskQueueDB.recalculateTQSharesForAll )
  gThreadScheduler.addPeriodicTask( 120, sendNumTaskQueues )

  sendNumTaskQueues()

  return S_OK()

def sendNumTaskQueues():
  result = gTaskQueueDB.getNumTaskQueues()
  if result[ 'OK' ]:
    gMonitor.addMark( 'numTQs', result[ 'Value' ] )
  else:
    gLogger.error( "Cannot get the number of task queues", result[ 'Message' ] )

class MatcherHandler( RequestHandler ):

  def initialize( self ):

    self.siteJobLimits = self.getCSOption( "SiteJobLimits", False )
    self.checkPilotVersion = self.getCSOption( "CheckPilotVersion", True )
    self.setup = gConfig.getValue( '/DIRAC/Setup', '' )

  def selectJob( self, resourceDescription ):
    """ Main job selection function to find the highest priority job
        matching the resource capacity
    """

    startTime = time.time()

    # Check and form the resource description dictionary
    resourceDict = {}
    if type( resourceDescription ) in StringTypes:
      classAdAgent = ClassAd( resourceDescription )
      if not classAdAgent.isOK():
        return S_ERROR( 'Illegal Resource JDL' )
      gLogger.verbose( classAdAgent.asJDL() )

      for name in gTaskQueueDB.getSingleValueTQDefFields():
        if classAdAgent.lookupAttribute( name ):
          if name == 'CPUTime':
            resourceDict[name] = classAdAgent.getAttributeInt( name )
          else:
            resourceDict[name] = classAdAgent.getAttributeString( name )

      for name in gTaskQueueDB.getMultiValueMatchFields():
        if classAdAgent.lookupAttribute( name ):
          resourceDict[name] = classAdAgent.getAttributeString( name )

      # Check if a JobID is requested
      if classAdAgent.lookupAttribute( 'JobID' ):
        resourceDict['JobID'] = classAdAgent.getAttributeInt( 'JobID' )

      if classAdAgent.lookupAttribute( 'DIRACVersion' ):
        resourceDict['DIRACVersion'] = classAdAgent.getAttributeString( 'DIRACVersion' )

      if classAdAgent.lookupAttribute( 'VirtualOrganization' ):
        resourceDict['VirtualOrganization'] = classAdAgent.getAttributeString( 'VirtualOrganization' )

    else:
      for name in gTaskQueueDB.getSingleValueTQDefFields():
        if resourceDescription.has_key( name ):
          resourceDict[name] = resourceDescription[name]

      for name in gTaskQueueDB.getMultiValueMatchFields():
        if resourceDescription.has_key( name ):
          resourceDict[name] = resourceDescription[name]

      if resourceDescription.has_key( 'JobID' ):
        resourceDict['JobID'] = resourceDescription['JobID']

      if resourceDescription.has_key( 'DIRACVersion' ):
        resourceDict['DIRACVersion'] = resourceDescription['DIRACVersion']

      if resourceDescription.has_key( 'VirtualOrganization' ):
        resourceDict['VirtualOrganization'] = resourceDescription['VirtualOrganization']

    # Check the pilot DIRAC version
    if self.checkPilotVersion:
      if not 'DIRACVersion' in resourceDict:
        return S_ERROR( 'Version check requested and not provided by Pilot' )

      # Check if the matching Request provides a VirtualOrganization
      if 'VirtualOrganization' in resourceDict:
        voName = resourceDict['VirtualOrganization']
      # Check if the matching Request provides an OwnerGroup
      elif 'OwnerGroup' in resourceDict:
        voName = getVOForGroup( resourceDict['OwnerGroup'] )
      # else take the default VirtualOrganization for the installation
      else:
        voName = getVOForGroup( '' )

      self.pilotVersion = gConfig.getValue( '/Operations/%s/%s/Versions/PilotVersion' % ( voName, self.setup ), '' )
      if self.pilotVersion and resourceDict['DIRACVersion'] != self.pilotVersion:
        return S_ERROR( 'Pilot version does not match the production version %s:%s' % \
                       ( resourceDict['DIRACVersion'], self.pilotVersion ) )

    # Get common site mask and check the agent site
    result = gJobDB.getSiteMask( siteState = 'Active' )
    if result['OK']:
      maskList = result['Value']
    else:
      return S_ERROR( 'Internal error: can not get site mask' )

    if not 'Site' in resourceDict:
      return S_ERROR( 'Missing Site Name in Resource JDL' )

    siteName = resourceDict['Site']
    if resourceDict['Site'] not in maskList:
      if 'GridCE' in resourceDict:
        del resourceDict['Site']
      else:
        return S_ERROR( 'Site not in mask and GridCE not specified' )

    resourceDict['Setup'] = self.serviceInfoDict['clientSetup']

    if DEBUG:
      print "Resource description:"
      for key, value in resourceDict.items():
        print key.rjust( 20 ), value

    # Check if Job Limits are imposed onto the site
    extraConditions = {}
    if self.siteJobLimits:
      result = self.getExtraConditions( siteName )
      if result['OK']:
        extraConditions = result['Value']
    if extraConditions:
      gLogger.info( 'Job Limits for site %s are: %s' % ( siteName, str( extraConditions ) ) )

    result = gTaskQueueDB.matchAndGetJob( resourceDict, extraConditions = extraConditions )

    if DEBUG:
      print result

    if not result['OK']:
      return result
    result = result['Value']
    if not result['matchFound']:
      return S_ERROR( 'No match found' )

    jobID = result['jobId']
    resAtt = gJobDB.getJobAttributes( jobID, ['OwnerDN', 'OwnerGroup', 'Status'] )
    if not resAtt['OK']:
      return S_ERROR( 'Could not retrieve job attributes' )
    if not resAtt['Value']:
      return S_ERROR( 'No attributes returned for job' )
    if not resAtt['Value']['Status'] == 'Waiting':
      gLogger.error( 'Job %s matched by the TQ is not in Waiting state' % str( jobID ) )
      result = gTaskQueueDB.deleteJob( jobID )

    result = gJobDB.setJobStatus( jobID, status = 'Matched', minor = 'Assigned' )
    result = gJobLoggingDB.addLoggingRecord( jobID,
                                           status = 'Matched',
                                           minor = 'Assigned',
                                           source = 'Matcher' )

    result = gJobDB.getJobJDL( jobID )
    if not result['OK']:
      return S_ERROR( 'Failed to get the job JDL' )

    resultDict = {}
    resultDict['JDL'] = result['Value']
    resultDict['JobID'] = jobID

    matchTime = time.time() - startTime
    gLogger.info( "Match time: [%s]" % str( matchTime ) )
    gMonitor.addMark( "matchTime", matchTime )

    # Get some extra stuff into the response returned
    resOpt = gJobDB.getJobOptParameters( jobID )
    if resOpt['OK']:
      for key, value in resOpt['Value'].items():
        resultDict[key] = value
    resAtt = gJobDB.getJobAttributes( jobID, ['OwnerDN', 'OwnerGroup'] )
    if not resAtt['OK']:
      return S_ERROR( 'Could not retrieve job attributes' )
    if not resAtt['Value']:
      return S_ERROR( 'No attributes returned for job' )

    resultDict['DN'] = resAtt['Value']['OwnerDN']
    resultDict['Group'] = resAtt['Value']['OwnerGroup']
    return S_OK( resultDict )

  def getExtraConditions( self, site ):
    """ Get extra conditions allowing site throttling
    """
    # Find Site job limits
    grid = site.split( '.' )[0]
    siteSection = '/Resources/Sites/%s/%s' % ( grid, site )
    result = gConfig.getSections( siteSection )
    if not result['OK']:
      return result
    if not 'JobLimits' in result['Value']:
      return S_OK( {} )
    result = gConfig.getSections( '%s/JobLimits' % siteSection )
    if not result['OK']:
      return result
    sections = result['Value']
    limitDict = {}
    resultDict = {}
    if sections:
      for section in sections:
        result = gConfig.getOptionsDict( '%s/JobLimits/%s' % ( siteSection, section ) )
        if not result['OK']:
          return result
        optionDict = result['Value']
        if optionDict:
          limitDict[section] = []
          for key, value in optionDict.items():
            limitDict[section].append( ( key, int( value ) ) )
    if not limitDict:
      return S_OK( {} )
    # Check if the site exceeding the given limits
    fields = limitDict.keys()
    for field in fields:
      for key, value in limitDict[field]:
        if int( value ) > 0:
          result = gJobDB.getCounters( 'Jobs', ['Status'], {'Site':site, field:key} )
          if not result['OK']:
            return result
          count = 0
          if result['Value']:
            for countDict, number in result['Value']:
              if countDict['Status'] in ["Running", "Matched"]:
                count += number
          if count > value:
            if not resultDict.has_key( field ):
              resultDict[field] = []
            resultDict[field].append( key )
            gLogger.verbose( 'Job Limit imposed at %s on %s/%s/%d,'
                             ' %d jobs already deployed' % ( site, field, key, value, count ) )
        else:
          if not resultDict.has_key( field ):
            resultDict[field] = []
          resultDict[field].append( key )
          gLogger.verbose( 'Jobs prohibited at %s for %s/%s' % ( site, field, key ) )

    return S_OK( resultDict )

##############################################################################
  types_requestJob = [ [StringType, DictType] ]
  def export_requestJob( self, resourceDescription ):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """

    result = self.selectJob( resourceDescription )
    gMonitor.addMark( "matchesDone" )
    return result

##############################################################################
  types_getActiveTaskQueues = []
  def export_getActiveTaskQueues( self ):
    """ Return all task queues
    """
    return gTaskQueueDB.retrieveTaskQueues()

##############################################################################
  types_getMatchingTaskQueues = [ DictType ]
  def export_getMatchingTaskQueues( self, resourceDict ):
    """ Return all task queues
    """
    return gTaskQueueDB.retrieveTaskQueuesThatMatch( resourceDict )

