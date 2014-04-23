"""  TransformationPlugin is a class wrapping the supported transformation plugins
"""
import re

from DIRAC                              import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE, getSEsForSite
from DIRAC.Core.Utilities.List          import breakListIntoChunks, sortList, uniqueElements, randomize

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Catalog.FileCatalog  import FileCatalog

__RCSID__ = "$Id$"

class TransformationPlugin( object ):
  """ A TransformationPlugin object should be instantiated by every transformation.
  """

  def __init__( self, plugin, transClient = None, dataManager = None ):
    """ plugin name has to be passed in: it will then be executed as one of the functions below, e.g.
        plugin = 'BySize' will execute TransformationPlugin('BySize')._BySize()
    """
    self.params = {}
    self.data = {}
    self.plugin = plugin
    self.files = False
    if transClient is None:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    if dataManager is None:
      self.dm = DataManager()
    else:
      self.dm = dataManager

    self.fc = FileCatalog()


  def isOK( self ):
    self.valid = True
    if ( not self.data ) or ( not self.params ):
      self.valid = False
    return self.valid

  def setInputData( self, data ):
    self.data = data

  def setTransformationFiles( self, files ): #TODO ADDED
    self.files = files

  def setParameters( self, params ):
    self.params = params

  def generateTasks( self ):
    """ this is a wrapper to invoke the plugin (self._%s()" % self.plugin)
    """
    try:
      evalString = "self._%s()" % self.plugin
      return eval( evalString )
    except AttributeError, x:
      if re.search( self.plugin, str( x ) ):
        return S_ERROR( "Plugin not found" )
      else:
        raise AttributeError, x
    except Exception, x:
      gLogger.exception()
      raise Exception, x

  def _Standard( self ):
    """ Simply group by replica location
    """
    res = self._groupByReplicas()
    if not res['OK']:
      return res
    newTasks = []
    for _se, lfns in res['Value']:
      newTasks.append( ( '', lfns ) )
    return S_OK( newTasks )

  def _BySize( self ):
    """ Alias for groupBySize
    """
    return self._groupBySize()

  def _Broadcast( self ):
    """ This plug-in takes files found at the sourceSE and broadcasts to all (or a selection of) targetSEs.
    """
    if not self.params:
      return S_ERROR( "TransformationPlugin._Broadcast: The 'Broadcast' plugin requires additional parameters." )

    sourceseParam = self.params['SourceSE']
    targetseParam = self.params['TargetSE']
    sourceSEs = []
    targetSEs = []
    if sourceseParam.count( '[' ):#assume it's an array
      sourceSEs = eval( sourceseParam )
    else:
      sourceSEs = [sourceseParam]
    sourceSEs = eval( self.params['SourceSE'] )
    if targetseParam.count( '[' ):
      targetSEs = eval( targetseParam )
    elif type(targetseParam)==type([]):
      targetSEs = targetseParam
    else:
      targetSEs = [targetseParam]
    #sourceSEs = eval(self.params['SourceSE'])
    #targetSEs = eval(self.params['TargetSE'])
    destinations = int( self.params.get( 'Destinations', 0 ) )
    if destinations and ( destinations >= len(targetSEs) ):
      destinations = 0

    status = self.params['Status']
    groupSize = self.params['GroupSize']#Number of files per tasks

    fileGroups = self._getFileGroups( self.data )#groups by SE
    targetSELfns = {}
    for replicaSE, lfns in fileGroups.items():
      ses = replicaSE.split( ',' )
      #sourceSites = self._getSitesForSEs(ses)
      atSource = False
      for se in ses:
        if se in sourceSEs:
          atSource = True
      if not atSource:
        continue

      for lfn in lfns:
        targets = []
        sources = self._getSitesForSEs( ses )
        for targetSE in randomize( targetSEs ):
          site = self._getSiteForSE( targetSE )['Value']
          if not site in sources:
            if ( destinations ) and ( len( targets ) >= destinations ):
              continue
            sources.append( site )
          targets.append( targetSE )#after all, if someone wants to copy to the source, it's his choice
        strTargetSEs = str.join( ',', sortList( targets ) )
        if not targetSELfns.has_key( strTargetSEs ):
          targetSELfns[strTargetSEs] = []
        targetSELfns[strTargetSEs].append( lfn )
    tasks = []
    for ses, lfns in targetSELfns.items():
      tasksLfns = breakListIntoChunks(lfns, groupSize)
      for taskLfns in tasksLfns:
        if ( status == 'Flush' ) or ( len( taskLfns ) >= int( groupSize ) ):
          #do not allow groups smaller than the groupSize, except if transformation is in flush state
          tasks.append( ( ses, taskLfns ) )
    return S_OK( tasks )

  def _ByShare( self, shareType = 'CPU' ):
    """ first get the shares from the CS, and then makes the grouping looking at the history
    """
    res = self._getShares( shareType, normalise = True )
    if not res['OK']:
      return res
    cpuShares = res['Value']
    gLogger.info( "Obtained the following target shares (%):" )
    for site in sortList( cpuShares.keys() ):
      gLogger.info( "%s: %.1f" % ( site.ljust( 15 ), cpuShares[site] ) )

    # Get the existing destinations from the transformationDB
    res = self._getExistingCounters( requestedSites = cpuShares.keys() )
    if not res['OK']:
      gLogger.error( "Failed to get existing file share", res['Message'] )
      return res
    existingCount = res['Value']
    if existingCount:
      gLogger.info( "Existing site utilization (%):" )
      normalisedExistingCount = self._normaliseShares( existingCount.copy() )
      for se in sortList( normalisedExistingCount.keys() ):
        gLogger.info( "%s: %.1f" % ( se.ljust( 15 ), normalisedExistingCount[se] ) )

    # Group the input files by their existing replicas
    res = self._groupByReplicas()
    if not res['OK']:
      return res
    replicaGroups = res['Value']

    tasks = []
    # For the replica groups 
    for replicaSE, lfns in replicaGroups:
      possibleSEs = replicaSE.split( ',' )
      # Determine the next site based on requested shares, existing usage and candidate sites
      res = self._getNextSite( existingCount, cpuShares, candidates = self._getSitesForSEs( possibleSEs ) )
      if not res['OK']:
        gLogger.error( "Failed to get next destination SE", res['Message'] )
        continue
      targetSite = res['Value']
      # Resolve the ses for the target site
      res = getSEsForSite( targetSite )
      if not res['OK']:
        continue
      ses = res['Value']
      # Determine the selected SE and create the task 
      for chosenSE in ses:
        if chosenSE in possibleSEs:
          tasks.append( ( chosenSE, lfns ) )
          if not existingCount.has_key( targetSite ):
            existingCount[targetSite] = 0
          existingCount[targetSite] += len( lfns )
    return S_OK( tasks )

  def _getShares( self, shareType, normalise = False ):
    """ Takes share from the CS, eventually normalize them
    """
    res = gConfig.getOptionsDict( '/Resources/Shares/%s' % shareType )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR( "/Resources/Shares/%s option contains no shares" % shareType )
    shares = res['Value']
    for site, value in shares.items():
      shares[site] = float( value )
    if normalise:
      shares = self._normaliseShares( shares )
    if not shares:
      return S_ERROR( "No non-zero shares defined" )
    return S_OK( shares )

  def _getExistingCounters( self, normalise = False, requestedSites = [] ):
    res = self.transClient.getCounters( 'TransformationFiles', ['UsedSE'],
                                        {'TransformationID':self.params['TransformationID']} )
    if not res['OK']:
      return res
    usageDict = {}
    for usedDict, count in res['Value']:
      usedSE = usedDict['UsedSE']
      if usedSE != 'Unknown':
        usageDict[usedSE] = count
    if requestedSites:
      siteDict = {}
      for se, count in usageDict.items():
        res = getSitesForSE( se, gridName = 'LCG' )
        if not res['OK']:
          return res
        for site in res['Value']:
          if site in requestedSites:
            siteDict[site] = count
      usageDict = siteDict.copy()
    if normalise:
      usageDict = self._normaliseShares( usageDict )
    return S_OK( usageDict )

  @classmethod
  def _normaliseShares( self, originalShares ):
    shares = originalShares.copy()
    total = 0.0
    for site in shares.keys():
      share = float( shares[site] )
      shares[site] = share
      total += share
    for site in shares.keys():
      share = 100.0 * ( shares[site] / total )
      shares[site] = share
    return shares

  def _getNextSite( self, existingCount, cpuShares, candidates = [] ):
    # normalise the shares
    siteShare = self._normaliseShares( existingCount )
    # then fill the missing share values to 0
    for site in cpuShares.keys():
      if ( not siteShare.has_key( site ) ):
        siteShare[site] = 0.0
    # determine which site is furthest from its share
    chosenSite = ''
    minShareShortFall = -float( "inf" )
    for site, cpuShare in cpuShares.items():
      if ( candidates ) and not ( site in candidates ):
        continue
      if not cpuShare:
        continue
      existingShare = siteShare[site]
      shareShortFall = cpuShare - existingShare
      if shareShortFall > minShareShortFall:
        minShareShortFall = shareShortFall
        chosenSite = site
    return S_OK( chosenSite )

  def _groupByReplicas( self ):
    """ Generates a job based on the location of the input data """
    if not self.params:
      return S_ERROR( "TransformationPlugin._Standard: The 'Standard' plug-in requires parameters." )
    status = self.params['Status']
    groupSize = self.params['GroupSize']
    # Group files by SE
    fileGroups = self._getFileGroups( self.data )
    # Create tasks based on the group size
    tasks = []
    for replicaSE in sortList( fileGroups.keys() ):
      lfns = fileGroups[replicaSE]
      tasksLfns = breakListIntoChunks( lfns, groupSize )
      for taskLfns in tasksLfns:
        if ( status == 'Flush' ) or ( len( taskLfns ) >= int( groupSize ) ):
          tasks.append( ( replicaSE, taskLfns ) )
    return S_OK( tasks )

  def _groupBySize( self ):
    """ Generate a task for a given amount of data """
    if not self.params:
      return S_ERROR( "TransformationPlugin._BySize: The 'BySize' plug-in requires parameters." )
    status = self.params['Status']
    requestedSize = float( self.params['GroupSize'] ) * 1000 * 1000 * 1000 # input size in GB converted to bytes
    maxFiles = self.params.get( 'MaxFiles', 100 )
    # Group files by SE
    fileGroups = self._getFileGroups( self.data )
    # Get the file sizes
    res = self.fc.getFileSize( self.data )
    if not res['OK']:
      return S_ERROR( "Failed to get sizes for files" )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to get sizes for all files" )
    fileSizes = res['Value']['Successful']
    tasks = []
    for replicaSE, lfns in fileGroups.items():
      taskLfns = []
      taskSize = 0
      for lfn in lfns:
        taskSize += fileSizes[lfn]
        taskLfns.append( lfn )
        if ( taskSize > requestedSize ) or ( len( taskLfns ) >= maxFiles ):
          tasks.append( ( replicaSE, taskLfns ) )
          taskLfns = []
          taskSize = 0
      if ( status == 'Flush' ) and taskLfns:
        tasks.append( ( replicaSE, taskLfns ) )
    return S_OK( tasks )

  @classmethod
  def _getFileGroups( cls, fileReplicas ):
    """ get file groups dictionary { "SE1,SE2,SE3" : [ lfn1, lfn2 ], ... }
    
    :param dict fileReplicas: { lfn : [SE1, SE2, SE3], ... }
    """
    fileGroups = {}
    for lfn, replicas in fileReplicas.items():
      replicaSEs = ",".join( sortList( uniqueElements( replicas ) ) )
      if replicaSEs not in fileGroups:
        fileGroups[replicaSEs] = []
      fileGroups[replicaSEs].append( lfn )
    return fileGroups

  @classmethod
  def _getSiteForSE( cls, se ):
    """ Get site name for the given SE
    """
    result = getSitesForSE( se, gridName = 'LCG' )
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK( result['Value'][0] )
    return S_OK( '' )

  @classmethod
  def _getSitesForSEs( cls, seList ):
    """ Get all the sites for the given SE list
    """
    sites = []
    for se in seList:
      result = getSitesForSE( se, gridName = 'LCG' )
      if result['OK']:
        sites += result['Value']
    return sites
