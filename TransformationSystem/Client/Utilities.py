########################################################################
# File : Utilities.py
# Author : Federico Stagni
########################################################################

"""
Utilities for Transformation system
"""

__RCSID__ = "$Id$"

import ast
import random

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.Core.Utilities.Time import timeThis
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Catalog.FileCatalog  import FileCatalog
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


class PluginUtilities( object ):
  """
  Utility class used by plugins
  """

  def __init__( self, plugin = 'Standard', transClient = None, dataManager = None, fc = None,
                debug = False, transInThread = None, transID = None ):
    """
    c'tor

    Setting defaults
    """
    # clients
    if transClient is None:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient
    if dataManager is None:
      self.dm = DataManager()
    else:
      self.dm = dataManager
    if fc is None:
      self.fc = FileCatalog()
    else:
      self.fc = fc

    self.dmsHelper = DMSHelpers()

    self.plugin = plugin
    self.transID = transID
    self.params = {}
    self.groupSize = 0
    self.maxFiles = 0
    self.cachedLFNSize = {}
    self.transString = ''
    self.debug = debug
    self.seConfig = {}
    if transInThread is None:
      self.transInThread = {}
    else:
      self.transInThread = transInThread

    self.log = gLogger.getSubLogger( "%s/PluginUtilities" % plugin )

  def logVerbose( self, message, param = '' ):
    if self.debug:
      self.log.info( '(V)' + self.transString + message, param )
    else:
      self.log.verbose( self.transString + message, param )

  def logDebug( self, message, param = '' ):
    self.log.debug( self.transString + message, param )

  def logInfo( self, message, param = '' ):
    self.log.info( self.transString + message, param )

  def logWarn( self, message, param = '' ):
    self.log.warn( self.transString + message, param )

  def logError( self, message, param = '' ):
    self.log.error( self.transString + message, param )

  def logException( self, message, param = '', lException = False ):
    self.log.exception( self.transString + message, param, lException )

  def setParameters( self, params ):
    self.params = params
    self.transID = params['TransformationID']
    self.transString = self.transInThread.get( self.transID, ' [NoThread] [%d] ' % self.transID ) + '%s: ' % self.plugin




  @timeThis
  def groupByReplicas( self, files, status ):
    """
    Generates tasks based on the location of the input data

   :param dict fileReplicas:
              {'/this/is/at.1': ['SE1'],
               '/this/is/at.12': ['SE1', 'SE2'],
               '/this/is/at.2': ['SE2'],
               '/this/is/at_123': ['SE1', 'SE2', 'SE3'],
               '/this/is/at_23': ['SE2', 'SE3'],
               '/this/is/at_4': ['SE4']}

    """
    tasks = []
    nTasks = 0

    if not len( files ):
      return S_OK( tasks )

    files = dict( files )

    # Parameters
    if not self.groupSize:
      self.groupSize = self.getPluginParam( 'GroupSize', 10 )
    flush = ( status == 'Flush' )
    self.logVerbose( "groupByReplicas: %d files, groupSize %d, flush %s" % ( len( files ), self.groupSize, flush ) )

    # Consider files by groups of SEs, a file is only in one group
    # Then consider files site by site, but a file can now be at more than one site
    for groupSE in ( True, False ):
      if not files:
        break
      seFiles = getFileGroups( files, groupSE = groupSE )
      self.logDebug( "fileGroups set: ", seFiles )

      for replicaSE in sortSEs( seFiles ):
        lfns = seFiles[replicaSE]
        if lfns:
          tasksLfns = breakListIntoChunks( lfns, self.groupSize )
          lfnsInTasks = []
          for taskLfns in tasksLfns:
            if ( flush and not groupSE ) or ( len( taskLfns ) >= self.groupSize ):
              tasks.append( ( replicaSE, taskLfns ) )
              lfnsInTasks += taskLfns
          # In case the file was at more than one site, remove it from the other sites' list
          # Remove files from global list
          for lfn in lfnsInTasks:
            files.pop( lfn )
          if not groupSE:
            # Remove files from other SEs
            for se in [se for se in seFiles if se != replicaSE]:
              seFiles[se] = [lfn for lfn in seFiles[se] if lfn not in lfnsInTasks]
      self.logVerbose( "groupByReplicas: %d tasks created (groupSE %s), %d files not included in tasks" % ( len( tasks ) - nTasks,
                                                                                                            str( groupSE ),
                                                                                                            len( files ) ) )
      nTasks = len( tasks )

    return S_OK( tasks )

  def createTasksBySize( self, lfns, replicaSE, fileSizes = None, flush = False ):
    """
    Split files in groups according to the size and create tasks for a given SE
    """
    tasks = []
    if fileSizes is None:
      fileSizes = self._getFileSize( lfns ).get( 'Value' )
    if fileSizes is None:
      self.logWarn( 'Error getting file sizes, no tasks created' )
      return tasks
    taskLfns = []
    taskSize = 0
    if not self.groupSize:
      self.groupSize = float( self.getPluginParam( 'GroupSize', 1. ) ) * 1000 * 1000 * 1000  # input size in GB converted to bytes
    if not self.maxFiles:
      self.maxFiles = self.getPluginParam( 'MaxFiles', 100 )
    lfns = sorted( lfns, key = fileSizes.get )
    for lfn in lfns:
      size = fileSizes.get( lfn, 0 )
      if size:
        if size > self.groupSize:
          tasks.append( ( replicaSE, [lfn] ) )
        else:
          taskSize += size
          taskLfns.append( lfn )
          if ( taskSize > self.groupSize ) or ( len( taskLfns ) >= self.maxFiles ):
            tasks.append( ( replicaSE, taskLfns ) )
            taskLfns = []
            taskSize = 0
    if flush and taskLfns:
      tasks.append( ( replicaSE, taskLfns ) )
    return tasks


  @timeThis
  def groupBySize( self, files, status ):
    """
    Generate a task for a given amount of data
    """
    tasks = []
    nTasks = 0

    if not len( files ):
      return S_OK( tasks )

    files = dict( files )
    # Parameters
    if not self.groupSize:
      self.groupSize = float( self.getPluginParam( 'GroupSize', 1 ) ) * 1000 * 1000 * 1000  # input size in GB converted to bytes
    flush = ( status == 'Flush' )
    self.logVerbose( "groupBySize: %d files, groupSize: %d, flush: %s" % ( len( files ), self.groupSize, flush ) )

    # Get the file sizes
    res = self._getFileSize( files.keys() )
    if not res['OK']:
      return res
    fileSizes = res['Value']

    for groupSE in ( True, False ):
      if not files:
        break
      seFiles = getFileGroups( files, groupSE = groupSE )

      for replicaSE in sorted( seFiles ) if groupSE else sortSEs( seFiles ):
        lfns = seFiles[replicaSE]
        newTasks = self.createTasksBySize( lfns, replicaSE, fileSizes = fileSizes, flush = flush )
        lfnsInTasks = []
        for task  in newTasks:
          lfnsInTasks += task[1]
        tasks += newTasks

        # Remove the selected files from the size cache
        self.clearCachedFileSize( lfnsInTasks )
        if not groupSE:
          # Remove files from other SEs
          for se in [se for se in seFiles if se != replicaSE]:
            seFiles[se] = [lfn for lfn in seFiles[se] if lfn not in lfnsInTasks]
        # Remove files from global list
        for lfn in lfnsInTasks:
          files.pop( lfn )

      self.logVerbose( "groupBySize: %d tasks created with groupSE %s" % ( len( tasks ) - nTasks, str( groupSE ) ) )
      self.logVerbose( "groupBySize: %d files have not been included in tasks" % len( files ) )
      nTasks = len( tasks )

    self.logVerbose( "Grouped %d files by size" % len( files ) )
    return S_OK( tasks )


  def getExistingCounters( self, normalise = False, requestedSites = [] ):
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
        res = getSitesForSE( se )
        if not res['OK']:
          return res
        for site in res['Value']:
          if site in requestedSites:
            siteDict[site] = count
      usageDict = siteDict.copy()
    if normalise:
      usageDict = self._normaliseShares( usageDict )
    return S_OK( usageDict )

  @timeThis
  def _getFileSize( self, lfns ):
    """ Get file size from a cache, if not from the catalog
    #FIXME: have to fill the cachedLFNSize!
    """
    lfns = list( lfns )
    cachedLFNSize = dict( self.cachedLFNSize )

    fileSizes = {}
    for lfn in [lfn for lfn in lfns if lfn in cachedLFNSize]:
      fileSizes[lfn] = cachedLFNSize[lfn]
    self.logDebug( "Found cache hit for File size for %d files out of %d" % ( len( fileSizes ), len( lfns ) ) )
    lfns = [lfn for lfn in lfns if lfn not in cachedLFNSize]
    if lfns:
      fileSizes = self._getFileSizeFromCatalog( lfns, fileSizes )
      if not fileSizes['OK']:
        self.logError( fileSizes['Message'] )
        return fileSizes
      fileSizes = fileSizes['Value']
    return S_OK( fileSizes )

  @timeThis
  def _getFileSizeFromCatalog( self, lfns, fileSizes ):
    """
    Get file size from the catalog
    """
    lfns = list( lfns )
    fileSizes = dict( fileSizes )

    res = self.fc.getFileSize( lfns )
    if not res['OK']:
      return S_ERROR( "Failed to get sizes for all files: %s" % res['Message'] )
    if res['Value']['Failed']:
      errorReason = sorted( set( res['Value']['Failed'].values() ) )
      self.logWarn( "Failed to get sizes for %d files:" % len( res['Value']['Failed'] ), errorReason )
    fileSizes.update( res['Value']['Successful'] )
    self.cachedLFNSize.update( ( res['Value']['Successful'] ) )
    self.logVerbose( "Got size of %d files from catalog" % len( lfns ) )
    return S_OK( fileSizes )

  def clearCachedFileSize( self, lfns ):
    """ Utility function
    """
    for lfn in [lfn for lfn in lfns if lfn in self.cachedLFNSize]:
      self.cachedLFNSize.pop( lfn )


  def getPluginParam( self, name, default = None ):
    """ Get plugin parameters using specific settings or settings defined in the CS
        Caution: the type returned is that of the default value
    """
    # get the value of a parameter looking 1st in the CS
    if default != None:
      valueType = type( default )
    else:
      valueType = None
    # First look at a generic value...
    optionPath = "TransformationPlugins/%s" % ( name )
    value = Operations().getValue( optionPath, None )
    self.logVerbose( "Default plugin param %s: '%s'" % ( optionPath, value ) )
    # Then look at a plugin-specific value
    optionPath = "TransformationPlugins/%s/%s" % ( self.plugin, name )
    value = Operations().getValue( optionPath, value )
    self.logVerbose( "Specific plugin param %s: '%s'" % ( optionPath, value ) )
    if value != None:
      default = value
    # Finally look at a transformation-specific parameter
    value = self.params.get( name, default )
    self.logVerbose( "Transformation plugin param %s: '%s', convert it to type %s" % ( name, value, valueType ) )
    if valueType and type( value ) is not valueType:
      if valueType is list:
        try:
          value = ast.literal_eval( value ) if value and value != 'None' else []
          if type( value ) is not list:
            value = list( value )
        except ValueError:
          pass
        if type( value ) is str:
          # Value should be a string already but pylint doesn't know
          value = [val for val in str( value ).replace( ' ', '' ).split( ',' ) if val]
      elif valueType is int:
        value = int( value ) if value else 0
      elif valueType is float:
        value = float( value ) if value else 0.
      elif valueType is bool:
        if value in ( 'False', 'No', 'None', '0' ):
          value = False
        else:
          value = bool( value )
      elif valueType is not str:
        self.logWarn( "Unknown parameter type (%s) for %s, passed as string" % ( str( valueType ), name ) )
    self.logVerbose( "Final plugin param %s: '%s'" % ( name, value ) )
    return value

  @staticmethod
  def _normaliseShares( originalShares ):
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

  def uniqueSEs( self, ses ):
    newSEs = []
    for se in ses:
      if not self.isSameSEInList( se, newSEs ):
        newSEs.append( se )
    return newSEs

  def isSameSE( self, se1, se2 ):
    if se1 == se2:
      return True
    for se in ( se1, se2 ):
      if se not in self.seConfig:
        self.seConfig[se] = {}
        res = StorageElement( se ).getStorageParameters( 'SRM2' )
        if res['OK']:
          params = res['Value']
          for item in ( 'Host', 'Path' ):
            self.seConfig[se][item] = params[item].replace( 't1d1', 't0d1' )
        else:
          self.logError( "Error getting StorageElement parameters for %s" % se, res['Message'] )

    return self.seConfig[se1] == self.seConfig[se2]

  def isSameSEInList( self, se1, seList ):
    if se1 in seList:
      return True
    for se in seList:
      if self.isSameSE( se1, se ):
        return True
    return False

  def closerSEs( self, existingSEs, targetSEs, local = False ):
    """ Order the targetSEs such that the first ones are closer to existingSEs. Keep all elements in targetSEs
    """
    setTarget = set( targetSEs )
    sameSEs = set( [se1 for se1 in setTarget for se2 in existingSEs if self.isSameSE( se1, se2 )] )
    targetSEs = setTarget - set( sameSEs )
    if targetSEs:
      # Some SEs are left, look for sites
      existingSites = [self.dmsHelper.getLocalSiteForSE( se ).get( 'Value' ) for se in existingSEs if not self.dmsHelper.isSEArchive( se ) ]
      existingSites = set( [site for site in existingSites if site] )
      closeSEs = set( [se for se in targetSEs if self.dmsHelper.getLocalSiteForSE( se ).get( 'Value' ) in existingSites] )
      # print existingSEs, existingSites, targetSEs, closeSEs
      otherSEs = targetSEs - closeSEs
      targetSEs = list( closeSEs )
      random.shuffle( targetSEs )
      if not local and otherSEs:
        otherSEs = list( otherSEs )
        random.shuffle( otherSEs )
        targetSEs += otherSEs
    else:
      targetSEs = []
    return ( targetSEs + list( sameSEs ) ) if not local else targetSEs


def getFileGroups( fileReplicas, groupSE = True ):
  """
  Group files by set of SEs

  :param dict fileReplicas:
              {'/this/is/at.1': ['SE1'],
               '/this/is/at.12': ['SE1', 'SE2'],
               '/this/is/at.2': ['SE2'],
               '/this/is/at_123': ['SE1', 'SE2', 'SE3'],
               '/this/is/at_23': ['SE2', 'SE3'],
               '/this/is/at_4': ['SE4']}

  If groupSE == False, group by SE, in which case a file can be in more than one element
  """
  fileGroups = {}
  for lfn, replicas in fileReplicas.items():
    if not replicas:
      continue
    replicas = sorted( list( set( replicas ) ) )
    if not groupSE or len( replicas ) == 1:
      for rep in replicas:
        fileGroups.setdefault( rep, [] ).append( lfn )
    else:
      replicaSEs = ','.join( replicas )
      fileGroups.setdefault( replicaSEs, [] ).append( lfn )
  return fileGroups


def sortSEs( ses ):
  seSvcClass = {}
  for se in ses:
    if len( se.split( ',' ) ) != 1:
      return sorted( ses )
    if se not in seSvcClass:
      seSvcClass[se] = StorageElement( se ).getStatus()['Value']['DiskSE']
  diskSEs = [se for se in ses if seSvcClass[se]]
  tapeSEs = [se for se in ses if se not in diskSEs]
  return sorted( diskSEs ) + sorted( tapeSEs )

def sortExistingSEs( lfnSEs, lfns = None ):
  """ Sort SEs according to the number of files in each (most first)
  """
  seFrequency = {}
  archiveSEs = []
  if not lfns:
    lfns = lfnSEs.keys()
  else:
    lfns = [lfn for lfn in lfns if lfn in lfnSEs]
  for lfn in lfns:
    existingSEs = lfnSEs[lfn]
    archiveSEs += [s for s in existingSEs if isArchive( s ) and s not in archiveSEs]
    for se in [s for s in existingSEs if not isFailover( s ) and s not in archiveSEs]:
      seFrequency[se] = seFrequency.setdefault( se, 0 ) + 1
  sortedSEs = seFrequency.keys()
  # sort SEs in reverse order of frequency
  sortedSEs.sort( key = seFrequency.get, reverse = True )
  # add the archive SEs at the end
  return sortedSEs + archiveSEs

def isArchive( se ):
  return DMSHelpers().isSEArchive( se )

def isFailover( se ):
  return DMSHelpers().isSEFailover( se )

def getActiveSEs( seList, access = 'Write' ):
  """ Utility function - uses the StorageElement cached status
  """
  return [ se for se in seList if StorageElement( se ).getStatus().get( 'Value', {} ).get( access, False )]
