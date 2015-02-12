########################################################################
# File : Utilities.py
# Author : Federico Stagni
########################################################################

"""
Utilities for Transformation system
"""

__RCSID__ = "$Id$"

import time
import ast

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
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

    self.plugin = plugin
    self.transID = transID
    self.params = {}
    self.groupSize = 0
    self.maxFiles = 0
    if transInThread is None:
      self.transInThread = {}
    else:
      self.transInThread = transInThread
    self.transString = ''
    self.debug = debug

    self.log = gLogger.getSubLogger( "%s-PU" % plugin )

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
    if not self.groupSize:
      self.groupSize = self.getPluginParam( 'GroupSize', 10 )

    flush = ( status == 'Flush' )
    self.logVerbose( "groupByReplicas: %d files, groupSize %d, flush %s" % ( len( files ), self.groupSize, flush ) )

    # Group files by SE
    fileGroups = getFileGroups( files )
    self.logDebug( "fileGroups set: ", fileGroups )

    # Create tasks based on the group size
    tasks = []
    nTasks = 0

    # Consider files by groups of SEs, a file is only in one group
    # Then consider files site by site, but a file can now be at more than one site
    for groupSE in ( True, False ):
      if not files:
        break
      seFiles = getFileGroups( files, groupSE = groupSE )

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

  def getFileSize( self, lfns ):
    """ Get file size from a cache, if not from the catalog
    """
    fileSizes = {}
    startTime1 = time.time()
    for lfn in [lfn for lfn in lfns if lfn in self.cachedLFNSize]:
      fileSizes[lfn] = self.cachedLFNSize[lfn]
    if fileSizes:
      self.logVerbose( "Cache hit for File size for %d files" % len( fileSizes ) )
    lfns = [lfn for lfn in lfns if lfn not in self.cachedLFNSize]
    if lfns:
      startTime = time.time()
      res = self.fc.getFileSize( lfns )
      if not res['OK']:
        return S_ERROR( "Failed to get sizes for all files: " % res['Message'] )
      if res['Value']['Failed']:
        errorReason = sorted( set( res['Value']['Failed'].values() ) )
        self.logWarn( "Failed to get sizes for %d files:" % len( res['Value']['Failed'] ), errorReason )
      fileSizes.update( res['Value']['Successful'] )
      self.cachedLFNSize.update( ( res['Value']['Successful'] ) )
      self.logVerbose( "Timing for getting size of %d files from catalog: %.3f seconds" % ( len( lfns ), ( time.time() - startTime ) ) )
    self.logVerbose( "Timing for getting size of files: %.3f seconds" % ( time.time() - startTime1 ) )
    return S_OK( fileSizes )

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
    self.logVerbose( "Transformation plugin param %s: '%s'" % ( name, value ) )
    if valueType and type( value ) != valueType:
      if valueType == type( [] ):
        value = ast.literal_eval( value )
      elif valueType == type( 0 ):
        value = int( value )
      elif valueType == type( 0. ):
        value = float( value )
      elif valueType == type( True ):
        if value in ( 'False', 'No' ):
          value = False
        else:
          value = bool( value )
      elif valueType != type( '' ):
        self.logWarn( "Unknown parameter type (%s) for %s, passed as string" % ( str( valueType ), name ) )
    self.logVerbose( "Final plugin param %s: '%s'" % ( name, value ) )
    return value



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

seSvcClass = {}
def sortSEs( ses ):
  for se in ses:
    if len( se.split( ',' ) ) != 1:
      return sorted( ses )
    if se not in seSvcClass:
      seSvcClass[se] = StorageElement( se ).getStatus()['Value']['DiskSE']
  diskSEs = [se for se in ses if seSvcClass[se]]
  tapeSEs = [se for se in ses if se not in diskSEs]
  return sorted( diskSEs ) + sorted( tapeSEs )
