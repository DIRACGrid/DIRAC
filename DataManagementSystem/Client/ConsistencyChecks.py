""" Main class for doing consistency checks, between files in:
    - File Catalog
    - Bookkeeping
    - TransformationSystem

    Should be extended to include the Storage (in DIRAC)
"""

import os
import copy
import time
import sys

import DIRAC

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC import gLogger


from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
# from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.Adler import compareAdler

# FIXME: this is quite dirty, what should be checked is exactly what it is done
prodsWithMerge = ( 'MCSimulation', 'DataStripping', 'MCStripping', 'DataSwimming', 'WGProduction' )

class ConsistencyChecks( object ):
  """ A class for handling some consistency check
  """
#  def __init__( self, interactive = True, dm = None, fc = None ):

  def __init__( self, interactive = True, transClient = None, dm = None, bkClient = None, fc = None ):
    """ c'tor

        One object for every production/BkQuery/directoriesList...
    """
    self.interactive = interactive
    self.transClient = TransformationClient() if transClient is None else transClient
    self.dm = DataManager() if dm is None else dm
    self.fc = FileCatalog() if fc is None else fc

    self.dirac = Dirac()

    # Base elements from which to start the consistency checks
    self._prod = 0
    self._bkQuery = None
    self._fileType = []
    self._fileTypesExcluded = []
    self._lfns = []
    self.noLFC = False
    self.directories = []

    # Accessory elements
    self.runsList = []
    self.runStatus = None
    self.fromProd = None
    self.transType = ''
    self.cachedReplicas = {}

    self.prcdWithDesc = []
    self.prcdWithoutDesc = []
    self.prcdWithMultDesc = []
    self.nonPrcdWithDesc = []
    self.nonPrcdWithoutDesc = []
    self.nonPrcdWithMultDesc = []
    self.descForPrcdLFNs = []
    self.descForNonPrcdLFNs = []
    self.removedFiles = []

    self.absentLFNsInFC = []
    self.existLFNsNoSE = {}
    self.existLFNsBadReplicas = {}
    self.existLFNsBadFiles = {}

    self.commonAncestors = {}
    self.multipleDescendants = {}
    self.ancestors = {}

  ################################################################################

  def getReplicasPresence( self, lfns ):
    """ get the replicas using the standard DataManager.getReplicas()
    """
    present = set()
    notPresent = set()

    chunkSize = 1000
    printProgress = ( len( lfns ) > chunkSize )
    startTime = time.time()
    self.__write( "Checking replicas for %d files%s" %
                  ( len( lfns ), ( ' (chunks of %d)' % chunkSize ) if printProgress else '... ' ) )
    for chunk in breakListIntoChunks( lfns, chunkSize ):
      if printProgress:
        self.__write( '.' )
      for _ in range( 1, 10 ):
        res = self.dm.getReplicas( chunk )
        if res['OK']:
          present.update( res['Value']['Successful'] )
          self.cachedReplicas.update( res['Value']['Successful'] )
          notPresent.update( res['Value']['Failed'] )
          break
        else:
          gLogger.error( "\nError getting replicas from FC, retry", res['Message'] )
          time.sleep( 0.1 )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    gLogger.info( "Found %d files with replicas and %d without" % ( len( present ), len( notPresent ) ) )
    return list( present ), list( notPresent )

  ################################################################################

  def getReplicasPresenceFromDirectoryScan( self, lfns ):
    """ Get replicas scanning the directories. Might be faster.
    """

    dirs = {}
    present = []
    notPresent = []
    compare = True

    for lfn in lfns:
      dirN = os.path.dirname( lfn )
      if lfn == dirN + '/':
        compare = False
      dirs.setdefault( dirN, [] ).append( lfn )

    if compare:
      self.__write( "Checking File Catalog for %d files from %d directories " % ( len( lfns ), len( dirs ) ) )
    else:
      self.__write( "Getting files from %d directories " % len( dirs ) )
    startTime = time.time()

    for dirN in sorted( dirs ):
      startTime1 = time.time()
      self.__write( '.' )
      lfnsFound = self._getFilesFromDirectoryScan( dirN )
      gLogger.verbose( "Obtained %d files in %.1f seconds" % ( len( lfnsFound ), time.time() - startTime1 ) )
      if compare:
        pr, notPr = self.__compareLFNLists( dirs[dirN], lfnsFound )
        notPresent += notPr
        present += pr
      else:
        present += lfnsFound

    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
    gLogger.info( "Found %d files with replicas and %d without" % ( len( present ), len( notPresent ) ) )
    return present, notPresent

  ################################################################################

  @staticmethod
  def __compareLFNLists( lfns, lfnsFound ):
    """ return files in both lists and files in lfns and not in lfnsFound
    """
    present = []
    notPresent = lfns
    startTime = time.time()
    gLogger.verbose( "Comparing list of %d LFNs with second list of %d" % ( len( lfns ), len( lfnsFound ) ) )
    if lfnsFound:
      # print sorted( lfns )
      # print sorted( lfnsFound )
      setLfns = set( lfns )
      setLfnsFound = set( lfnsFound )
      present = list( setLfns & setLfnsFound )
      notPresent = list( setLfns - setLfnsFound )
      # print sorted( present )
      # print sorted( notPresent )
    gLogger.verbose( "End of comparison: %.1f seconds" % ( time.time() - startTime ) )
    return present, notPresent

  def _getFilesFromDirectoryScan( self, dirs ):
    """ calls dm.getFilesFromDirectory
    """

    level = gLogger.getLevel()
    gLogger.setLevel( 'FATAL' )
    res = self.dm.getFilesFromDirectory( dirs )
    gLogger.setLevel( level )
    if not res['OK']:
      if 'No such file or directory' not in res['Message']:
        gLogger.error( "Error getting files from directories %s:" % dirs, res['Message'] )
      return []
    if res['Value']:
      lfnsFound = res['Value']
    else:
      lfnsFound = []

    return lfnsFound

  ################################################################################

  def _getTSFiles( self ):
    """ Helper function - get files from the TS
    """

    selectDict = { 'TransformationID': self.prod}
    if self._lfns:
      selectDict['LFN'] = self._lfns
    elif self.runStatus and self.fromProd:
      res = self.transClient.getTransformationRuns( {'TransformationID': self.fromProd, 'Status':self.runStatus} )
      if not res['OK']:
        gLogger.error( "Failed to get runs for transformation %d" % self.prod )
      else:
        if res['Value']:
          self.runsList.extend( [run['RunNumber'] for run in res['Value'] if run['RunNumber'] not in self.runsList] )
          gLogger.always( "%d runs selected" % len( res['Value'] ) )
        elif not self.runsList:
          gLogger.always( "No runs selected, check completed" )
          DIRAC.exit( 0 )
    if not self._lfns and self.runsList:
      selectDict['RunNumber'] = self.runsList

    res = self.transClient.getTransformation( self.prod )
    if not res['OK']:
      gLogger.error( "Failed to find transformation %s" % self.prod )
      return [], [], []
    status = res['Value']['Status']
    if status not in ( 'Active', 'Stopped', 'Completed', 'Idle' ):
      gLogger.always( "Transformation %s in status %s, will not check if files are processed" % ( self.prod, status ) )
      processedLFNs = []
      nonProcessedLFNs = []
      nonProcessedStatuses = []
      if self._lfns:
        processedLFNs = self._lfns
    else:
      res = self.transClient.getTransformationFiles( selectDict )
      if not res['OK']:
        gLogger.error( "Failed to get files for transformation %d" % self.prod, res['Message'] )
        return [], [], []
      else:
        processedLFNs = [item['LFN'] for item in res['Value'] if item['Status'] == 'Processed']
        nonProcessedLFNs = [item['LFN'] for item in res['Value'] if item['Status'] != 'Processed']
        nonProcessedStatuses = list( set( [item['Status'] for item in res['Value'] if item['Status'] != 'Processed'] ) )

    return processedLFNs, nonProcessedLFNs, nonProcessedStatuses

  ################################################################################

  def __write( self, text ):
    if self.interactive:
      sys.stdout.write( text )
      sys.stdout.flush()

  ################################################################################

  def _selectByFileType( self, lfnDict, fileTypes = None, fileTypesExcluded = None ):
    """ Select only those files from the values of lfnDict that have a certain type
    """
    if not lfnDict:
      return {}
    if not fileTypes:
      fileTypes = self.fileType
    if not fileTypesExcluded:
      fileTypesExcluded = self.fileTypesExcluded
    else:
      fileTypesExcluded += [ft for ft in self.fileTypesExcluded if ft not in fileTypesExcluded]
    # lfnDict is a dictionary of dictionaries including the metadata, create a deep copy to get modified
    ancDict = copy.deepcopy( lfnDict )
    if fileTypes == ['']:
      fileTypes = []
    # and loop on the original dictionaries
    for ancestor in lfnDict:
      for desc in lfnDict[ancestor]:
        ft = lfnDict[ancestor][desc]['FileType']
        if ft in fileTypesExcluded or ( fileTypes and ft not in fileTypes ):
          ancDict[ancestor].pop( desc )
      if len( ancDict[ancestor] ) == 0:
        ancDict.pop( ancestor )
    return ancDict

  @staticmethod
  def _getFileTypesCount( lfnDict ):
    """ return file types count
    """
    ft_dict = {}
    for ancestor in lfnDict:
      t_dict = {}
      for desc in lfnDict[ancestor]:
        ft = lfnDict[ancestor][desc]['FileType']
        t_dict[ft] = t_dict.setdefault( ft, 0 ) + 1
      ft_dict[ancestor] = t_dict

    return ft_dict

  def __getLFNsFromFC( self ):
    if not self.lfns:
      directories = []
      for dirName in self.__getDirectories():
        if not dirName.endswith( '/' ):
          dirName += '/'
        directories.append( dirName )
      present, notPresent = self.getReplicasPresenceFromDirectoryScan( directories )
      gLogger.always( '%d files found in the FC' % len( present ) )
    else:
      present, notPresent = self.getReplicasPresence( self.lfns )
      gLogger.always( 'Out of %d files, %d are in the FC, %d are not' \
                      % ( len( self.lfns ), len( present ), len( notPresent ) ) )
    return present, notPresent

  def compareChecksum( self, lfns ):
    """compare the checksum of the file in the FC and the checksum of the physical replicas.
       Returns a dictionary containing 3 sub-dictionaries: one with files with missing PFN, one with
       files with all replicas corrupted, and one with files with some replicas corrupted and at least
       one good replica
    """
    retDict = {'AllReplicasCorrupted' : {}, 'SomeReplicasCorrupted': {}, 'MissingPFN':{}, 'NoReplicas':{}}

    chunkSize = 1000
    replicas = {}
    setLfns = set( lfns )
    cachedLfns = setLfns & set( self.cachedReplicas )
    for lfn in cachedLfns:
      replicas[lfn] = self.cachedReplicas[lfn]
    lfnsLeft = list( setLfns - cachedLfns )
    startTime = time.time()
    if lfnsLeft:
      self.__write( "Get replicas for %d files (chunks of %d): " % ( len( lfnsLeft ), chunkSize ) )
      for lfnChunk in breakListIntoChunks( lfnsLeft, chunkSize ):
        self.__write( '.' )
        replicasRes = self.dm.getReplicas( lfnChunk )
        if not replicasRes['OK']:
          gLogger.error( "error:  %s" % replicasRes['Message'] )
          raise RuntimeError( "error:  %s" % replicasRes['Message'] )
        replicasRes = replicasRes['Value']
        if replicasRes['Failed']:
          retDict['NoReplicas'].update( replicasRes['Failed'] )
        replicas.update( replicasRes['Successful'] )
      self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    self.__write( "Get FC metadata for %d files to be checked: " % len( lfns ) )
    metadata = {}
    for lfnChunk in breakListIntoChunks( replicas.keys(), chunkSize ):
      self.__write( '.' )
      res = self.fc.getFileMetadata( lfnChunk )
      if not res['OK']:
        raise RuntimeError( "error %s" % res['Message'] )
      metadata.update( res['Value']['Successful'] )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    gLogger.always( "Check existence and compare checksum file by file..." )
    csDict = {}
    seFiles = {}
    surlLfn = {}
    startTime = time.time()
    # Reverse the LFN->SE dictionary
    for lfn in replicas:
      csDict.setdefault( lfn, {} )[ 'LFCChecksum' ] = metadata.get( lfn, {} ).get( 'Checksum' )
      replicaDict = replicas[ lfn ]
      for se in replicaDict:
        surl = replicaDict[ se ]
        surlLfn[surl] = lfn
        seFiles.setdefault( se, [] ).append( surl )

    checkSum = {}
    self.__write( 'Getting checksum of %d replicas in %d SEs (chunks of %d): ' % ( len( surlLfn ), len( seFiles ), chunkSize ) )
    pfnNotAvailable = {}
    logLevel = gLogger.getLevel()
    gLogger.setLevel( 'FATAL' )
    for num, se in enumerate( sorted( seFiles ) ):
      self.__write( '\n%d. At %s (%d files): ' % ( num, se, len( seFiles[se] ) ) )
      oSe = StorageElement( se )
      for surlChunk in breakListIntoChunks( seFiles[se], chunkSize ):
        self.__write( '.' )
        surlRes = oSe.getFileMetadata( surlChunk )
        if not surlRes['OK']:
          gLogger.error( "error StorageElement.getFileMetadata returns %s" % ( surlRes['Message'] ) )
          raise RuntimeError( "error StorageElement.getFileMetadata returns %s" % ( surlRes['Message'] ) )
        surlRes = surlRes['Value']
        for surl in surlRes['Failed']:
          lfn = surlLfn[surl]
          gLogger.info( "SURL was not found at %s! %s " % ( se, surl ) )
          pfnNotAvailable.setdefault( lfn, [] ).append( se )
        for surl in surlRes['Successful']:
          lfn = surlLfn[surl]
          checkSum.setdefault( lfn, {} )[se] = surlRes['Successful'][ surl ]['Checksum']
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
    gLogger.setLevel( logLevel )
    retDict[ 'MissingPFN'] = {}

    startTime = time.time()
    self.__write( 'Verifying checksum of %d files (chunks of %d) ' % ( len( replicas ), chunkSize ) )
    for num, lfn in enumerate( replicas ):
      # get the lfn checksum from the LFC
      if num % chunkSize == 0:
        self.__write( '.' )

      replicaDict = replicas[ lfn ]
      oneGoodReplica = False
      allGoodReplicas = True
      lfcChecksum = csDict[ lfn ].pop( 'LFCChecksum' )
      for se in replicaDict:
        # If replica doesn't exist skip check
        if se in pfnNotAvailable.get( lfn, [] ):
          allGoodReplicas = False
          continue
        surl = replicaDict[ se ]
        # get the surls metadata and compare the checksum
        surlChecksum = checkSum.get( lfn, {} ).get( se, '' )
        if not surlChecksum or not compareAdler( lfcChecksum , surlChecksum ):
          # if lfcChecksum does not match surlChecksum
          csDict[ lfn ][ se ] = {'SURL':surl, 'PFNChecksum': surlChecksum}
          gLogger.info( "ERROR!! checksum mismatch at %s for LFN %s:  LFC checksum: %s , PFN checksum : %s "
                        % ( se, lfn, lfcChecksum, surlChecksum ) )
          allGoodReplicas = False
        else:
          oneGoodReplica = True
      if not oneGoodReplica:
        if lfn in pfnNotAvailable:
          gLogger.info( "=> All replicas are missing" )
          retDict['MissingPFN'][ lfn] = 'All'
        else:
          gLogger.info( "=> All replicas have bad checksum" )
          retDict['AllReplicasCorrupted'][ lfn ] = csDict[ lfn ]
      elif not allGoodReplicas:
        if lfn in pfnNotAvailable:
          gLogger.info( "=> At least one replica missing" )
          retDict['MissingPFN'][lfn] = pfnNotAvailable[lfn]
        else:
          gLogger.info( "=> At least one replica with good Checksum" )
          retDict['SomeReplicasCorrupted'][ lfn ] = csDict[ lfn ]

    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
    return retDict

  ################################################################################
  # properties

  def set_prod( self, value ):
    """ Setter """
    if value:
      value = int( value )
      res = self.transClient.getTransformation( value, extraParams = False )
      if not res['OK']:
        raise RuntimeError( "Couldn't find transformation %d: %s" % ( value, res['Message'] ) )
      else:
        self.transType = res['Value']['Type']
      if self.interactive:
        gLogger.info( "Production %d has type %s" % ( value, self.transType ) )
    else:
      value = 0
    self._prod = value
  def get_prod( self ):
    """ Getter """
    return self._prod
  prod = property( get_prod, set_prod )

  def set_fileType( self, value ):
    """ Setter """
    fts = [ft.upper() for ft in value]
    self._fileType = fts
  def get_fileType( self ):
    """ Getter """
    return self._fileType
  fileType = property( get_fileType, set_fileType )

  def set_fileTypesExcluded( self, value ):
    """ Setter """
    fts = [ft.upper() for ft in value]
    self._fileTypesExcluded = fts
  def get_fileTypesExcluded( self ):
    """ Getter """
    return self._fileTypesExcluded
  fileTypesExcluded = property( get_fileTypesExcluded, set_fileTypesExcluded )

  def set_lfns( self, value ):
    """ Setter """
    if type( value ) == type( "" ):
      value = [value]
    value = [v.replace( ' ', '' ).replace( '//', '/' ) for v in value]
    self._lfns = value
  def get_lfns( self ):
    """ Getter """
    return self._lfns
  lfns = property( get_lfns, set_lfns )
