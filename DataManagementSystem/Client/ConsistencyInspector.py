""" Main class for doing consistency checks, between files in:
    - File Catalog
    - TransformationSystem

    Should be extended to include the Storage (in DIRAC)
"""

import os
import time
import sys
import re
import errno
import DIRAC

from DIRAC                                                  import S_OK, S_ERROR, gLogger
from DIRAC.DataManagementSystem.Client.DataManager          import DataManager
from DIRAC.Resources.Storage.StorageElement                 import StorageElement
from DIRAC.Resources.Catalog.FileCatalog                    import FileCatalog
from DIRAC.Core.Utilities.List                              import breakListIntoChunks
from DIRAC.Interfaces.API.Dirac                             import Dirac
from DIRAC.DataManagementSystem.Client.DataIntegrityClient  import DataIntegrityClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.Core.Utilities.Adler                             import compareAdler

class ConsistencyInspector( object ):
  """ A class for handling some consistency checks
  """
  def __init__( self, interactive = True, transClient = None, dm = None, fc = None, dic = None ):
    """ c'tor
        interactive: Data Manager (True) or DIRAC Agente (False)
        transClient: TransformationClient() if None, else transClient params
        dm: DataManager() if None, else dm params
        fc: FileCatalog() if None, else fc params
        One object for every production/directoriesList...
    """
    self.interactive = interactive
    self.transClient = TransformationClient() if transClient is None else transClient
    self.dm = dm if dm else DataManager()
    self.fc = fc if fc else FileCatalog()
    self.dic = dic if dic else DataIntegrityClient()
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
    self.existLFNsNotExisting = {}
    self.commonAncestors = {}
    self.multipleDescendants = {}
    self.ancestors = {}

    self._verbose = False

  def __logVerbose( self, msg, msg1 = '' ):
    if self._verbose:
      newMsg = '[ConsistencyChecks] ' + ( '[%s] ' % str( self.prod ) ) if self.prod else ''
      # Add that prefix to all lines of the message
      newMsg1 = msg1.replace( '\n', '\n' + newMsg )
      newMsg += msg.replace( '\n', '\n' + newMsg )
      gLogger.notice( newMsg, newMsg1 )
    else:
      gLogger.verbose( msg, msg1 )

  ################################################################################

  def checkFC2SE( self ):
    repDict = self.compareChecksum( self.lfns )
    self.existLFNsNoSE = repDict['MissingReplica']
    self.existLFNsNotExisting = repDict['MissingAllReplicas']
    self.existLFNsBadReplicas = repDict['SomeReplicasCorrupted']
    self.existLFNsBadFiles = repDict['AllReplicasCorrupted']


  def getReplicasPresence( self, lfns ):
    """ get the replicas using the standard FileCatalog.getReplicas()
    """
    present = set()
    notPresent = set()

    chunkSize = 100
    printProgress = ( len( lfns ) > chunkSize )
    startTime = time.time()
    self.__write( "Checking replicas for %d files%s" %
                  ( len( lfns ), ( ' (chunks of %d)' % chunkSize ) if printProgress else '... ' ) )
    for chunk in breakListIntoChunks( lfns, chunkSize ):
      if printProgress:
        self.__write( '.' )
      for _ in xrange( 1, 10 ):
        res = self.fc.getReplicas( chunk )
        if res['OK']:
          present.update( res['Value']['Successful'] )
          self.cachedReplicas.update( res['Value']['Successful'] )
          notPresent.update( res['Value']['Failed'] )
          break
        else:
          time.sleep( 0.1 )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    if notPresent:
      self.__logVerbose( "Files without replicas:", '\n'.join( [''] + sorted( notPresent ) ) )
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

  def __compareLFNLists( self, lfns, lfnsFound ):
    """ return files in both lists and files in lfns and not in lfnsFound
    """
    present = []
    notPresent = lfns
    startTime = time.time()
    self.__logVerbose( "Comparing list of %d LFNs with second list of %d" % ( len( lfns ), len( lfnsFound ) ) )
    if lfnsFound:
      setLfns = set( lfns )
      setLfnsFound = set( lfnsFound )
      present = list( setLfns & setLfnsFound )
      notPresent = list( setLfns - setLfnsFound )
    self.__logVerbose( "End of comparison: %.1f seconds" % ( time.time() - startTime ) )
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
          gLogger.notice( "%d runs selected" % len( res['Value'] ) )
        elif not self.runsList:
          gLogger.notice( "No runs selected, check completed" )
          DIRAC.exit( 0 )
    if not self._lfns and self.runsList:
      selectDict['RunNumber'] = self.runsList

    res = self.transClient.getTransformation( self.prod )
    if not res['OK']:
      gLogger.error( "Failed to find transformation %s" % self.prod )
      return [], [], []
    status = res['Value']['Status']
    if status not in ( 'Active', 'Stopped', 'Completed', 'Idle' ):
      gLogger.notice( "Transformation %s in status %s, will not check if files are processed" % ( self.prod, status ) )
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
        nonProcessedStatuses = list( set( item['Status'] for item in res['Value'] if item['Status'] != 'Processed' ) )

    return processedLFNs, nonProcessedLFNs, nonProcessedStatuses

  def __getDirectories( self ):
    """ get the directories where to look into (they are either given, or taken from the transformation ID
    """
    if self.directories:
      directories = []
      printout = False
      for directory in self.directories:
        if not directory.endswith( '...' ):
          directories.append( directory )
        else:
          printout = True
          topDir = os.path.dirname( directory )
          res = self.fc.listDirectory( topDir )
          if not res['OK']:
            return S_ERROR(errno.ENOENT, res['Message']) #DError(errno.ENOENT, res['Message'] )
          else:
            matchDir = directory.split( '...' )[0]
            directories += [d for d in res['Value']['Successful'].get( topDir, {} ).get( 'SubDirs', [] ) if d.startswith( matchDir )]
      if printout:
        gLogger.always( 'Expanded list of %d directories:\n%s' % ( len( directories ), '\n'.join( directories ) ) )
      return directories
    else:
      return S_ERROR(errno.ENOENT, 'Need to specify the directories')  #DError(errno.ENOENT, 'Need to specify the directories')
  ################################################################################

  def __write( self, text ):
    if self.interactive:
      sys.stdout.write( text )
      sys.stdout.flush()
      print text

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
    ancDict = dict( lfnDict )
    if fileTypes == ['']:
      fileTypes = []
    # and loop on the original dictionaries
    for ancestor in lfnDict:
      for desc in lfnDict[ancestor].keys():
        ft = lfnDict[ancestor][desc]['FileType']
        if ft in fileTypesExcluded or ( fileTypes and ft not in fileTypes ):
          ancDict[ancestor].pop( desc )
      if not len( ancDict[ancestor] ):
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
    else:
      present, notPresent = self.getReplicasPresence( self.lfns )
    return present, notPresent

  def compareChecksum( self, lfns ):
    """compare the checksum of the file in the FC and the checksum of the physical replicas.
       Returns a dictionary containing 3 sub-dictionaries: one with files with missing PFN, one with
       files with all replicas corrupted, and one with files with some replicas corrupted and at least
       one good replica
    """
    retDict = {'AllReplicasCorrupted' : {}, 'SomeReplicasCorrupted': {}, 'MissingReplica':{}, 'MissingAllReplicas':{}, 'NoReplicas':{}}

    chunkSize = 100
    replicas = {}
    setLfns = set( lfns )
    cachedLfns = setLfns & set( self.cachedReplicas )
    for lfn in cachedLfns:
      replicas[lfn] = self.cachedReplicas[lfn]
    lfnsLeft = list( setLfns - cachedLfns )
    if lfnsLeft:
      self.__write( "Get replicas for %d files (chunks of %d): " % ( len( lfnsLeft ), chunkSize ) )
      for lfnChunk in breakListIntoChunks( lfnsLeft, chunkSize ):
        self.__write( '.' )
        replicasRes = self.fc.getReplicas( lfnChunk )
        if not replicasRes['OK']:
          gLogger.error( "error:  %s" % replicasRes['Message'] )
          return S_ERROR(errno.ENOENT, "error:  %s" % replicasRes['Message'])
        replicasRes = replicasRes['Value']
        if replicasRes['Failed']:
          retDict['NoReplicas'].update( replicasRes['Failed'] )
        replicas.update( replicasRes['Successful'] )

    self.__write( "Get FC metadata for %d files to be checked: " % len( lfns ) )
    metadata = {}
    for lfnChunk in breakListIntoChunks( replicas.keys(), chunkSize ):
      self.__write( '.' )
      res = self.fc.getFileMetadata( lfnChunk )
      if not res['OK']:
        return S_ERROR(errno.ENOENT, "error %s" % res['Message'])
      metadata.update( res['Value']['Successful'] )

    gLogger.notice( "Check existence and compare checksum file by file..." )
    csDict = {}
    seFiles = {}
    # Reverse the LFN->SE dictionary
    nReps = 0
    for lfn in replicas:
      csDict.setdefault( lfn, {} )[ 'LFCChecksum' ] = metadata.get( lfn, {} ).get( 'Checksum' )
      for se in replicas[ lfn ]:
        seFiles.setdefault( se, [] ).append( lfn )
        nReps += 1

    gLogger.notice( 'Getting checksum of %d replicas in %d SEs' % ( nReps, len( seFiles ) ) )
    checkSum = {}
    lfnNotExisting = {}
    lfnNoInfo = {}
    logLevel = gLogger.getLevel()
    gLogger.setLevel( 'FATAL' )
    for num, se in enumerate( sorted( seFiles ) ):
      self.__write( '\n%d. At %s (%d files): ' % ( num, se, len( seFiles[se] ) ) )
      oSe = StorageElement( se )
      notFound = 0
      for surlChunk in breakListIntoChunks( seFiles[se], chunkSize ):
        self.__write( '.' )
        metadata = oSe.getFileMetadata( surlChunk )
        if not metadata['OK']:
          gLogger.error( "Error: getFileMetadata returns %s. Ignore those replicas" % ( metadata['Message'] ) )
          # Remove from list of replicas as we don't know whether it is OK or not
          for lfn in seFiles[se]:
            lfnNoInfo.setdefault( lfn, [] ).append( se )
        else:
          metadata = metadata['Value']
          notFound += len( metadata['Failed'] )
          for lfn in metadata['Failed']:
            lfnNotExisting.setdefault( lfn, [] ).append( se )
          for lfn in metadata['Successful']:
            checkSum.setdefault( lfn, {} )[se] = metadata['Successful'][ lfn ]['Checksum']
      if notFound:
        gLogger.error('%d files not found' % notFound)

    gLogger.setLevel( logLevel )

    gLogger.notice( 'Verifying checksum of %d files' % len( replicas ) )
    for lfn in replicas:
      # get the lfn checksum from the LFC
      replicaDict = replicas[ lfn ]
      oneGoodReplica = False
      allGoodReplicas = True
      lfcChecksum = csDict[ lfn ].pop( 'LFCChecksum' )
      for se in replicaDict:
        # If replica doesn't exist skip check
        if se in lfnNotExisting.get( lfn, [] ):
          allGoodReplicas = False
          continue
        if se in lfnNoInfo.get( lfn, [] ):
          # If there is no info, a priori it could be good
          oneGoodReplica = True
          continue
        # get the surls metadata and compare the checksum
        surlChecksum = checkSum.get( lfn, {} ).get( se, '' )
        if not surlChecksum or not compareAdler( lfcChecksum , surlChecksum ):
          # if lfcChecksum does not match surlChecksum
          csDict[ lfn ][ se ] = {'PFNChecksum': surlChecksum}
          gLogger.info( "ERROR!! checksum mismatch at %s for LFN %s:  LFC checksum: %s , PFN checksum : %s "
                        % ( se, lfn, lfcChecksum, surlChecksum ) )
          allGoodReplicas = False
        else:
          oneGoodReplica = True
      if not oneGoodReplica:
        if lfn in lfnNotExisting:
          gLogger.info( "=> All replicas are missing", lfn )
          retDict['MissingAllReplicas'][ lfn] = 'All'
        else:
          gLogger.info( "=> All replicas have bad checksum", lfn )
          retDict['AllReplicasCorrupted'][ lfn ] = csDict[ lfn ]
      elif not allGoodReplicas:
        if lfn in lfnNotExisting:
          gLogger.info( "=> At least one replica missing", lfn )
          retDict['MissingReplica'][lfn] = lfnNotExisting[lfn]
        else:
          gLogger.info( "=> At least one replica with good Checksum", lfn )
          retDict['SomeReplicasCorrupted'][ lfn ] = csDict[ lfn ]

    return S_OK(retDict)

  ################################################################################
  # properties

  def set_prod( self, value ):
    """ Setter """
    if value:
      value = int( value )
      res = self.transClient.getTransformation( value, extraParams = False )
      if not res['OK']:
        S_ERROR(errno.ENOENT, "Couldn't find transformation %d: %s" % ( value, res['Message'] ))
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
    self._fileType = [ft.upper() for ft in value]

  def get_fileType( self ):
    """ Getter """
    return self._fileType
  fileType = property( get_fileType, set_fileType )

  def set_fileTypesExcluded( self, value ):
    """ Setter """
    self._fileTypesExcluded = [ft.upper() for ft in value]

  def get_fileTypesExcluded( self ):
    """ Getter """
    return self._fileTypesExcluded
  fileTypesExcluded = property( get_fileTypesExcluded, set_fileTypesExcluded )

  def set_lfns( self, value ):
    """ Setter """
    if isinstance( value, basestring ):
      value = [value]
    value = [v.replace( ' ', '' ).replace( '//', '/' ) for v in value]
    self._lfns = value
  def get_lfns( self ):
    """ Getter """
    return self._lfns
  lfns = property( get_lfns, set_lfns )

  ###############################################################################################
  #
  #  This part was backported from DataIntegrityClient
  #
  #
  #  This section contains the specific methods for File Catalog->SE checks
  #

  def catalogDirectoryToSE( self, lfnDir ):
    """ This obtains the replica and metadata information from the catalog
        for the supplied directory and checks against the storage elements.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the FC->SE check" )
    gLogger.info( "-" * 40 )
    if isinstance( lfnDir, basestring ):
      lfnDir = [lfnDir]
    res = self._getCatalogDirectoryContents( lfnDir )
    if not res['OK']:
      return res
    replicas = res['Value']['Replicas']
    catalogMetadata = res['Value']['Metadata']
    res = self.checkPhysicalFiles( replicas, catalogMetadata )
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata, 'CatalogReplicas':replicas}
    return S_OK( resDict )

  def catalogFileToSE( self, lfns ):
    """ This obtains the replica and metadata information from the catalog and checks against the storage elements.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the FC->SE check" )
    gLogger.info( "-" * 40 )
    if isinstance( lfns, basestring):
      lfns = [lfns]
    res = self._getCatalogMetadata( lfns )
    if not res['OK']:
      return res
    catalogMetadata, _missingCatalogFiles, _zeroSizeFiles = res['Value']
    res = self._getCatalogReplicas( catalogMetadata.keys() )
    if not res['OK']:
      return res
    replicas, _zeroReplicaFiles = res['Value']
    res = self.checkPhysicalFiles( replicas, catalogMetadata )
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata, 'CatalogReplicas':replicas}
    return S_OK( resDict )

  def checkPhysicalFiles( self, replicas, catalogMetadata, ses = None ):
    """ This obtains takes the supplied replica and metadata information obtained from the catalog and checks against the storage elements.
    """

    #FIXME: we better use the compareChecksum function instead of this one! or maybe directly checkFC2SE

    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the LFC->SE check" )
    gLogger.info( "-" * 40 )
    seLfns = {}
    for lfn, replicaDict in replicas.iteritems():
      for se, _url in replicaDict.iteritems():
        if ( ses ) and ( se not in ses ):
          continue
        seLfns.setdefault( se, [] ).append( lfn )
    gLogger.info( '%s %s' % ( 'Storage Element'.ljust( 20 ), 'Replicas'.rjust( 20 ) ) )

    for se in sorted( seLfns ):
      files = len( seLfns[se] )
      gLogger.info( '%s %s' % ( se.ljust( 20 ), str( files ).rjust( 20 ) ) )

      lfns = seLfns[se]
      sizeMismatch = []
      res = self.__checkPhysicalFileMetadata( lfns, se )
      if not res['OK']:
        gLogger.error( 'Failed to get physical file metadata.', res['Message'] )
        return res
      for lfn, metadata in res['Value'].iteritems():
        if lfn in catalogMetadata:
          if metadata['Size'] != catalogMetadata[lfn]['Size']:                # and ( metadata['Size'] != 0 ):
            sizeMismatch.append( ( lfn, 'deprecatedUrl', se, 'CatalogPFNSizeMismatch' ) )
      if sizeMismatch:
        self.dic.reportProblematicReplicas( sizeMismatch, se, 'CatalogPFNSizeMismatch' )
    return S_OK()

  def __checkPhysicalFileMetadata( self, lfns, se ):
    """ Check obtain the physical file metadata and check the files are available
    """
    gLogger.info( 'Checking the integrity of %s physical files at %s' % ( len( lfns ), se ) )


    res = StorageElement( se ).getFileMetadata( lfns )

    if not res['OK']:
      gLogger.error( 'Failed to get metadata for lfns.', res['Message'] )
      return res
    lfnMetadataDict = res['Value']['Successful']
    # If the replicas are completely missing
    missingReplicas = []
    for lfn, reason in res['Value']['Failed'].iteritems():
      if re.search( 'File does not exist', reason ):
        missingReplicas.append( ( lfn, 'deprecatedUrl', se, 'PFNMissing' ) )
    if missingReplicas:
      self.dic.reportProblematicReplicas( missingReplicas, se, 'PFNMissing' )
    lostReplicas = []
    unavailableReplicas = []
    zeroSizeReplicas = []
    # If the files are not accessible
    for lfn, lfnMetadata in lfnMetadataDict.iteritems():
      if lfnMetadata['Lost']:
        lostReplicas.append( ( lfn, 'deprecatedUrl', se, 'PFNLost' ) )
      if lfnMetadata['Unavailable']:
        unavailableReplicas.append( ( lfn, 'deprecatedUrl', se, 'PFNUnavailable' ) )
      if not lfnMetadata['Size']:
        zeroSizeReplicas.append( ( lfn, 'deprecatedUrl', se, 'PFNZeroSize' ) )
    if lostReplicas:
      self.dic.reportProblematicReplicas( lostReplicas, se, 'PFNLost' )
    if unavailableReplicas:
      self.dic.reportProblematicReplicas( unavailableReplicas, se, 'PFNUnavailable' )
    if zeroSizeReplicas:
      self.dic.reportProblematicReplicas( zeroSizeReplicas, se, 'PFNZeroSize' )
    gLogger.info( 'Checking the integrity of physical files at %s complete' % se )
    return S_OK( lfnMetadataDict )

  ##########################################################################
  #
  # This section contains the specific methods for SE->File Catalog checks
  #

  def storageDirectoryToCatalog( self, lfnDir, storageElement ):
    """ This obtains the file found on the storage element in the supplied directories and determines whether they exist in the catalog and checks their metadata elements
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the SE->FC check at %s" % storageElement )
    gLogger.info( "-" * 40 )
    if isinstance( lfnDir, basestring):
      lfnDir = [lfnDir]
    res = self.getStorageDirectoryContents( lfnDir, storageElement )
    if not res['OK']:
      return res
    storageFileMetadata = res['Value']
    if storageFileMetadata:
      return self.__checkCatalogForSEFiles( storageFileMetadata, storageElement )
    return S_OK( {'CatalogMetadata':{}, 'StorageMetadata':{}} )

  def __checkCatalogForSEFiles( self, storageMetadata, storageElement ):
    gLogger.info( 'Checking %s storage files exist in the catalog' % len( storageMetadata ) )


    res = self.fc.getReplicas( storageMetadata )
    if not res['OK']:
      gLogger.error( "Failed to get replicas for LFN", res['Message'] )
      return res
    failedLfns = res['Value']['Failed']
    successfulLfns = res['Value']['Successful']
    notRegisteredLfns = []

    for lfn in storageMetadata:
      if lfn in failedLfns:
        if 'No such file or directory' in failedLfns[lfn]:
          notRegisteredLfns.append( ( lfn, 'deprecatedUrl', storageElement, 'LFNNotRegistered' ) )
          failedLfns.pop( lfn )
      elif storageElement not in successfulLfns[lfn]:
        notRegisteredLfns.append( ( lfn, 'deprecatedUrl', storageElement, 'LFNNotRegistered' ) )

    if notRegisteredLfns:
      self.dic.reportProblematicReplicas( notRegisteredLfns, storageElement, 'LFNNotRegistered' )
    if failedLfns:
      return S_ERROR(errno.ENOENT, 'Failed to obtain replicas' )


    # For the LFNs found to be registered obtain the file metadata from the catalog and verify against the storage metadata
    res = self._getCatalogMetadata( storageMetadata )
    if not res['OK']:
      return res
    catalogMetadata, _missingCatalogFiles, _zeroSizeFiles = res['Value']
    sizeMismatch = []
    for lfn, lfnCatalogMetadata in catalogMetadata.iteritems():
      lfnStorageMetadata = storageMetadata[lfn]
      if ( lfnStorageMetadata['Size'] != lfnCatalogMetadata['Size'] ) and ( lfnStorageMetadata['Size'] != 0 ):
        sizeMismatch.append( ( lfn, 'deprecatedUrl', storageElement, 'CatalogPFNSizeMismatch' ) )
    if sizeMismatch:
      self.dic.reportProblematicReplicas( sizeMismatch, storageElement, 'CatalogPFNSizeMismatch' )
    gLogger.info( 'Checking storage files exist in the catalog complete' )
    resDict = {'CatalogMetadata':catalogMetadata, 'StorageMetadata':storageMetadata}
    return S_OK( resDict )

  def getStorageDirectoryContents( self, lfnDir, storageElement ):
    """ This obtains takes the supplied lfn directories and recursively obtains the files in the supplied storage element
    """
    gLogger.info( 'Obtaining the contents for %s directories at %s' % ( len( lfnDir ), storageElement ) )

    se = StorageElement( storageElement )

    res = se.exists( lfnDir )
    if not res['OK']:
      gLogger.error( "Failed to obtain existance of directories", res['Message'] )
      return res
    for directory, error in res['Value']['Failed'].iteritems():
      gLogger.error( 'Failed to determine existance of directory', '%s %s' % ( directory, error ) )
    if res['Value']['Failed']:
      return S_ERROR(errno.ENOENT, 'Failed to determine existance of directory' )
    directoryExists = res['Value']['Successful']
    activeDirs = []
    for directory in sorted( directoryExists ):
      exists = directoryExists[directory]
      if exists:
        activeDirs.append( directory )
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = se.listDirectory( currentDir )
      activeDirs.remove( currentDir )
      if not res['OK']:
        gLogger.error( 'Failed to get directory contents', res['Message'] )
        return res
      elif currentDir in res['Value']['Failed']:
        gLogger.error( 'Failed to get directory contents', '%s %s' % ( currentDir, res['Value']['Failed'][currentDir] ) )
        return S_ERROR(errno.ENOENT, res['Value']['Failed'][currentDir] )
      else:
        dirContents = res['Value']['Successful'][currentDir]
        activeDirs.extend( se.getLFNFromURL( dirContents['SubDirs'] ).get( 'Value', {} ).get( 'Successful', [] ) )
        fileURLMetadata = dirContents['Files']
        fileMetadata = {}
        res = se.getLFNFromURL( fileURLMetadata )
        if not res['OK']:
          gLogger.error( 'Failed to get directory content LFNs', res['Message'] )
          return res

        for url, error in res['Value']['Failed'].iteritems():
          gLogger.error( "Failed to get LFN for URL", "%s %s" % ( url, error ) )
        if res['Value']['Failed']:
          return S_ERROR(errno.ENOENT, "Failed to get LFNs for PFNs" )
        urlLfns = res['Value']['Successful']
        for urlLfn, lfn in urlLfns.iteritems():
          fileMetadata[lfn] = fileURLMetadata[urlLfn]
        allFiles.update( fileMetadata )

    zeroSizeFiles = []

    for lfn in sorted( allFiles ):
      if os.path.basename( lfn ) == 'dirac_directory':
        allFiles.pop( lfn )
      else:
        metadata = allFiles[lfn]
        if not metadata['Size']:
          zeroSizeFiles.append( ( lfn, 'deprecatedUrl', storageElement, 'PFNZeroSize' ) )
    if zeroSizeFiles:
      self.dic.reportProblematicReplicas( zeroSizeFiles, storageElement, 'PFNZeroSize' )

    gLogger.info( 'Obtained at total of %s files for directories at %s' % ( len( allFiles ), storageElement ) )
    return S_OK( allFiles )

  def _getCatalogDirectoryContents( self, lfnDirs ):
    """ Obtain the contents of the supplied directory, recursively
    """

    def _getDirectoryContent( directory ):
      """ Inner function: recursively scan a directory, returns list of LFNs
      """
      filesInDirectory = {}

      gLogger.debug("Examining %s" %directory)

      res = self.fc.listDirectory( directory )
      if not res['OK']:
        gLogger.error( 'Failed to get directory contents', res['Message'] )
        return res
      if directory in res['Value']['Failed']:
        gLogger.error( 'Failed to get directory content', '%s %s' % ( directory, res['Value']['Failed'][directory] ) )
        return S_ERROR( 'Failed to get directory content' )
      if directory not in res['Value']['Successful']:
        return S_ERROR( 'Directory not existing?' )

      # first, adding the files found in the current directory
      gLogger.debug("Files in %s: %d" %(directory, len(res['Value']['Successful'][directory]['Files'])))
      filesInDirectory.update(res['Value']['Successful'][directory]['Files'])

      #then, looking for subDirectories content
      if res['Value']['Successful'][directory]['SubDirs']:
        for l_dir in res['Value']['Successful'][directory]['SubDirs']:
          #recursion here
          subDirContent = _getDirectoryContent(l_dir)
          if not subDirContent['OK']:
            return subDirContent
          else:
            filesInDirectory.update(subDirContent['Value'])

      return S_OK(filesInDirectory)



    gLogger.info( 'Obtaining the catalog contents for %d directories' % len( lfnDirs ) )

    allFiles = {}
    for lfnDir in lfnDirs:
      dirContent = _getDirectoryContent(lfnDir)
      if not dirContent['OK']:
        return dirContent
      else:
        gLogger.debug("Content of directory %s: %d files" %(lfnDir, len(dirContent['Value'])))
        allFiles.update(dirContent['Value'])

    gLogger.debug( "Content of directories examined: %d files" %len(allFiles) )

    replicas = self.fc.getReplicas(list(allFiles))
    if not replicas['OK']:
      return replicas
    if replicas['Value']['Failed']:
      return S_ERROR("Failures in replicas discovery")

    return S_OK( {'Metadata':allFiles, 'Replicas':replicas['Value']['Successful']} )


  def _getCatalogReplicas( self, lfns ):
    """ Obtain the file replicas from the catalog while checking that there are replicas
    """
    gLogger.info( 'Obtaining the replicas for %s files' % len( lfns ) )

    zeroReplicaFiles = []
    res = self.fc.getReplicas( lfns, allStatus = True )
    if not res['OK']:
      gLogger.error( 'Failed to get catalog replicas', res['Message'] )
      return res
    allReplicas = res['Value']['Successful']
    for lfn, error in res['Value']['Failed'].iteritems():
      if re.search( 'File has zero replicas', error ):
        zeroReplicaFiles.append( lfn )
    gLogger.info( 'Obtaining the replicas for files complete' )
    return S_OK( (allReplicas, zeroReplicaFiles) )

  def _getCatalogMetadata( self, lfns ):
    """ Obtain the file metadata from the catalog while checking they exist
    """
    if not lfns:
      return S_OK( {} )
    gLogger.info( 'Obtaining the catalog metadata for %s files' % len( lfns ) )

    missingCatalogFiles = []
    zeroSizeFiles = []
    res = self.fc.getFileMetadata( lfns )
    if not res['OK']:
      gLogger.error( 'Failed to get catalog metadata', res['Message'] )
      return res
    allMetadata = res['Value']['Successful']
    for lfn, error in res['Value']['Failed'].iteritems():
      if re.search( 'No such file or directory', error ):
        missingCatalogFiles.append( lfn )
    gLogger.info( 'Obtaining the catalog metadata complete' )
    return S_OK( (allMetadata, missingCatalogFiles, zeroSizeFiles) )
