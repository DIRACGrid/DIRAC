""" Main class for doing consistency checks, between files in:
    - File Catalog
    - Bookkeeping
    - TransformationSystem

    Should be extended to include the Storage (in DIRAC)
"""

import os, copy, ast, time, sys

import DIRAC
from DIRAC import gLogger, S_ERROR, S_OK
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Storage.StorageElement import StorageElement

### Was LHCbDIRAC TS Client
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.Adler import compareAdler

class ConsistencyChecks( object ):
  """ A class for handling some consistency check
  """

  def __init__( self, interactive = True, transClient = None, dm = None ):
    """ c'tor

        One object for every production/directoriesList...
    """
    self.interactive = interactive
    self.transClient = TransformationClient() if transClient is None else transClient
    self.dm = DataManager() if dm is None else dm
    
    self.dirac = Dirac()

    # Base elements from which to start the consistency checks
    self._prod = 0
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

    # Results of the checks

    self.prcdWithDesc = []
    self.prcdWithoutDesc = []
    self.prcdWithMultDesc = []
    self.nonPrcdWithDesc = []
    self.nonPrcdWithoutDesc = []
    self.nonPrcdWithMultDesc = []
    self.descForPrcdLFNs = []
    self.descForNonPrcdLFNs = []
    self.removedFiles = []

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
      while True:
        res = self.dm.getReplicas( chunk )
        if res['OK']:
          present.update( res['Value']['Successful'] )
          self.cachedReplicas.update( res['Value']['Successful'] )
          notPresent.update( res['Value']['Failed'] )
          break
        else:
          gLogger.error( "\nError getting replicas from FC, retry", res['Message'] )
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

  def __getDaughtersInfo( self, lfns, status, filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants ):
    """ Get BK information about daughers of lfns """
    chunkSize = 100 if self.transType == 'DataStripping' and len( self.fileType ) > 1 else 500
    self.__write( "Now getting daughters for %d %s mothers in production %d (chunks of %d) "
                  % ( len( lfns ), status, self.prod, chunkSize ) )
    startTime = time.time()
    daughtersBKInfo = {}
    for lfnChunk in breakListIntoChunks( lfns, chunkSize ):
      self.__write( '.' )
      while True:
        resChunk = self.bkClient.getFileDescendants( lfnChunk, depth = 1,
                                                     production = self.prod, checkreplica = False )
        if resChunk['OK']:
          # Key is ancestor, value is metadata dictionary of daughters
          descDict = self._selectByFileType( resChunk['Value']['WithMetadata'] )
          # Do the daughters have a replica flag in BK? Store file type as well... Key is daughter
          daughtersBKInfo.update( dict( ( lfn, ( desc[lfn]['GotReplica'] == 'Yes', desc[lfn]['FileType'] ) )
                                      for desc in descDict.values() for lfn in desc ) )
          # Count the daughters per file type (key is ancestor)
          ft_count = self._getFileTypesCount( descDict )
          for lfn in lfnChunk:
            # Check if file has a daughter and how many per file type
            if lfn in descDict:
              # Assign the daughters list to the initial LFN
              filesWithDescendants[lfn] = descDict[lfn].keys()
              # Is there a file type with more than one daughter of a given file type?
              multi = dict( [( ft, ftc ) for ft, ftc in ft_count[lfn].items() if ftc > 1] )
              if multi:
                filesWithMultipleDescendants[lfn] = multi
            else:
              # No daughter, easy case!
              filesWithoutDescendants[lfn] = None
          break
        else:
          gLogger.error( "\nError getting daughters for %d files, retry"
                        % len( lfnChunk ), resChunk['Message'] )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
    return daughtersBKInfo

  def getDescendants( self, lfns, status = '' ):
    """ get the descendants of a list of LFN (for the production)
    """
    if type( lfns ) == type( '' ):
      lfns = [lfns]
    elif type( lfns ) == type( {} ):
      lfns = lfns.keys()
    filesWithDescendants = {}
    filesWithoutDescendants = {}
    filesWithMultipleDescendants = {}
    fileTypesExcluded = Operations().getValue( 'DataConsistency/IgnoreDescendantsOfType', [] )
    inFCNotInBK = []
    inBKNotInFC = []
    allDaughters = []
    removedFiles = []
    if not lfns:
      return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, \
        allDaughters, inFCNotInBK, inBKNotInFC, removedFiles

    daughtersBKInfo = self.__getDaughtersInfo( lfns, status, filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants )

    # This is the list of all daughters, sets will contain unique entries
    setAllDaughters = set( daughtersBKInfo )
    allDaughters = list( setAllDaughters )
    inBK = set( [lfn for lfn in setAllDaughters if daughtersBKInfo[lfn][0]] )
    setRealDaughters = set()
    # Now check whether these daughters files have replicas or have descendants that have replicas
    chunkSize = 100 if self.transType == 'DataStripping' and len( self.fileType ) > 1 else 500
    if filesWithDescendants:
      # First check in LFC the presence of daughters
      if not self.noLFC:
        present, notPresent = self.getReplicasPresenceFromDirectoryScan( allDaughters ) \
                                if len( allDaughters ) > 10 * chunkSize and \
                                   len( inBK ) < len( allDaughters ) / 2 else \
                              self.getReplicasPresence( allDaughters )
        setPresent = set( present )
        setNotPresent = set( notPresent )
      else:
        setPresent = inBK
        setNotPresent = setAllDaughters - inBK

      setRealDaughters = setPresent
      # Now check consistency between BK and FC for daughters
      inBKNotInFC = list( inBK & setNotPresent )
      inFCNotInBK = list( setPresent - inBK )

      # Now check whether the daughters without replica have a descendant
      if setNotPresent:
        chunkSize = 500
        startTime = time.time()
        self.__write( "Now checking descendants from %d daughters without replicas (chunks of %d) "
                      % ( len( setNotPresent ), chunkSize ) )
        # Get existing descendants of notPresent daughters
        setDaughtersWithDesc = set()
        for lfnChunk in breakListIntoChunks( list( setNotPresent ), chunkSize ):
          self.__write( '.' )
          while True:
            res = self.bkClient.getFileDescendants( lfnChunk, depth = 99, checkreplica = True )
            if res['OK']:
              # Exclude ignored file types, but select any other file type, key is daughters
              setDaughtersWithDesc.update( self._selectByFileType( res['Value']['WithMetadata'], fileTypes = [''],
                                                                   fileTypesExcluded = fileTypesExcluded ) )
              break
            else:
              gLogger.error( "\nError getting descendants for %d files, retry"
                             % len( lfnChunk ), res['Message'] )
        self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
        # print "%d not Present daughters, %d have a descendant" % ( len( notPresent ), len( setDaughtersWithDesc ) )

        startTime = time.time()
        chunkSize = 500
        self.__write( "Now establishing final list of existing descendants for %d mothers (chunks of %d)"
                      % ( len( filesWithDescendants ), chunkSize ) )
        i = -1
        for lfn in set( filesWithDescendants ):
          verbose = False
          setDaughters = set( filesWithDescendants[lfn] )
          i += 1
          if i % chunkSize == 0:
            self.__write( '.' )
          # If all daughters are present, all is easy...
          daughtersNotPresent = setDaughters & setNotPresent
          if not daughtersNotPresent:
            continue
          if verbose:
            print '\n\nLFN', lfn
            print 'Daughters', sorted( filesWithDescendants[lfn] )
            print 'Not present daughters', sorted( list( daughtersNotPresent ) )
            # print 'Multiple descendants', filesWithMultipleDescendants.get( lfn )
          # Only interested in daughters without replica, so if all have one, skip

          # Some daughters may have a replica though, take them into account
          daughtersWithReplica = setDaughters & setPresent
          # and add those without a replica but that have  a descendant with replica
          realDaughters = list( daughtersWithReplica.union( daughtersNotPresent & setDaughtersWithDesc ) )
          if verbose:
            print 'realDaughters', realDaughters
          # descToCheck: dictionary with key = daughter and value = dictionary of file type counts
          daughtersDict = dict( [( daughter, {daughter:{'FileType':daughtersBKInfo[daughter][1]}} ) for daughter in realDaughters] )
          if verbose:
            print 'daughtersDict', daughtersDict
          descToCheck = self._getFileTypesCount ( daughtersDict )
          if verbose:
            print 'descToCheck', descToCheck

          # Update the result dictionaries according to the final set of descendants
          if len( descToCheck ) == 0:
            # Mother has no descendant
            gLogger.verbose( '%s has no real descendants' % lfn )
            filesWithMultipleDescendants.pop( lfn, None )
            filesWithDescendants.pop( lfn, None )
            filesWithoutDescendants[lfn] = None
          else:
            filesWithDescendants[lfn] = realDaughters
            setRealDaughters.update( realDaughters )
            # Count the descendants by file type
            ft_count = {}
            for counts in descToCheck.values():
              for ft in counts:
                ft_count[ft] = ft_count.setdefault( ft, 0 ) + counts.get( ft, 0 )
            if verbose:
              print 'ft_count', ft_count
            multi = dict( [( ft, ftc ) for ft, ftc in ft_count.items() if ftc > 1] )
            if verbose:
              print 'Multi', multi
            # Mother has at least one real descendant
            # Now check whether there are more than one descendant of the same file type
            if not multi:
              filesWithMultipleDescendants.pop( lfn, None )
              prStr = 'single'
            else:
              filesWithMultipleDescendants[lfn] = multi
              prStr = 'multiple'
            gLogger.verbose( '%s has %s descendants: %s' % ( lfn, prStr, sorted( descToCheck ) ) )
        self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
        startTime = time.time()
        gLogger.verbose( "Reduced list of descendants in %.1f seconds" % ( time.time() - startTime ) )
    # print 'Final multiple descendants', filesWithMultipleDescendants

    # File files without descendants don't exist, not important
    if filesWithoutDescendants:
      present, removedFiles = self.getReplicasPresence( filesWithoutDescendants.keys() )
      filesWithoutDescendants = dict.fromkeys( present )
    else:
      removedFiles = []

    # Remove files with multiple descendants from files with descendants
    for lfn in filesWithMultipleDescendants:
      filesWithDescendants.pop( lfn, None )
    # For files in FC and not in BK, ignore if they are not active
    if inFCNotInBK:
      inFCNotInBK = self.getReplicasPresence( inFCNotInBK )[0]
    return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, \
      list( setRealDaughters ), inFCNotInBK, inBKNotInFC, removedFiles

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

  ################################################################################

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
          res = self.dm.getCatalogListDirectory( topDir )
          if not res['OK']:
            raise RuntimeError( res['Message'] )
          else:
            matchDir = directory.split( '...' )[0]
            directories += [d for d in res['Value']['Successful'].get( topDir, {} ).get( 'SubDirs', [] ) if d.startswith( matchDir )]
      if printout:
        gLogger.always( 'Expanded list of %d directories:\n%s' % ( len( directories ), '\n'.join( directories ) ) )
      return directories
    elif self.prod:
      res = self.transClient.getTransformationParameters( self.prod, ['OutputDirectories'] )
      if not res['OK']:
        raise RuntimeError( res['Message'] )
      else:
        directories = []
        dirList = res['Value'].split( '\n' )
        for dirName in dirList:
          # There is a shortcut when multiple streams are used, only the stream name is repeated!
          if ';' in dirName:
            items = dirName.split( ';' )
            baseDir = os.path.dirname( items[0] )
            items[0] = os.path.basename( items[0] )
            lastItems = items[-1].split( '/' )
            items[-1] = lastItems[0]
            if len( lastItems ) > 1:
              suffix = '/'.join( lastItems[1:] )
            else:
              suffix = ''
            for it in items:
              directories.append( os.path.join( baseDir, it, suffix ) )
          else:
            directories.append( dirName )
        return directories
    else:
      raise RuntimeError( "Need to specify either the directories or a production id" )



  ################################################################################

  def checkFC2SE( self, bkCheck = True ):
    self.checkFC2BK( bkCheck = bkCheck )
    if self.existLFNsBKRepYes or self.existLFNsBKRepNo:
      repDict = self.compareChecksum( self.existLFNsBKRepYes + self.existLFNsBKRepNo.keys() )
      if not repDict['OK']:
        gLogger.error( "Error when comparing checksum", repDict['Message'] )
        return
      repDict = repDict['Value']
      self.existLFNsNoSE = repDict['MissingPFN']
      self.existLFNsBadReplicas = repDict['SomeReplicasCorrupted']
      self.existLFNsBadFiles = repDict['AllReplicasCorrupted']

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
        if not replicasRes[ 'OK' ]:
          gLogger.error( "error:  %s" % replicasRes['Message'] )
          return replicasRes
        replicasRes = replicasRes['Value']
        if replicasRes['Failed']:
          retDict['NoReplicas'].update( replicasRes['Failed'] )
        replicas.update( replicasRes['Successful'] )
      self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    self.__write( "Get FC metadata for %d files to be checked" % len( lfns ) )
    metadata = {}
    for lfnChunk in breakListIntoChunks( replicas.keys(), chunkSize ):
      self.__write( '.' )
      res = self.dm.getCatalogFileMetadata( lfnChunk )
      if not res['OK']:
        gLogger.error( "error %s" % res['Message'] )
        return res
      metadata.update( res['Value']['Successful'] )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    gLogger.always( "Check existence and compare checksum file by file ..." )
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
    self.__write( 'Getting checksum of %d replicas (chunks of %d) ' % ( len( surlLfn ), chunkSize ) )
    pfnNotAvailable = {}
    logLevel = gLogger.getLevel()
    gLogger.setLevel( 'FATAL' )
    for se in seFiles:
      self.__write( '\nAt %s ' % se )
      oSe = StorageElement( se )
      for surlChunk in breakListIntoChunks( seFiles[se], chunkSize ):
        self.__write( '.' )
        surlRes = oSe.getFileMetadata( surlChunk )
        if not surlRes[ 'OK' ]:
          gLogger.error( "error StorageElement.getFileMetadata returns %s" % ( surlRes['Message'] ) )
          return surlRes
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
    for lfn in replicas:
      # get the lfn checksum from the LFC
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
    return S_OK( retDict )

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
