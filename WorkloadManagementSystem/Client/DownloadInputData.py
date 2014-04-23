########################################################################
# $HeadURL$
# File :    DownloadInputData.py
# Author :  Stuart Paterson
########################################################################

""" The Download Input Data module wraps around the Replica Management
    components to provide access to datasets by available site protocols as
    defined in the CS for the VO.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.Resources.Storage.StorageElement                         import StorageElement
from DIRAC.Core.Utilities.Os                                        import getDiskSpace
from DIRAC                                                          import S_OK, S_ERROR, gLogger

import os, tempfile, random

COMPONENT_NAME = 'DownloadInputData'

class DownloadInputData:
  """
   retrieve InputData LFN from localSEs (if available) or from elsewhere.
  """

  #############################################################################
  def __init__( self, argumentsDict ):
    """ Standard constructor
    """
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )
    self.inputData = argumentsDict['InputData']
    self.configuration = argumentsDict['Configuration']
    self.fileCatalogResult = argumentsDict['FileCatalog']
    # By default put each input data file into a separate directory
    self.inputDataDirectory = argumentsDict.get( 'InputDataDirectory', 'PerFile' )
    self.jobID = None
    self.storageElements = {}
    self.counter = 1

  def __storageElement( self, seName ):
    return self.storageElements.setdefault( seName, StorageElement( seName ) )

  #############################################################################
  def execute( self, dataToResolve = None ):
    """This method is called to download the requested files in the case where
       enough local disk space is available.  A buffer is left in this calculation
       to leave room for any produced files.
    """

    # Define local configuration options present at every site
    localSEList = self.configuration['LocalSEList']

    self.jobID = self.configuration.get( 'JobID' )

    if dataToResolve:
      self.log.verbose( 'Data to resolve passed directly to DownloadInputData module' )
      self.inputData = dataToResolve  # e.g. list supplied by another module

    self.inputData = sorted( [x.replace( 'LFN:', '' ) for x in self.inputData] )
    self.log.info( 'InputData to be downloaded is:\n%s' % '\n'.join( self.inputData ) )

    replicas = self.fileCatalogResult['Value']['Successful']

    # Problematic files will be returned and can be handled by another module
    failedReplicas = set()
    # For the unlikely case that a file is found on two SEs at the same site
    # disk-based replicas are favoured.
    downloadReplicas = {}
    # determine Disk and Tape SEs
    diskSEs = set()
    tapeSEs = set()
    for localSE in localSEList:
      seStatus = self.__storageElement( localSE ).getStatus()['Value']
      if seStatus['Read'] and seStatus['DiskSE']:
        diskSEs.add( localSE )
      elif seStatus['Read'] and seStatus['TapeSE']:
        tapeSEs.add( localSE )

    for lfn, reps in replicas.items():
      if lfn not in self.inputData:
        self.log.verbose( 'LFN %s is not in requested input data to download' )
        failedReplicas.add( lfn )
        continue

      if not ( 'Size' in reps and 'GUID' in reps ):
        self.log.error( 'Missing LFN metadata', "%s %s" % ( lfn, str( reps ) ) )
        failedReplicas.add( lfn )
        continue

      size = reps['Size']
      guid = reps['GUID' ]
      del reps['Size']
      del reps['GUID']
      downloadReplicas[lfn] = {'SE':[], 'Size':size, 'GUID':guid}
      # First get Disk replicas
      for seName in diskSEs:
        if seName in reps:
          downloadReplicas[lfn]['SE'].append( seName )
      # If no disk replicas, take tape replicas
      if not downloadReplicas[lfn]['SE']:
        for seName in tapeSEs:
          if seName in reps:
            # Only consider replicas that are cached
            result = self.__storageElement( seName ).getFileMetadata( lfn )
            cached = result.get( 'Value', {} ).get( 'Successful', {} ).get( lfn, {} ).get( 'Cached', False )
            if cached:
              downloadReplicas[lfn]['SE'].append( seName )

    totalSize = 0
    self.log.verbose( 'Replicas to download are:' )
    for lfn, reps in downloadReplicas.items():
      self.log.verbose( lfn )
      if not reps['SE']:
        self.log.info( 'Failed to find data at local SEs, will try to download from anywhere', lfn )
        reps['SE'] = ''
      else:
        if len( reps['SE'] ) > 1:
          # if more than one SE is available randomly select one
          random.shuffle( reps['SE'] )
        # get SE and pfn from tuple
        reps['SE'] = reps['SE'][0]
      totalSize += int( reps.get( 'Size', 0 ) )
      for item, value in sorted( reps.items() ):
        if value:
          self.log.verbose( '\t%s %s' % ( item, value ) )

    self.log.info( 'Total size of files to be downloaded is %s bytes' % ( totalSize ) )
    for lfn in failedReplicas:
      self.log.warn( 'Not all file metadata (SE,PFN,Size,GUID) was available for LFN', lfn )

    # Now need to check that the list of replicas to download fits into
    # the available disk space. Initially this is a simple check and if there is not
    # space for all input data, no downloads are attempted.
    result = self.__checkDiskSpace( totalSize )
    if not result['OK']:
      self.log.warn( 'Problem checking available disk space:\n%s' % ( result ) )
      return result

    if not result['Value']:
      report = 'Not enough disk space available for download: %s / %s bytes' % ( result['Value'], totalSize )
      self.log.warn( report )
      self.__setJobParam( COMPONENT_NAME, report )
      return S_OK( { 'Failed': self.inputData, 'Successful': {}} )

    resolvedData = {}
    localSECount = 0
    for lfn in downloadReplicas:
      seName = downloadReplicas[lfn]['SE']
      guid = downloadReplicas[lfn]['GUID']
      reps = replicas.get( lfn, {} )
      if seName:
        result = self.__storageElement( seName ).getFileMetadata( lfn )
        if not result['OK']:
          self.log.error( "Error getting metadata", result['Message'] )
          failedReplicas.add( lfn )
          continue
        if lfn in result['Value']['Failed']:
          self.log.error( 'Could not get Storage Metadata for %s at %s: %s' % ( lfn, seName, result['Value']['Failed'][lfn] ) )
          failedReplicas.add( lfn )
          continue
        metadata = result['Value']['Successful'][lfn]
        if metadata['Lost']:
          error = "PFN has been Lost by the StorageElement"
        elif metadata['Unavailable']:
          error = "PFN is declared Unavailable by the StorageElement"
        elif seName in tapeSEs and not metadata['Cached']:
          error = "PFN is no longer in StorageElement Cache"
        else:
          error = ''
        if error:
          self.log.error( error, lfn )
          failedReplicas.add( lfn )
          continue

        self.log.info( 'Preliminary checks OK, download %s from %s:' % ( lfn, seName ) )
        result = self.__downloadPFN( lfn, seName, reps, guid )
        if not result['OK']:
          self.log.error( 'Download from %s failed:' % seName, result['Message'] )
      else:
        result = {'OK':False}

      if not result['OK']:
        reps.pop( seName, None )
        # Check the other SEs
        if reps:
          self.log.info( 'Trying to download from any SE' )
          result = self.__downloadLFN( lfn, reps, guid )
          if not result['OK']:
            self.log.error( 'Download from any SE failed', result['Message'] )
            failedReplicas.add( lfn )
        else:
          failedReplicas.add( lfn )
      else:
        localSECount += 1
      if result['OK']:
        # Rename file if downloaded FileName does not match the LFN... How can this happen?
        lfnName = os.path.basename( lfn )
        oldPath = result['Value']['path']
        fileName = os.path.basename( oldPath )
        if lfnName != fileName:
          newPath = os.path.join( os.path.dirname( oldPath ), lfnName )
          os.rename( oldPath, newPath )
          result['Value']['path'] = newPath
        resolvedData[lfn] = result['Value']

    # Report datasets that could not be downloaded
    report = ''
    if failedReplicas:
      self.log.warn( 'The following LFN(s) could not be downloaded to the WN:\n%s' % 'n'.join( sorted( failedReplicas ) ) )

    if resolvedData:
      report = 'Successfully downloaded LFN(s):\n'
      report += '\n'.join( sorted( resolvedData ) )
      report += '\nDownloaded %d / %d files from local Storage Elements on first attempt.' % ( localSECount, len( resolvedData ) )
      self.__setJobParam( COMPONENT_NAME, report )

    result = S_OK()
    result.update( {'Successful': resolvedData, 'Failed':sorted( failedReplicas )} )
    return result

  #############################################################################
  def __checkDiskSpace( self, totalSize ):
    """Compare available disk space to the file size reported from the catalog
       result.
    """
    diskSpace = getDiskSpace( self.__getDownloadDir( False ) )  # MB
    availableBytes = diskSpace * 1024 * 1024  # bytes
    # below can be a configuration option sent via the job wrapper in the future
    # Moved from 3 to 5 GB (PhC 130822) for standard output file
    data = 5 * 1024 * 1024 * 1024  # 5GB in bytes
    if ( data + totalSize ) < availableBytes:
      msg = 'Enough disk space available (%s bytes)' % ( availableBytes )
      self.log.verbose( msg )
      return S_OK( msg )
    else:
      msg = 'Not enough disk space available for download %s (including 3GB buffer) > %s bytes' \
             % ( ( data + totalSize ), availableBytes )
      self.log.warn( msg )
      return S_ERROR( msg )

  def __getDownloadDir( self, incrementCounter = True ):
    if self.inputDataDirectory == "PerFile":
      if incrementCounter:
        self.counter += 1
      return tempfile.mkdtemp( prefix = 'InputData_%s' % ( self.counter ), dir = os.getcwd() )
    elif self.inputDataDirectory == "CWD":
      return os.getcwd()
    else:
      return self.inputDataDirectory

  #############################################################################
  def __downloadLFN( self, lfn, reps, guid ):
    """ Download a local copy of a single LFN from a list of Storage Elements.
        This is used as a last resort to attempt to retrieve the file.
    """
    downloadDir = self.__getDownloadDir()
    diskSEs = set()
    tapeSEs = set()
    for seName in reps:
      seStatus = self.__storageElement( seName ).getStatus()['Value']
      if seStatus['Read'] and seStatus['DiskSE']:
        diskSEs.add( seName )
      elif seStatus['Read'] and seStatus['TapeSE']:
        tapeSEs.add( seName )

    for seName in list( diskSEs ) + list( tapeSEs ):
      if seName in tapeSEs:
        # Check if file is cached
        result = self.__storageElement( seName ).getFileMetadata( lfn )
        if not result['OK'] or not result.get( 'Value', {} ).get( 'Successful', {} ).get( 'Cached', False ):
          continue
      result = self.__downloadPFN( lfn, seName, reps, guid )
      if result['OK'] and lfn in result['Value']['Successful']:
        return result
    return S_ERROR( 'Unable to download the file from any SE' )

  #############################################################################
  def __downloadPFN( self, lfn, seName, reps, guid ):
    """ Download a local copy of a single PFN from the specified Storage Element.
    """
    if not lfn:
      return S_ERROR( 'Assume file is not at this site' )

    downloadDir = self.__getDownloadDir()
    fileName = os.path.basename( lfn )
    for localFile in ( os.path.join( os.getcwd(), fileName ), os.path.join( downloadDir, fileName ) ):
      if os.path.exists( localFile ):
        self.log.info( 'File %s already exists locally as %s' % ( fileName, localFile ) )
        fileDict = { 'turl':'LocalData',
                     'protocol':'LocalData',
                     'se':seName,
                     'pfn':reps[seName],
                     'guid':guid,
                     'path': localFile}
        return S_OK( fileDict )


    result = self.__storageElement( seName ).getFile( lfn, localPath = downloadDir )
    if not result['OK']:
      self.log.warn( 'Problem getting %s at %s:\n%s' % ( lfn, seName, result['Message'] ) )
      return result
    if lfn in result['Value']['Failed']:
      self.log.warn( 'Problem getting %s at %s:\n%s' % ( lfn, seName, result['Value']['Failed'][lfn] ) )
      return S_ERROR( result['Value']['Failed'][lfn] )

    if os.path.exists( localFile ):
      self.log.verbose( 'File %s exists in download directory' % ( fileName ) )
      fileDict = {'turl':'Downloaded', 'protocol':'Downloaded', 'se':seName, 'pfn':reps[seName], 'guid':guid, 'path':localFile}
      return S_OK( fileDict )
    else:
      self.log.warn( 'File does not exist in local directory after download' )
      return S_ERROR( 'OK download result but file missing in current directory' )

  #############################################################################
  def __setJobParam( self, name, value ):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_ERROR( 'JobID not defined' )

    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate', timeout = 120 )
    jobParam = jobReport.setJobParameter( int( self.jobID ), str( name ), str( value ) )
    self.log.verbose( 'setJobParameter(%s,%s,%s)' % ( self.jobID, name, value ) )
    if not jobParam['OK']:
      self.log.warn( jobParam['Message'] )

    return jobParam

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
