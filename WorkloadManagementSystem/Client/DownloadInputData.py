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
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
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
    self.inputDataDirectory = 'PerFile'
    if argumentsDict.has_key( 'InputDataDirectory' ):
      self.inputDataDirectory = argumentsDict['InputDataDirectory']
    self.jobID = None
    self.replicaManager = ReplicaManager()
    self.counter = 1

  #############################################################################
  def execute( self, dataToResolve = None ):
    """This method is called to download the requested files in the case where
       enough local disk space is available.  A buffer is left in this calculation
       to leave room for any produced files.
    """

    #Define local configuration options present at every site
    localSEList = self.configuration['LocalSEList']

    if self.configuration.has_key( 'JobID' ):
      self.jobID = self.configuration['JobID']

    #Problematic files will be returned and can be handled by another module
    failedReplicas = []

    if dataToResolve:
      self.log.verbose( 'Data to resolve passed directly to DownloadInputData module' )
      self.inputData = dataToResolve #e.g. list supplied by another module

    self.inputData = [x.replace( 'LFN:', '' ) for x in self.inputData]
    self.log.info( 'InputData to be downloaded is:' )
    for i in self.inputData:
      self.log.verbose( i )

    replicas = self.fileCatalogResult['Value']['Successful']

    #For the unlikely case that a file is found on two SEs at the same site
    #disk-based replicas are favoured.
    downloadReplicas = {}
    # determine Disk and Tape SEs
    diskSEs = []
    tapeSEs = []
    for localSE in localSEList:
      seStatus = StorageElement( localSE ).getStatus()['Value']
      if seStatus['Read'] and seStatus['DiskSE']:
        if localSE not in diskSEs:
          diskSEs.append( localSE )
      elif seStatus['Read'] and seStatus['TapeSE']:
        if localSE not in tapeSEs:
          tapeSEs.append( localSE )

    for lfn, reps in replicas.items():
      if lfn not in self.inputData:
        self.log.verbose( 'LFN %s is not in requested input data to download' )
        failedReplicas.append( lfn )
        continue

      if not ( 'Size' in reps and 'GUID' in reps ):
        self.log.error( 'Missing LFN metadata', "%s %s" % ( lfn, str( reps ) ) )
        failedReplicas.append( lfn )
        continue

      size = reps['Size']
      guid = reps['GUID' ]
      downloadReplicas[lfn] = {'SE':[], 'Size':size, 'GUID':guid}
      for se in diskSEs:
        if se in reps:
          downloadReplicas[lfn]['SE'].append( ( se, reps[se] ) )
      if not downloadReplicas[lfn]['SE']:
        for se in tapeSEs:
          if se in reps:
            downloadReplicas[lfn]['SE'].append( ( se, reps[se] ) )

    totalSize = 0
    self.log.verbose( 'Replicas to download are:' )
    for lfn, reps in downloadReplicas.items():
      self.log.verbose( lfn )
      if not reps['SE']:
        self.log.info( 'Failed to find data at local SEs, will try to download from anywhere', lfn )
        reps['SE'] = ''
        reps['PFN'] = ''
      else:
        if len( reps['SE'] ) > 1:
          # if more than one SE is available randomly select one
          random.shuffle( reps['SE'] )
        reps['PFN'] = reps['SE'][0][1]
        reps['SE'] = reps['SE'][0][0]
      for item, value in sorted( reps.items() ):
        if value:
          self.log.verbose( '%s %s' % ( item, value ) )
        if item == 'Size':
          totalSize += int( value ) #bytes

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
      result = S_OK()
      result['Failed'] = self.inputData
      result['Successful'] = {}
      return result

    resolvedData = {}
    localSECount = 0
    for lfn in downloadReplicas.keys():
      result = self.__getPFN( downloadReplicas[lfn]['PFN'],
                              downloadReplicas[lfn]['SE'],
                              downloadReplicas[lfn]['GUID'] )
      if not result['OK']:
        self.log.warn( 'Download of file from localSE failed with message:\n%s' % ( result ) )
        result = self.__getLFN( lfn, downloadReplicas[lfn]['PFN'],
                                downloadReplicas[lfn]['SE'],
                                downloadReplicas[lfn]['GUID'] )
        if not result['OK']:
          self.log.warn( 'Download of file from any SE failed with message:\n%s' % ( result ) )
          failedReplicas.append( lfn )
      else:
        localSECount += 1
      if result['OK']:
        # Rename file if downloaded FileName does not match the LFN
        lfnName = os.path.basename( lfn )
        oldPath = result['Value']['path']
        fileName = os.path.basename( oldPath )
        if lfnName != fileName:
          newPath = os.path.join( os.path.dirname( oldPath, lfnName ) )
          os.rename( oldPath, newPath )
          result['Value']['path'] = newPath
        resolvedData[lfn] = result['Value']

    #Report datasets that could not be downloaded
    report = ''
    if failedReplicas:
      report = 'The following LFN(s) could not be downloaded to the WN:\n'
      for lfn in failedReplicas:
        report += '%s\n' % ( lfn )
        self.log.warn( report )

    if resolvedData:
      report = 'Successfully downloaded LFN(s):\n'
      for lfn, reps in resolvedData.items():
        report += '%s\n' % ( lfn )
      totalLFNs = len( resolvedData.keys() )
      report += '\nDownloaded %s / %s files from local Storage Elements on first attempt.' % ( localSECount, totalLFNs )
      self.__setJobParam( COMPONENT_NAME, report )

    result = S_OK()
    result['Successful'] = resolvedData
    result['Failed'] = failedReplicas #lfn list to be passed to another resolution mechanism
    return result

  #############################################################################
  def __checkDiskSpace( self, totalSize ):
    """Compare available disk space to the file size reported from the catalog
       result.
    """
    diskSpace = getDiskSpace() #MB
    availableBytes = diskSpace * 1024 * 1024 #bytes
    #below can be a configuration option sent via the job wrapper in the future
    data = 3 * 1024 * 1024 * 1024 # 3GB in bytes
    if ( data + totalSize ) < availableBytes:
      msg = 'Enough disk space available (%s bytes)' % ( availableBytes )
      self.log.verbose( msg )
      return S_OK( msg )
    else:
      msg = 'Not enough disk space available for download %s (including 3GB buffer) > %s bytes' \
             % ( ( buffer + totalSize ), availableBytes )
      self.log.warn( msg )
      return S_ERROR( msg )

  #############################################################################
  def __getLFN( self, lfn, pfn, se, guid ):
    """ Download a local copy of a single LFN from the specified Storage Element.
        This is used as a last resort to attempt to retrieve the file.  The Replica
        Manager will perform an LFC lookup to refresh the stored result.
    """
    start = os.getcwd()
    if self.inputDataDirectory == "PerFile":
      downloadDir = tempfile.mkdtemp( prefix = 'InputData_%s' % ( self.counter ), dir = start )
    elif self.inputDataDirectory == "CWD":
      downloadDir = start
    else:
      downloadDir = self.inputDataDirectory
    self.counter += 1
    os.chdir( downloadDir )
    self.log.verbose( 'Attempting to ReplicaManager.getFile for %s in %s' % ( lfn, downloadDir ) )
    result = self.replicaManager.getFile( lfn )
    os.chdir( start )
    if not result['OK']:
      return result
    self.log.verbose( result )
    if lfn in result['Value']['Failed']:
      return S_ERROR( 'Download failed with error %s' % result['Value']['Failed'][lfn] )
    if lfn not in result['Value']['Successful']:
      return S_ERROR( 'Donwload failed' )
    fileName = os.path.basename( result['Value']['Successful'][lfn] )
    localFile = os.path.join( downloadDir, fileName )
    if os.path.exists( localFile ):
      self.log.verbose( 'File %s exists in current directory' % ( fileName ) )
      fileDict = {'turl':'Downloaded', 'protocol':'Downloaded', 'se':se, 'pfn':pfn, 'guid':guid, 'path':localFile}
      return S_OK( fileDict )
    else:
      self.log.warn( 'File does not exist in local directory after download' )
      return S_ERROR( 'OK download result but file missing in current directory' )

  #############################################################################
  def __getPFN( self, pfn, se, guid ):
    """ Download a local copy of a single PFN from the specified Storage Element.
    """
    if not pfn:
      return S_ERROR( 'Assume file is not at this site' )

    fileName = os.path.basename( pfn )
    if os.path.exists( fileName ):
      self.log.verbose( 'File already %s exists in current directory' % ( fileName ) )
      fileDict = { 'turl':'LocalData',
                   'protocol':'LocalData',
                   'se':se,
                   'pfn':pfn,
                   'guid':guid,
                   'path': os.path.join( os.getcwd(), fileName )}
      return S_OK( fileDict )

    start = os.getcwd()
    if self.inputDataDirectory == "PerFile":
      downloadDir = tempfile.mkdtemp( prefix = 'InputData_%s' % ( self.counter ), dir = start )
    elif self.inputDataDirectory == "CWD":
      downloadDir = start
    else:
      downloadDir = self.inputDataDirectory
    self.counter += 1

    result = self.replicaManager.getStorageFile( pfn, se, localPath = downloadDir, singleFile = True )
    if not result['OK']:
      self.log.warn( 'Problem getting PFN %s:\n%s' % ( pfn, result ) )
      return result
    self.log.verbose( result )

    localFile = os.path.join( downloadDir, fileName )
    if os.path.exists( localFile ):
      self.log.verbose( 'File %s exists in current directory' % ( fileName ) )
      fileDict = {'turl':'Downloaded', 'protocol':'Downloaded', 'se':se, 'pfn':pfn, 'guid':guid, 'path':localFile}
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

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
