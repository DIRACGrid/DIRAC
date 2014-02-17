########################################################################
# $HeadURL$
# File :    InputDataByProtocol.py
# Author :  Stuart Paterson
########################################################################

""" The Input Data By Protocol module wraps around the Replica Management
    components to provide access to datasets by available site protocols as
    defined in the CS for the VO.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.Resources.Storage.StorageElement                         import StorageElement
from DIRAC                                                          import S_OK, S_ERROR, gLogger

COMPONENT_NAME = 'InputDataByProtocol'

class InputDataByProtocol:

  #############################################################################
  def __init__( self, argumentsDict ):
    """ Standard constructor
    """
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )
    self.inputData = argumentsDict['InputData']
    self.configuration = argumentsDict['Configuration']
    self.fileCatalogResult = argumentsDict['FileCatalog']
    self.jobID = None

  def __storagelement( self, seName ):
    return self.storageElements.setdefault( seName, StorageElement( seName ) )

  #############################################################################
  def execute( self, dataToResolve = None ):
    """This method is called to obtain the TURLs for all requested input data
       firstly by available site protocols and redundantly via TURL construction.
       If TURLs are missing these are conveyed in the result to
    """

    # Define local configuration options present at every site
    localSEList = self.configuration['LocalSEList']
    self.jobID = self.configuration.get( 'JobID' )

    if dataToResolve:
      self.log.verbose( 'Data to resolve passed directly to InputDataByProtocol module' )
      self.inputData = dataToResolve  # e.g. list supplied by another module

    self.inputData = [x.replace( 'LFN:', '' ) for x in self.inputData]
    self.log.verbose( 'InputData requirement to be resolved by protocol is:\n%s' % '\n', join( self.inputData ) )

    # First make a check in case replicas have been removed or are not accessible
    # from the local site (remove these from consideration for local protocols)
    replicas = self.fileCatalogResult['Value']['Successful']
    self.log.verbose( 'File Catalogue result is:\n%s' % str( replicas ) )

    diskSEs = set()
    tapeSEs = set()
    for localSE in localSEList:
      seStatus = self.__storageElement( localSE ).getStatus()['Value']
      if seStatus['Read'] and seStatus['DiskSE']:
        diskSEs.add( localSE )
      elif seStatus['Read'] and seStatus['TapeSE']:
        tapeSEs.add( localSE )

    # For the unlikely case that a file is found on two SEs at the same site
    # disk-based replicas are favoured.
    # Problematic files will be returned and can be handled by another module
    failedReplicas = set()
    newReplicasDict = {}
    for lfn, reps in replicas.items():
      if lfn in self.inputData:
        # Check that all replicas are on a valid local SE
        if not [se for se in reps if se in diskSEs + tapeSEs]:
          failedReplicas.add( lfn )
          continue
        for seName in diskSEs & set( reps ):
          newReplicasDict.setdefault( lfn, [] ).append( seName )
        if not newReplicasDict[lfn]:
          for seName in tapeSEs & set( reps ):
            newReplicasDict.setdefault( lfn, [] ).append( seName )

    # Check that all LFNs have at least one replica and GUID
    if failedReplicas:
      # in principle this is not a failure but depends on the policy of the VO
      # datasets could be downloaded from another site
      self.log.info( 'The following file(s) were found not to have replicas for available LocalSEs:\n%s' % '\n'.join( sorted( failedReplicas ) ) )

    # Need to group files by SE in order to stage optimally
    # we know from above that all remaining files have a replica
    # (preferring disk if >1) in the local storage.
    # IMPORTANT, only add replicas for input data that is requested
    # since this module could have been executed after another.
    seFilesDict = {}
    for lfn, seList in newReplicasDict.items():
      for seName in seList:
        seFilesDict.setdefault( seName, [] ).append( lfn )

    sortedSEs = sorted( [ ( len( lfns ), seName ) for seName, lfns in seFilesDict.items() ], reverse = True )

    trackLFNs = {}
    for _len, seName in sortedSEs:
      for lfn in seFilesDict[seName]:
        if lfn not in trackLFNs:
          if 'Size' in replicas[lfn] and 'GUID' in replicas[lfn]:
            trackLFNs[lfn] = { 'pfn': replicas.get( lfn, {} ).get( seName, lfn ), 'se': seName, 'size': replicas[lfn]['Size'], 'guid': replicas[lfn]['GUID'] }
        else:
          # Remove the lfn from those SEs with less lfns
          del seFilesDict[seName][lfn]

    self.log.verbose( 'Files grouped by LocalSE are:\n%s' % str( seFilesDict ) )
    for seName, lfns in seFilesDict.items():
      self.log.info( ' %s LFNs found from catalog at LocalSE %s\n%s' % ( len( lfns ), seName, '\n'.join( lfns ) ) )

    # Can now start to obtain TURLs for files grouped by localSE
    # for requested input data
    requestedProtocol = self.configuration.get( 'Protocol', '' )
    for seName, lfns in seFilesDict.items():
      if not lfns:
        continue
      result = self.__storageElement( seName ).getFileMetadata( lfns )
      if not result['OK']:
        self.log.error( "Error getting metadata.", result['Message'] + ':\n%s' % '\n'.join( lfns ) )
        # If we can not get MetaData, most likely there is a problem with the SE
        # declare the replicas failed and continue
        failedReplicas.update( lfns )
        continue
      failed = result['Value']['Failed']
      if failed:
        # If MetaData can not be retrieved for some PFNs
        # declared them failed and go on
        for lfn in failed:
          if type( failed ) == type( {} ):
            self.log.error( failed( lfn ), lfn )
          failedReplicas.add( lfn )
      for lfn, metadata in result['Value']['Successful'].items():
        if metadata['Lost']:
          error = "File has been Lost by the StorageElement %s" % seName
        elif metadata['Unavailable']:
          error = "File is declared Unavailable by the StorageElement %s" % seName
        elif seName in tapeSEs and not metadata['Cached']:
          error = "File is no longer in StorageElement %s Cache" % seName
        else:
          error = ''
        if error:
          lfns.remove( lfn )
          self.log.error( error, lfn )
          # If PFN is not available
          # declared it failed and go on
          failedReplicas.add( lfn )

      if None in failedReplicas:
        failedReplicas.remove( None )
      if not failedReplicas:
        self.log.info( 'Preliminary checks OK, getting TURLS for at %s:\n' % seName, '\n'.join( lfns ) )
      else:
        self.log.warn( "Errors during preliminary checks for %d files" % len( failedReplicas ) )

      result = self.__storageElement( seName ).getAccessUrl( lfns, protocol = requestedProtocol )
      if not result['OK']:
        self.log.error( "Error getting TURLs", result['Message'] )
        return result

      badTURLCount = 0
      badTURLs = []
      seResult = result['Value']

      for lfn, cause in seResult['Failed'].items():
        badTURLCount += 1
        badTURLs.append( 'Failed to obtain TURL for %s: %s' % ( lfn, cause ) )
        failedReplicas.add( lfn )

      if badTURLCount:
        self.log.warn( 'Found %s problematic TURL(s) for job %s' % ( badTURLCount, self.jobID ) )
        param = '\n'.join( badTURLs )
        self.log.info( param )
        result = self.__setJobParam( 'ProblematicTURLs', param )
        if not result['OK']:
          self.log.warn( "Error setting job param", result['Message'] )

      for lfn, turl in seResult['Successful'].items():
        trackLFNs[lfn]['turl'] = turl
        seName = trackLFNs[lfn]['se']
        self.log.info( 'Resolved input data\n>>>> SE: %s\n>>>>LFN: %s\n>>>>TURL: %s' %
                       ( seName, lfn, turl ) )


    self.log.debug( trackLFNs )
    for lfn, mdata in trackLFNs.items():
      if 'turl' not in mdata:
        self.log.verbose( 'No TURL resolved for %s' % lfn )

    # Remove any failed replicas from the resolvedData dictionary
    if failedReplicas:
      self.log.verbose( 'The following LFN(s) were not resolved by protocol:\n%s' % ( '\n'.join( sorted( failedReplicas ) ) ) )
      for lfn in failedReplicas:
        trackLFNs.pop( lfn, None )

    result = S_OK()
    result['Successful'] = trackLFNs
    result['Failed'] = sorted( failedReplicas )  # lfn list to be passed to another resolution mechanism
    return result

  #############################################################################
  def __setJobParam( self, name, value ):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_ERROR( 'JobID not defined' )

    self.log.verbose( 'setJobParameter(%s, %s, %s)' % ( self.jobID, name, value ) )
    return RPCClient( 'WorkloadManagement/JobStateUpdate', timeout = 120 ).setJobParameter( int( self.jobID ), str( name ), str( value ) )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
