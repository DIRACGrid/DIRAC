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

  #############################################################################
  def execute( self, dataToResolve = None ):
    """This method is called to obtain the TURLs for all requested input data
       firstly by available site protocols and redundantly via TURL construction.
       If TURLs are missing these are conveyed in the result to
    """

    #Define local configuration options present at every site
    localSEList = self.configuration['LocalSEList']
    if self.configuration.has_key( 'JobID' ):
      self.jobID = self.configuration['JobID']

    #Problematic files will be returned and can be handled by another module
    failedReplicas = []

    if dataToResolve:
      self.log.verbose( 'Data to resolve passed directly to InputDataByProtocol module' )
      self.inputData = dataToResolve #e.g. list supplied by another module

    self.inputData = [x.replace( 'LFN:', '' ) for x in self.inputData]
    self.log.info( 'InputData requirement to be resolved by protocol is:' )
    for i in self.inputData:
      self.log.verbose( i )

    #First make a check in case replicas have been removed or are not accessible
    #from the local site (remove these from consideration for local protocols)
    replicas = self.fileCatalogResult['Value']['Successful']
    self.log.verbose( 'File Catalogue result is:' )
    self.log.verbose( replicas )

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

    # Check that all replicas are on a valid local SE
    for lfn, reps in replicas.items():
      localReplica = False
      for seName in reps:
        if seName in diskSEs or seName in tapeSEs:
          localReplica = True
      if not localReplica:
        failedReplicas.append( lfn )

    # Check that all LFNs have at least one replica and GUID
    if failedReplicas:
      # in principle this is not a failure but depends on the policy of the VO
      # datasets could be downloaded from another site
      self.log.info( 'The following file(s) were found not to have replicas for available LocalSEs:' )
      self.log.info( '', '\n'.join( failedReplicas ) )

    #For the unlikely case that a file is found on two SEs at the same site
    #disk-based replicas are favoured.
    newReplicasDict = {}
    for lfn, reps in replicas.items():
      newReplicasDict[lfn] = []
      for seName in diskSEs:
        if seName in reps:
          newReplicasDict[lfn].append( ( seName, reps[seName] ) )
      if not newReplicasDict[lfn]:
        for seName in tapeSEs:
          if seName in reps:
            newReplicasDict[lfn].append( ( seName, reps[seName] ) )

    #Need to group files by SE in order to stage optimally
    #we know from above that all remaining files have a replica
    #(preferring disk if >1) in the local storage.
    #IMPORTANT, only add replicas for input data that is requested
    #since this module could have been executed after another.
    seFilesDict = {}
    for lfn, seList in newReplicasDict.items():
      if lfn not in self.inputData:
        continue
      for seName, pfn in seList:
        if seName not in seFilesDict:
          seFilesDict[seName] = {}
        seFilesDict[seName][lfn] = pfn

    sortedSEs = sorted( [ ( len( lfns ), seName ) for seName, lfns in seFilesDict.items() ] )

    trackLFNs = {}
    for lfns, seName in reversed( sortedSEs ):
      for lfn, pfn in seFilesDict[seName].items():
        if lfn not in trackLFNs:
          if 'Size' in replicas[lfn] and 'GUID' in replicas[lfn]:
            trackLFNs[lfn] = { 'pfn': pfn, 'se': seName, 'size': replicas[lfn]['Size'], 'guid': replicas[lfn]['GUID'] }
        else:
          # Remove the lfn from those SEs with less lfns
          del seFilesDict[seName][lfn]

    self.log.verbose( 'Files grouped by LocalSE are:' )
    self.log.verbose( seFilesDict )
    for seName, pfnList in seFilesDict.items():
      seTotal = len( pfnList )
      self.log.info( ' %s SURLs found from catalog for LocalSE %s' % ( seTotal, seName ) )
      for pfn in pfnList:
        self.log.info( '%s %s' % ( seName, pfn ) )

    #Can now start to obtain TURLs for files grouped by localSE
    #for requested input data
    requestedProtocol = self.configuration.get( 'Protocol', '' )
    for seName, lfnDict in seFilesDict.items():
      pfnList = lfnDict.values()
      if not pfnList:
        continue
      result = StorageElement( seName ).getFileMetadata( pfnList )
      if not result['OK']:
        self.log.warn( result['Message'] )
        # If we can not get MetaData, most likely there is a problem with the SE
        # declare the replicas failed and continue
        failedReplicas.extend( lfnDict.keys() )
        continue
      if result['Value']['Failed']:
        error = 'Could not get Storage Metadata from %s' % seName
        self.log.error( error )
        # If MetaData can not be retrieved for some PFNs 
        # declared them failed and go on
        for lfn in lfnDict:
          pfn = lfnDict[lfn]
          if pfn in result['Value']['Failed']:
            failedReplicas.append( lfn )
            pfnList.remove( pfn )
      for pfn, metadata in result['Value']['Successful'].items():
        if metadata['Lost']:
          error = "PFN has been Lost by the StorageElement"
          self.log.error( error , pfn )
          # If PFN has been lost 
          # declared it failed and go on
          for lfn in lfnDict:
            if pfn == lfnDict[lfn]:
              failedReplicas.append( lfn )
              pfnList.remove( pfn )
        elif metadata['Unavailable']:
          error = "PFN is declared Unavailable by the StorageElement"
          self.log.error( error, pfn )
          # If PFN is not available
          # declared it failed and go on
          for lfn in lfnDict:
            if pfn == lfnDict[lfn]:
              failedReplicas.append( lfn )
              pfnList.remove( pfn )
        elif seName in tapeSEs and not metadata['Cached']:
          error = "PFN is no longer in StorageElement Cache"
          self.log.error( error, pfn )
          # If PFN is not in the disk Cache
          # declared it failed and go on
          for lfn in lfnDict:
            if pfn == lfnDict[lfn]:
              failedReplicas.append( lfn )
              pfnList.remove( pfn )

      self.log.info( 'Preliminary checks OK, getting TURLS:\n', '\n'.join( pfnList ) )

      result = StorageElement( seName ).getAccessUrl( pfnList, protocol = requestedProtocol )
      self.log.debug( result )
      if not result['OK']:
        self.log.warn( result['Message'] )
        return result

      badTURLCount = 0
      badTURLs = []
      seResult = result['Value']

      if seResult.has_key( 'Failed' ):
        for pfn, cause in seResult['Failed'].items():
          badTURLCount += 1
          badTURLs.append( 'Failed to obtain TURL for %s\n Problem: %s' % ( pfn, cause ) )
          for lfn in lfnDict:
            if lfnDict[lfn] == pfn:
              break
          if not lfn in failedReplicas:
            failedReplicas.append( lfn )

      if badTURLCount:
        self.log.warn( 'Found %s problematic TURL(s) for job %s' % ( badTURLCount, self.jobID ) )
        param = '\n'.join( badTURLs )
        self.log.info( param )
        result = self.__setJobParam( 'ProblematicTURLs', param )
        if not result['OK']:
          self.log.warn( result )

      pfnTurlDict = seResult['Successful']
      for pfn, turl in pfnTurlDict.items():
        for lfn in lfnDict:
          if lfnDict[lfn] == pfn:
            break
        trackLFNs[lfn]['turl'] = turl
        seName = trackLFNs[lfn]['se']
        self.log.info( 'Resolved input data\n>>>> SE: %s\n>>>>LFN: %s\n>>>>PFN: %s\n>>>>TURL: %s' %
                       ( seName, lfn, pfn, turl ) )


    self.log.verbose( trackLFNs )
    for lfn, mdata in trackLFNs.items():
      if not mdata.has_key( 'turl' ):
        self.log.verbose( '%s: No TURL resolved for %s' % ( COMPONENT_NAME, lfn ) )

    #Remove any failed replicas from the resolvedData dictionary
    if failedReplicas:
      self.log.verbose( 'The following LFN(s) were not resolved by protocol:\n%s' % ( '\n'.join( failedReplicas ) ) )
      for lfn in failedReplicas:
        if trackLFNs.has_key( lfn ):
          del trackLFNs[lfn]

    result = S_OK()
    result['Successful'] = trackLFNs
    result['Failed'] = failedReplicas #lfn list to be passed to another resolution mechanism
    return result

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
