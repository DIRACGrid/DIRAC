"""
This is the Data Integrity Client which allows the simple reporting of
problematic file and replicas to the IntegrityDB and their status
correctly updated in the FileCatalog.
"""

import re

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.DataManagementSystem.Client.DataManager        import DataManager
from DIRAC.Resources.Storage.StorageElement               import StorageElement
from DIRAC.Resources.Catalog.FileCatalog                  import FileCatalog
from DIRAC.Core.Utilities.ReturnValues                    import returnSingleResult
from DIRAC.Core.Base.Client                               import Client

__RCSID__ = "$Id$"


class DataIntegrityClient( Client ):

  """
  The following methods are supported in the service but are not mentioned explicitly here:

          getProblematic()
             Obtains a problematic file from the IntegrityDB based on the LastUpdate time

          getPrognosisProblematics(prognosis)
            Obtains all the problematics of a particular prognosis from the integrityDB

          getProblematicsSummary()
            Obtains a count of the number of problematics for each prognosis found

          getDistinctPrognosis()
            Obtains the distinct prognosis found in the integrityDB

          getTransformationProblematics(prodID)
            Obtains the problematics for a given production

          incrementProblematicRetry(fileID)
            Increments the retry count for the supplied file ID

          changeProblematicPrognosis(fileID,newPrognosis)
            Changes the prognosis of the supplied file to the new prognosis

          setProblematicStatus(fileID,status)
            Updates the status of a problematic in the integrityDB

          removeProblematic(self,fileID)
            This removes the specified file ID from the integrity DB

          insertProblematic(sourceComponent,fileMetadata)
            Inserts file with supplied metadata into the integrity DB

  """

  def __init__( self, **kwargs ):

    super(DataIntegrityClient, self).__init__( **kwargs )
    self.setServer( 'DataManagement/DataIntegrity' )
    self.dm = DataManager()
    self.fc = FileCatalog()

  def setFileProblematic( self, lfn, reason, sourceComponent = '' ):
    """ This method updates the status of the file in the FileCatalog and the IntegrityDB

        lfn - the lfn of the file
        reason - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
    """
    if isinstance( lfn, list ):
      lfns = lfn
    elif isinstance( lfn, basestring ):
      lfns = [lfn]
    else:
      errStr = "DataIntegrityClient.setFileProblematic: Supplied file info must be list or a single LFN."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    gLogger.info( "DataIntegrityClient.setFileProblematic: Attempting to update %s files." % len( lfns ) )
    fileMetadata = {}
    for lfn in lfns:
      fileMetadata[lfn] = {'Prognosis':reason, 'LFN':lfn, 'PFN':'', 'SE':''}
    res = self.insertProblematic( sourceComponent, fileMetadata )
    if not res['OK']:
      gLogger.error( "DataIntegrityClient.setReplicaProblematic: Failed to insert problematics to integrity DB" )
    return res

  def reportProblematicReplicas( self, replicaTuple, se, reason ):
    """ Simple wrapper function around setReplicaProblematic """
    gLogger.info( 'The following %s files had %s at %s' % ( len( replicaTuple ), reason, se ) )
    for lfn, _pfn, se, reason in sorted( replicaTuple ):
      if lfn:
        gLogger.info( lfn )
    res = self.setReplicaProblematic( replicaTuple, sourceComponent = 'DataIntegrityClient' )
    if not res['OK']:
      gLogger.info( 'Failed to update integrity DB with replicas', res['Message'] )
    else:
      gLogger.info( 'Successfully updated integrity DB with replicas' )

  def setReplicaProblematic( self, replicaTuple, sourceComponent = '' ):
    """ This method updates the status of the replica in the FileCatalog and the IntegrityDB
        The supplied replicaDict should be of the form {lfn :{'PFN':pfn,'SE':se,'Prognosis':prognosis}

        lfn - the lfn of the file
        pfn - the pfn if available (otherwise '')
        se - the storage element of the problematic replica (otherwise '')
        prognosis - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
    """
    if isinstance( replicaTuple, tuple ):
      replicaTuple = [replicaTuple]
    elif isinstance( replicaTuple, list ):
      pass
    else:
      errStr = "DataIntegrityClient.setReplicaProblematic: Supplied replica info must be a tuple or list of tuples."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    gLogger.info( "DataIntegrityClient.setReplicaProblematic: Attempting to update %s replicas." % len( replicaTuple ) )
    replicaDict = {}
    for lfn, pfn, se, reason in replicaTuple:
      replicaDict[lfn] = {'Prognosis':reason, 'LFN':lfn, 'PFN':pfn, 'SE':se}
    res = self.insertProblematic( sourceComponent, replicaDict )
    if not res['OK']:
      gLogger.error( "DataIntegrityClient.setReplicaProblematic: Failed to insert problematic to integrity DB" )
      return res
    for lfn in replicaDict.keys():
      replicaDict[lfn]['Status'] = 'Problematic'

    res = self.fc.setReplicaStatus( replicaDict )
    if not res['OK']:
      errStr = "DataIntegrityClient.setReplicaProblematic: Completely failed to update replicas."
      gLogger.error( errStr, res['Message'] )
      return res
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  ##########################################################################
  #
  # This section contains the resolution methods for various prognoses
  #

  def __updateCompletedFiles( self, prognosis, fileID ):
    gLogger.info( "%s file (%d) is resolved" % ( prognosis, fileID ) )
    return self.setProblematicStatus( fileID, 'Resolved' )

  def __returnProblematicError( self, fileID, res ):
    self.incrementProblematicRetry( fileID )
    gLogger.error( 'DataIntegrityClient failure', res['Message'] )
    return res

  def __updateReplicaToChecked( self, problematicDict ):
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']
    prognosis = problematicDict['Prognosis']
    problematicDict['Status'] = 'Checked'

    res = returnSingleResult( self.fc.setReplicaStatus( {lfn:problematicDict} ) )

    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    gLogger.info( "%s replica (%d) is updated to Checked status" % ( prognosis, fileID ) )
    return self.__updateCompletedFiles( prognosis, fileID )

  def resolveCatalogPFNSizeMismatch( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the CatalogPFNSizeMismatch prognosis
    """
    lfn = problematicDict['LFN']
    se = problematicDict['SE']
    fileID = problematicDict['FileID']


    res = returnSingleResult( self.fc.getFileSize( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    catalogSize = res['Value']
    res = returnSingleResult( StorageElement( se ).getFileSize( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    storageSize = res['Value']
    bkKCatalog = FileCatalog( ['BookkeepingDB'] )
    res = returnSingleResult( bkKCatalog.getFileSize( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    bookkeepingSize = res['Value']
    if bookkeepingSize == catalogSize == storageSize:
      gLogger.info( "CatalogPFNSizeMismatch replica (%d) matched all registered sizes." % fileID )
      return self.__updateReplicaToChecked( problematicDict )
    if catalogSize == bookkeepingSize:
      gLogger.info( "CatalogPFNSizeMismatch replica (%d) found to mismatch the bookkeeping also" % fileID )
      res = returnSingleResult( self.fc.getReplicas( lfn ) )
      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      if len( res['Value'] ) <= 1:
        gLogger.info( "CatalogPFNSizeMismatch replica (%d) has no other replicas." % fileID )
        return S_ERROR( "Not removing catalog file mismatch since the only replica" )
      else:
        gLogger.info( "CatalogPFNSizeMismatch replica (%d) has other replicas. Removing..." % fileID )
        res = self.dm.removeReplica( se, lfn )
        if not res['OK']:
          return self.__returnProblematicError( fileID, res )
        return self.__updateCompletedFiles( 'CatalogPFNSizeMismatch', fileID )
    if ( catalogSize != bookkeepingSize ) and ( bookkeepingSize == storageSize ):
      gLogger.info( "CatalogPFNSizeMismatch replica (%d) found to match the bookkeeping size" % fileID )
      res = self.__updateReplicaToChecked( problematicDict )
      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      return self.changeProblematicPrognosis( fileID, 'BKCatalogSizeMismatch' )
    gLogger.info( "CatalogPFNSizeMismatch replica (%d) all sizes found mismatch. Updating retry count" % fileID )
    return self.incrementProblematicRetry( fileID )

  #FIXME: Unused?
  def resolvePFNNotRegistered( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNNotRegistered prognosis
    """
    lfn = problematicDict['LFN']
    seName = problematicDict['SE']
    fileID = problematicDict['FileID']

    se = StorageElement( seName )
    res = returnSingleResult( self.fc.exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if not res['Value']:
      # The file does not exist in the catalog
      res = returnSingleResult( se.removeFile( lfn ) )
      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      return self.__updateCompletedFiles( 'PFNNotRegistered', fileID )
    res = returnSingleResult( se.getFileMetadata( lfn ) )
    if ( not res['OK'] ) and ( re.search( 'File does not exist', res['Message'] ) ):
      gLogger.info( "PFNNotRegistered replica (%d) found to be missing." % fileID )
      return self.__updateCompletedFiles( 'PFNNotRegistered', fileID )
    elif not res['OK']:
      return self.__returnProblematicError( fileID, res )
    storageMetadata = res['Value']
    if storageMetadata['Lost']:
      gLogger.info( "PFNNotRegistered replica (%d) found to be Lost. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'PFNLost' )
    if storageMetadata['Unavailable']:
      gLogger.info( "PFNNotRegistered replica (%d) found to be Unavailable. Updating retry count" % fileID )
      return self.incrementProblematicRetry( fileID )

    # HACK until we can obtain the space token descriptions through GFAL
    site = seName.split( '_' )[0].split( '-' )[0]
    if not storageMetadata['Cached']:
      if lfn.endswith( '.raw' ):
        seName = '%s-RAW' % site
      else:
        seName = '%s-RDST' % site
    elif storageMetadata['Migrated']:
      if lfn.startswith( '/lhcb/data' ):
        seName = '%s_M-DST' % site
      else:
        seName = '%s_MC_M-DST' % site
    else:
      if lfn.startswith( '/lhcb/data' ):
        seName = '%s-DST' % site
      else:
        seName = '%s_MC-DST' % site

    problematicDict['SE'] = seName
    res = returnSingleResult( se.getURL( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )

    problematicDict['PFN'] = res['Value']

    res = returnSingleResult( self.fc.addReplica( {lfn:problematicDict} ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    res = returnSingleResult( self.fc.getFileMetadata( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if res['Value']['Size'] != storageMetadata['Size']:
      gLogger.info( "PFNNotRegistered replica (%d) found with catalog size mismatch. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'CatalogPFNSizeMismatch' )
    return self.__updateCompletedFiles( 'PFNNotRegistered', fileID )

  #FIXME: Unused?
  def resolveLFNCatalogMissing( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the LFNCatalogMissing prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = returnSingleResult( self.fc.exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if res['Value']:
      return self.__updateCompletedFiles( 'LFNCatalogMissing', fileID )
    # Remove the file from all catalogs
    # RF_NOTE : here I can do it because it's a single file, but otherwise I would need to sort the path
    res = returnSingleResult( self.fc.removeFile( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    return self.__updateCompletedFiles( 'LFNCatalogMissing', fileID )

  #FIXME: Unused?
  def resolvePFNMissing( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNMissing prognosis
    """
    se = problematicDict['SE']
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = returnSingleResult( self.fc.exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if not res['Value']:
      gLogger.info( "PFNMissing file (%d) no longer exists in catalog" % fileID )
      return self.__updateCompletedFiles( 'PFNMissing', fileID )

    res = returnSingleResult( StorageElement( se ).exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if res['Value']:
      gLogger.info( "PFNMissing replica (%d) is no longer missing" % fileID )
      return self.__updateReplicaToChecked( problematicDict )
    gLogger.info( "PFNMissing replica (%d) does not exist" % fileID )
    res = returnSingleResult( self.fc.getReplicas( lfn, allStatus = True ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    replicas = res['Value']
    seSite = se.split( '_' )[0].split( '-' )[0]
    found = False
    print replicas
    for replicaSE in replicas.keys():
      if re.search( seSite, replicaSE ):
        found = True
        problematicDict['SE'] = replicaSE
        se = replicaSE
    if not found:
      gLogger.info( "PFNMissing replica (%d) is no longer registered at SE. Resolved." % fileID )
      return self.__updateCompletedFiles( 'PFNMissing', fileID )
    gLogger.info( "PFNMissing replica (%d) does not exist. Removing from catalog..." % fileID )
    res = returnSingleResult( self.fc.removeReplica( {lfn:problematicDict} ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if len( replicas ) == 1:
      gLogger.info( "PFNMissing replica (%d) had a single replica. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'LFNZeroReplicas' )
    res = self.dm.replicateAndRegister( problematicDict['LFN'], se )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    # If we get here the problem is solved so we can update the integrityDB
    return self.__updateCompletedFiles( 'PFNMissing', fileID )

  #FIXME: Unused?
  def resolvePFNUnavailable( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the PFNUnavailable prognosis
    """
    lfn = problematicDict['LFN']
    se = problematicDict['SE']
    fileID = problematicDict['FileID']

    res = returnSingleResult( StorageElement( se ).getFileMetadata( lfn ) )
    if ( not res['OK'] ) and ( re.search( 'File does not exist', res['Message'] ) ):
      # The file is no longer Unavailable but has now dissapeared completely
      gLogger.info( "PFNUnavailable replica (%d) found to be missing. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'PFNMissing' )
    if ( not res['OK'] ) or res['Value']['Unavailable']:
      gLogger.info( "PFNUnavailable replica (%d) found to still be Unavailable" % fileID )
      return self.incrementProblematicRetry( fileID )
    if res['Value']['Lost']:
      gLogger.info( "PFNUnavailable replica (%d) is now found to be Lost. Updating prognosis" % fileID )
      return self.changeProblematicPrognosis( fileID, 'PFNLost' )
    gLogger.info( "PFNUnavailable replica (%d) is no longer Unavailable" % fileID )
    # Need to make the replica okay in the Catalog
    return self.__updateReplicaToChecked( problematicDict )

  #FIXME: Unused?
  def resolvePFNZeroSize( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolves the PFNZeroSize prognosis
    """
    lfn = problematicDict['LFN']
    seName = problematicDict['SE']
    fileID = problematicDict['FileID']

    se = StorageElement( seName )

    res = returnSingleResult( se.getFileSize( lfn ) )
    if ( not res['OK'] ) and ( re.search( 'File does not exist', res['Message'] ) ):
      gLogger.info( "PFNZeroSize replica (%d) found to be missing. Updating prognosis" % problematicDict['FileID'] )
      return self.changeProblematicPrognosis( fileID, 'PFNMissing' )
    storageSize = res['Value']
    if storageSize == 0:
      res = returnSingleResult( se.removeFile( lfn ) )

      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      gLogger.info( "PFNZeroSize replica (%d) removed. Updating prognosis" % problematicDict['FileID'] )
      return self.changeProblematicPrognosis( fileID, 'PFNMissing' )


    res = returnSingleResult( self.fc.getReplicas( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if seName not in res['Value']:
      gLogger.info( "PFNZeroSize replica (%d) not registered in catalog. Updating prognosis" % problematicDict['FileID'] )
      return self.changeProblematicPrognosis( fileID, 'PFNNotRegistered' )
    res = returnSingleResult( self.fc.getFileMetadata( lfn ) )

    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    catalogSize = res['Value']['Size']
    if catalogSize != storageSize:
      gLogger.info( "PFNZeroSize replica (%d) size found to differ from registered metadata. Updating prognosis" % problematicDict['FileID'] )
      return self.changeProblematicPrognosis( fileID, 'CatalogPFNSizeMismatch' )
    return self.__updateCompletedFiles( 'PFNZeroSize', fileID )

  ############################################################################################

  #FIXME: Unused?
  def resolveLFNZeroReplicas( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolves the LFNZeroReplicas prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = returnSingleResult( self.fc.getReplicas( lfn, allStatus = True ) )
    if res['OK'] and res['Value']:
      gLogger.info( "LFNZeroReplicas file (%d) found to have replicas" % fileID )
    else:
      gLogger.info( "LFNZeroReplicas file (%d) does not have replicas. Checking storage..." % fileID )
      pfnsFound = False
      for storageElementName in sorted( gConfig.getValue( 'Resources/StorageElementGroups/Tier1_MC_M-DST', [] ) ):
        res = self.__getStoragePathExists( [lfn], storageElementName )
        if lfn in res['Value']:
          gLogger.info( "LFNZeroReplicas file (%d) found storage file at %s" % ( fileID, storageElementName ) )
          self.reportProblematicReplicas( [( lfn, 'deprecatedUrl', storageElementName, 'PFNNotRegistered' )], storageElementName, 'PFNNotRegistered' )
          pfnsFound = True
      if not pfnsFound:
        gLogger.info( "LFNZeroReplicas file (%d) did not have storage files. Removing..." % fileID )
        res = returnSingleResult( self.fc.removeFile( lfn ) )
        if not res['OK']:
          gLogger.error( 'DataIntegrityClient: failed to remove file', res['Message'] )
          # Increment the number of retries for this file
          self.server.incrementProblematicRetry( fileID )
          return res
        gLogger.info( "LFNZeroReplicas file (%d) removed from catalog" % fileID )
    # If we get here the problem is solved so we can update the integrityDB
    return self.__updateCompletedFiles( 'LFNZeroReplicas', fileID )


  def _reportProblematicFiles( self, lfns, reason ):
    """ Simple wrapper function around setFileProblematic
    """
    gLogger.info( 'The following %s files were found with %s' % ( len( lfns ), reason ) )
    for lfn in sorted( lfns ):
      gLogger.info( lfn )
    res = self.setFileProblematic( lfns, reason, sourceComponent = 'DataIntegrityClient' )
    if not res['OK']:
      gLogger.info( 'Failed to update integrity DB with files', res['Message'] )
    else:
      gLogger.info( 'Successfully updated integrity DB with files' )
