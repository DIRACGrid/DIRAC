""" Class for the LCG File Catalog Client

"""
import DIRAC
from DIRAC                                                    import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers                 import getVO
from DIRAC.Resources.Catalog.FileCatalogueBase                import FileCatalogueBase
from DIRAC.Core.Utilities.Time                                import fromEpoch
from DIRAC.Core.Utilities.List                                import sortList, breakListIntoChunks
from DIRAC.Core.Security.ProxyInfo                            import getProxyInfo, formatProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Registry        import getDNForUsername, getVOMSAttributeForGroup
from stat import *
import os, re, types, time

lfc = None
importedLFC = None

class LcgFileCatalogClient( FileCatalogueBase ):

  def __init__( self, infosys = None, host = None ):
    global lfc, importedLFC

    FileCatalogueBase.__init__( self, 'LFC' )

    if importedLFC == None:
      try:
        import lfcthr as lfc
        # This is necessary to make the LFC client thread safe.
        lfc.init()
        importedLFC = True
        gLogger.debug( "LcgFileCatalogClient.__init__: Successfully imported lfc module." )

      except ImportError:
        importedLFC = False
        gLogger.exception( "LcgFileCatalogClient.__init__: Failed to import lfc module." )

    self.valid = importedLFC

    if not infosys:
      # if not provided, take if from CS
      infosys = gConfig.getValue( '/Resources/FileCatalogs/LcgFileCatalog/LcgGfalInfosys', '' )
    if not infosys and 'LCG_GFAL_INFOSYS' in os.environ:
      # if not in CS take from environ
      infosys = os.environ['LCG_GFAL_INFOSYS']

    if not host:
      # if not provided, take if from CS
      host = gConfig.getValue( '/Resources/FileCatalogs/LcgFileCatalog/MasterHost', '' )
    if not host and 'LFC_HOST' in os.environ:
      # if not in CS take from environ
      host = os.environ['LFC_HOST']

    self.host = host
    result = gConfig.getOption( '/DIRAC/Setup' )
    if not result['OK']:
      gLogger.fatal( 'Failed to get the /DIRAC/Setup' )
      return

    if host:
      os.environ['LFC_HOST'] = host
    if infosys:
      os.environ['LCG_GFAL_INFOSYS'] = infosys

    if not 'LFC_CONRETRYINT' in os.environ:
      os.environ['LFC_CONRETRYINT'] = '5'
    if not 'LFC_CONNTIMEOUT' in os.environ:
      os.environ['LFC_CONNTIMEOUT'] = '5'
    if not 'LFC_CONRETRY' in os.environ:
      os.environ['LFC_CONRETRY'] = '5'

    self.prefix = '/grid'
    self.session = False
    self.transaction = False

  ####################################################################
  #
  # These are the get/set methods for use within the client
  #

  def isOK( self ):
    return self.valid

  def getName( self ):
    return S_OK( self.name )

  ####################################################################
  #
  # These are the methods for session/transaction manipulation
  #

  def __openSession( self ):
    """Open the LFC client/server session"""
    if self.session:
      return False
    else:
      sessionName = 'DIRAC_%s.%s at %s at time %s' % ( DIRAC.majorVersion,
                                                       DIRAC.minorVersion,
                                                       DIRAC.siteName(),
                                                       time.time() )
      lfc.lfc_startsess( self.host, sessionName )
      self.session = True
      return True

  def __closeSession( self ):
    """Close the LFC client/server session"""
    if self.session:
      lfc.lfc_endsess()
      self.session = False

  def __startTransaction( self ):
    """ Begin transaction for one time commit """
    if not self.transaction:
      transactionName = 'Transaction: DIRAC_%s.%s at %s at time %s' % ( DIRAC.majorVersion,
                                                                        DIRAC.minorVersion,
                                                                        DIRAC.siteName(),
                                                                        time.time() )
      lfc.lfc_starttrans( self.host, transactionName )
      self.transaction = True

  def __abortTransaction( self ):
    """ Abort transaction """
    if self.transaction:
      lfc.lfc_aborttrans()
      self.transaction = False

  def __endTransaction( self ):
    """ End transaction gracefully """
    if self.transaction:
      lfc.lfc_endtrans()
      self.transaction = False

  def setAuthorizationId( self, dn ):
    """ Set authorization id for the proxy-less LFC communication """
    lfc.lfc_client_setAuthorizationId( 0, 0, 'GSI', dn )

  ####################################################################
  #
  # The following are read methods for paths
  #

  def exists( self, path ):
    """ Check if the path exists
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    for lfn, guid in lfns.items():
      res = self.__existsLfn( lfn )
      if not res['OK']:
        failed[lfn] = res['Message']
      elif res['Value']:
        successful[lfn] = lfn
      elif not guid:
        successful[lfn] = False
      else:
        res = self.__existsGuid( guid )
        if not res['OK']:
          failed[lfn] = res['Message']
        elif not res['Value']:
          successful[lfn] = False
        else:
          successful[lfn] = self.__getLfnForGUID( guid )['Value']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __getClientCertInfo( self ):
    res = getProxyInfo( False, False )
    if not res['OK']:
      gLogger.error( "ReplicaManager.__getClientCertGroup: Failed to get client proxy information.",
                     res['Message'] )
      return res
    proxyInfo = res['Value']
    gLogger.debug( formatProxyInfoAsString( proxyInfo ) )
    if not proxyInfo.has_key( 'group' ):
      errStr = "ReplicaManager.__getClientCertGroup: Proxy information does not contain the group."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    if not proxyInfo.has_key( 'VOMS' ):
      proxyInfo['VOMS'] = getVOMSAttributeForGroup( proxyInfo['group'] )
      errStr = "ReplicaManager.__getClientCertGroup: Proxy information does not contain the VOMs information."
      gLogger.warn( errStr )
    res = getDNForUsername( proxyInfo['username'] )
    if not res['OK']:
      errStr = "ReplicaManager.__getClientCertGroup: Error getting known proxies for user."
      gLogger.error( errStr, res['Message'] )
      return S_ERROR( errStr )
    resDict = { 'DN'       : proxyInfo['identity'],
                'Role'     : proxyInfo['VOMS'],
                'User'     : proxyInfo['username'],
                'AllDNs'   : res['Value']
              }
    return S_OK( resDict )

  def __getPathAccess( self, path ):
    """ Determine the permissions using the lfc function lfc_access """
    fullLfn = '%s%s' % ( self.prefix, path )
    permDict = { 'Read': 1,
                 'Write': 2,
                 'Execute': 4}
    resDict = {}
    for p in permDict.keys():

      code = permDict[ p ]
      value = lfc.lfc_access( fullLfn, code )
      if value == 0:
        resDict[ p ] = True
      else:
        resDict[ p ] = False
    return S_OK( resDict )

  def getPathPermissions( self, path ):
    """ Determine the VOMs based ACL information for a supplied path
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__getBasePath( path )
      if not res['OK']:
        failed[path] = res['Message']
      else:
        basePath = res['Value']
        res = self.__getPathAccess( basePath )
        if not res['OK']:
          failed[path] = res['Message']
        else:
          LFCPerm = res['Value']
          res = self.__getACLInformation( basePath )
          if not res['OK']:
            failed[path] = res['Message']
          else:
          # Evaluate access rights
            val = res['Value']
            try:
              LFCPerm['user'] = val['user']
              LFCPerm['group'] = val['group']
              LFCPerm['world'] = val['world']
              LFCPerm['DN'] = val['DN']
              LFCPerm['Role'] = val['Role']
            except KeyError:
              print 'key not found: __getACLInformation returned incomplete dictionary', KeyError
              failed[path] = LFCPerm
              continue
          successful[path] = LFCPerm

    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # The following are read methods for files
  #

  def isFile( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat( lfn )
      if not res['OK']:
        failed[lfn] = res['Message']
      elif S_ISREG( res['Value'].filemode ):
        successful[lfn] = True
      else:
        successful[lfn] = False
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getFileMetadata( self, lfn, ownership = False ):
    """ Returns the file metadata associated to a supplied LFN
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat( lfn )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        fstat = res['Value']
        successful[lfn] = {}
        successful[lfn]['Size'] = fstat.filesize
        successful[lfn]['CheckSumType'] = fstat.csumtype
        successful[lfn]['Checksum'] = fstat.csumvalue
        successful[lfn]['GUID'] = fstat.guid
        successful[lfn]['Status'] = fstat.status
        successful[lfn]['CreationDate'] = fromEpoch( fstat.ctime )
        successful[lfn]['ModificationDate'] = fromEpoch( fstat.mtime )
        successful[lfn]['NumberOfLinks'] = fstat.nlink
        successful[lfn]['Mode'] = S_IMODE( fstat.filemode )
        if ownership:
          res = self.__getDNFromUID( fstat.uid )
          if res['OK']:
            successful[lfn]['OwnerDN'] = res['Value']
          else:
            successful[lfn]['OwnerDN'] = None
          res = self.__getRoleFromGID( fstat.gid )
          if res['OK']:
            successful[lfn]['OwnerRole'] = res['Value']
          else:
            successful[lfn]['OwnerRole'] = None
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getFileSize( self, lfn ):
    """ Get the size of a supplied file
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len( lfns ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat( lfn )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value'].filesize
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )


  def getReplicas( self, lfn, allStatus = False ):
    """ Returns replicas for an LFN or list of LFNs
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    lfnChunks = breakListIntoChunks( lfns.keys(), 1000 )
    # If we have less than three groups to query a session doesn't make sense
    created = False
    if len( lfnChunks ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for lfnList in lfnChunks:
      fullLfnList = []
      for lfn in lfnList:
        fullLfn = '%s%s' % ( self.prefix, lfn )
        fullLfnList.append ( fullLfn )
      value, replicaList = lfc.lfc_getreplicasl( fullLfnList, '' )
      if value != 0:
        for lfn in lfnList:
          failed[lfn] = lfc.sstrerror( lfc.cvar.serrno )
        continue
      guid = ''
      it = iter( lfnList )
      replicas = {}
      for oReplica in replicaList:
        if oReplica.errcode != 0:
          if ( oReplica.guid == '' ) or ( oReplica.guid != guid ):
            if len( replicas ):
              successful[lfn] = replicas
              replicas = {}
            lfn = it.next()
            failed[lfn] = lfc.sstrerror( oReplica.errcode )
            guid = oReplica.guid
        elif oReplica.sfn == '':
          if len( replicas ):
            successful[lfn] = replicas
            replicas = {}
          lfn = it.next()
          failed[lfn] = 'File has zero replicas'
          guid = oReplica.guid
        else:
          if ( oReplica.guid != guid ):
            if len( replicas ):
              successful[lfn] = replicas
              replicas = {}
            lfn = it.next()
            guid = oReplica.guid
          if ( oReplica.status != 'P' ) or allStatus:
            se = oReplica.host
            pfn = oReplica.sfn#.strip()
            replicas[se] = pfn
      if len( replicas ):
        successful[lfn] = replicas
    if created: self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getReplicaStatus( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len( lfns ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for lfn, se in lfns.items():
      res = self.__getFileReplicaStatus( lfn, se )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = res['Value']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getLFNForPFN( self, pfn ):
    res = self.__checkArgumentFormat( pfn )
    if not res['OK']:
      return res
    pfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    for pfn in pfns:
      res = self.__getLFNForPFN( pfn )
      if not res['OK']:
        failed[pfn] = res['Message']
      else:
        successful[pfn] = res['Value']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # The following a read methods for directories
  #

  def isDirectory( self, lfn ):
    """ Determine whether the path is a directory
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len( lfns ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat( lfn )
      if not res['OK']:
        failed[lfn] = res['Message']
      elif S_ISDIR( res['Value'].filemode ):
        successful[lfn] = True
      else:
        successful[lfn] = False
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getDirectoryMetadata( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = self.__openSession()
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      res = self.__getPathStat( lfn )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        fstat = res['Value']
        successful[lfn] = {}
        successful[lfn]['Size'] = fstat.filesize
        successful[lfn]['CheckSumType'] = fstat.csumtype
        successful[lfn]['Checksum'] = fstat.csumvalue
        successful[lfn]['GUID'] = fstat.guid
        successful[lfn]['Status'] = fstat.status
        successful[lfn]['CreationDate'] = fromEpoch( fstat.ctime )
        successful[lfn]['ModificationDate'] = fromEpoch( fstat.mtime )
        successful[lfn]['NumberOfSubPaths'] = fstat.nlink
        res = self.__getDNFromUID( fstat.uid )
        if res['OK']:
          successful[lfn]['OwnerDN'] = res['Value']
        else:
          successful[lfn]['OwnerDN'] = None
        res = self.__getRoleFromGID( fstat.gid )
        if res['OK']:
          successful[lfn]['OwnerRole'] = res['Value']
        else:
          successful[lfn]['OwnerRole'] = None
        successful[lfn]['Mode'] = S_IMODE( fstat.filemode )
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getDirectoryReplicas( self, lfn, allStatus = False ):
    """ This method gets all of the pfns in the directory
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__getDirectoryContents( path )
      if not res['OK']:
        failed[path] = res['Message']
      else:
        pathReplicas = {}
        files = res['Value']['Files']
        for lfn, fileDict in files.items():
          pathReplicas[lfn] = {}
          for se, seDict in fileDict['Replicas'].items():
            pfn = seDict['PFN']
            status = seDict['Status']
            if ( status != 'P' ) or allStatus:
              pathReplicas[lfn][se] = pfn
        successful[path] = pathReplicas
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def listDirectory( self, lfn, verbose = False ):
    """ Returns the result of __getDirectoryContents for multiple supplied paths
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__getDirectoryContents( path, verbose )
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def getDirectorySize( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__getDirectorySize( path )
      if res['OK']:
        successful[path] = res['Value']
      else:
        failed[path] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # The following are read methods for links
  #

  def isLink( self, link ):
    res = self.__checkArgumentFormat( link )
    if not res['OK']:
      return res
    links = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len( links ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    self.__openSession()
    for link in links.keys():
      res = self.__getLinkStat( link )
      if not res['OK']:
        failed[link] = res['Message']
      elif S_ISLNK( res['Value'].filemode ):
        successful[link] = True
      else:
        successful[link] = False
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def readLink( self, link ):
    res = self.__checkArgumentFormat( link )
    if not res['OK']:
      return res
    links = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len( links ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for link in links.keys():
      res = self.__readLink( link )
      if res['OK']:
        successful[link] = res['Value']
      else:
        failed[link] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # The following are read methods for datasets
  #

  def resolveDataset( self, dataset, allStatus = False ):
    res = self.__checkArgumentFormat( dataset )
    if not res['OK']:
      return res
    datasets = res['Value']
    created = self.__openSession()
    successful = {}
    failed = {}
    for datasetName in datasets.keys():
      res = self.__getDirectoryContents( datasetName )
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        #linkDict = res['Value']['Links']
        linkDict = res['Value']['Files']
        datasetFiles = {}
        for link, fileMetadata in linkDict.items():
          #target = fileMetadata[link]['MetaData']['Target']
          target = link
          datasetFiles[target] = fileMetadata['Replicas']
        successful[datasetName] = datasetFiles
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # The following a write methods for files
  #

  def addFile( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    baseDirs = []
    for lfn in lfns.keys():
      baseDir = os.path.dirname( lfn )
      if baseDir in baseDirs:
        continue
      baseDirs.append( baseDir )
      res = self.__executeOperation( baseDir, 'exists' )
      # If we failed to find out whether the directory exists
      if not res['OK']:
        continue
      # If the directory exists
      if res['Value']:
        continue
      #Make the directories recursively if needed
      res = self.__makeDirs( baseDir )
      # If we failed to make the directory for the file
      if not res['OK']:
        continue
    lfc.lfc_umask( 0000 )
    for lfnList in breakListIntoChunks( sortList( lfns.keys() ), 1000 ):
      fileChunk = []
      for lfn in list( lfnList ):
        lfnInfo = lfns[lfn]
        pfn = lfnInfo['PFN']
        size = lfnInfo['Size']
        se = lfnInfo['SE']
        guid = lfnInfo['GUID']
        checksum = lfnInfo['Checksum']
        res = self.__checkAddFile( lfn, pfn, size, se, guid, checksum )
        if not res['OK']:
          failed[lfn] = res['Message']
          lfnList.remove( lfn )
        elif not res['Value']:
          successful[lfn] = True
          lfnList.remove( lfn )
        else:
          oFile = lfc.lfc_filereg()
          oFile.lfn = "%s/%s" % ( self.prefix, lfn )
          oFile.sfn = pfn
          oFile.size = size
          oFile.mode = 0664
          oFile.server = se
          oFile.guid = guid
          oFile.csumtype = 'AD'
          oFile.status = 'U'
          oFile.csumvalue = lfnInfo['Checksum']
          fileChunk.append( oFile )
      if not fileChunk:
        continue
      value, errCodes = lfc.lfc_registerfiles( fileChunk )
      if ( value != 0 ) or ( len( errCodes ) != len( lfnList ) ):
        for lfn in lfnList:
          failed[lfn] = lfc.sstrerror( lfc.cvar.serrno )
        continue
      for index in range( len( errCodes ) ):
        lfn = lfnList[index]
        errCode = errCodes[index]
        if errCode == 0:
          successful[lfn] = True
        elif errCode == 17:
          failed[lfn] = "The supplied GUID is already used"
          res = self.__getLfnForGUID( guid )
          if res['OK']:
            failed[lfn] = "The supplied GUID is already used by %s" % res['Value']
        else:
          failed[lfn] = lfc.sstrerror( errCode )
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def addReplica( self, lfn ):
    """ This adds a replica to the catalogue.
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      pfn = info['PFN']
      se = info['SE']
      if not info.has_key( 'Master' ):
        master = False
      else:
        master = info['Master']
      res = self.__getLFNGuid( lfn )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        guid = res['Value']
        res = self.__addReplica( guid, pfn, se, master )
        if res['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeFile( self, lfn ):
    """ Remove the supplied path
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    res = self.exists( lfns )
    if not res['OK']:
      if created:
        self.__closeSession()
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn, exists in res['Value']['Successful'].items():
      if not exists:
        successful[lfn] = True
      else:
        res = self.__unlinkPath( lfn )
        if res['OK']:
          successful[lfn] = True
        else:
          failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeReplica( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = False
    if len( lfns ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      if ( not info.has_key( 'PFN' ) ) or ( not info.has_key( 'SE' ) ):
        failed[lfn] = "Required parameters not supplied"
      else:
        pfn = info['PFN']
        se = info['SE']
        res = self.__removeReplica( pfn )
        if res['OK']:
          successful[lfn] = True
        else:
          if res['Message'] == 'No such file or directory':
            # The PFN didn't exist, but maybe it wsa changed...
            res1 = self.getReplicas( lfn )
            if res1['OK']:
              pfn1 = res1['Value']['Successful'].get( lfn, {} ).get( se )
              if pfn1 and pfn1 != pfn:
                res = self.__removeReplica( pfn1 )
          if res['OK']:
            successful[lfn] = True
          else:
            failed[lfn] = res['Message']
    lfnRemoved = successful.keys()
    if len( lfnRemoved ) > 0:
      res = self.getReplicas( lfnRemoved, True )
      zeroReplicaFiles = []
      if not res['OK']:
        if created:
          self.__closeSession()
        return res
      else:
        for lfn, repDict in res['Value']['Successful'].items():
          if len( repDict.keys() ) == 0:
            zeroReplicaFiles.append( lfn )
      if len( zeroReplicaFiles ) > 0:
        res = self.removeFile( zeroReplicaFiles )
        if not res['OK']:
          if created:
            self.__closeSession()
          return res
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def setReplicaStatus( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = False
    if len( lfns ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      pfn = info['PFN']
      se = info['SE']
      status = info['Status']
      res = self.__setReplicaStatus( pfn, status[0] )
      if res['OK']:
        successful[lfn] = True
      else:
        failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def setReplicaHost( self, lfn ):
    """ This modifies the replica metadata for the SE.
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = False
    if len( lfns ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      pfn = info['PFN']
      se = info['SE']
      newse = info['NewSE']
      res = self.__modReplica( pfn, newse )
      if res['OK']:
        successful[lfn] = True
      else:
        failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # The following are write methods for directories
  #

  def removeDirectory( self, lfn, recursive = False ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    res = self.exists( lfns )
    if not res['OK']:
      if created:
        self.__closeSession()
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn, exists in res['Value']['Successful'].items():
      if not exists:
        successful[lfn] = True
        continue
      if recursive:
        res = self.__removeDirs( lfn )
      else:
        res = self.__removeDirectory( lfn )
      if res['OK']:
        successful[lfn] = True
      else:
        failed[lfn] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def createDirectory( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    created = self.__openSession()
    failed = {}
    successful = {}
    for path in lfns.keys():
      res = self.__makeDirs( path )
      if res['OK']:
        successful[path] = True
      else:
        failed[path] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # The following are write methods for links
  #

  def createLink( self, link ):
    res = self.__checkArgumentFormat( link )
    if not res['OK']:
      return res
    links = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = self.__openSession()
    failed = {}
    successful = {}
    for link, target in links.items():
      res = self.__makeDirs( os.path.dirname( link ) )
      if not res['OK']:
        failed[link] = res['Message']
      else:
        res = self.__makeLink( link, target )
        if not res['OK']:
          failed[link] = res['Message']
        else:
          successful[link] = target
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeLink( self, link ):
    res = self.__checkArgumentFormat( link )
    if not res['OK']:
      return res
    links = res['Value']
    # If we have less than three lfns to query a session doesn't make sense
    created = False
    if len( links ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for link in links.keys():
      res = self.__unlinkPath( link )
      if not res['OK']:
        failed[link] = res['Message']
      else:
        successful[link] = True
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # The following are write methods for datasets
  #

  def createDataset( self, dataset ):
    res = self.__checkArgumentFormat( dataset )
    if not res['OK']:
      return res
    datasets = res['Value']
    created = self.__openSession()
    successful = {}
    failed = {}
    for datasetName, lfns in datasets.items():
      res = self.__executeOperation( datasetName, 'exists' )
      if not res['OK']:
        return res
      elif res['Value']:
        return S_ERROR( "LcgFileCatalogClient.createDataset: This dataset already exists." )
      res = self.__createDataset( datasetName, lfns )
      if res['OK']:
        successful[datasetName] = True
      else:
        self.__executeOperation( datasetName, 'removeDataset' )
        failed[datasetName] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeDataset( self, dataset ):
    res = self.__checkArgumentFormat( dataset )
    if not res['OK']:
      return res
    datasets = res['Value']
    created = self.__openSession()
    successful = {}
    failed = {}
    for datasetName in datasets.keys():
      res = self.__removeDataset( datasetName )
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        successful[datasetName] = True
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeFileFromDataset( self, dataset ):
    res = self.__checkArgumentFormat( dataset )
    if not res['OK']:
      return res
    datasets = res['Value']
    created = self.__openSession()
    successful = {}
    failed = {}
    for datasetName, lfns in datasets.items():
      res = self.__removeFilesFromDataset( datasetName, lfns )
      if not res['OK']:
        failed[datasetName] = res['Message']
      else:
        successful[datasetName] = True
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # These are the internal methods to be used by all methods
  #

  def __checkArgumentFormat( self, path ):
    if type( path ) in types.StringTypes:
      urls = {path:False}
    elif type( path ) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type( path ) == types.DictType:
      urls = path
    else:
      return S_ERROR( "LcgFileCatalogClient.__checkArgumentFormat: Supplied path is not of the correct format." )
    return S_OK( urls )

  def __executeOperation( self, path, method ):
    """ Executes the requested functionality with the supplied path
    """
    fcn = None
    if hasattr( self, method ) and callable( getattr( self, method ) ):
      fcn = getattr( self, method )
    if not fcn:
      return S_ERROR( "Unable to invoke %s, it isn't a member function of LcgFileCatalogClient" % method )
    res = fcn( path )
    if type( path ) == types.DictType:
      path = path.keys()[0]
    if not res['OK']:
      return res
    elif path not in res['Value']['Successful']:
      return S_ERROR( res['Value']['Failed'][path] )
    return S_OK( res['Value']['Successful'][path] )

  def __getLFNForPFN( self, pfn ):
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statr( pfn, fstat )
    if value != 0:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )
    return self.__getLfnForGUID( fstat.guid )

  def __existsLfn( self, lfn ):
    """ Check whether the supplied LFN exists
    """
    fullLfn = '%s%s' % ( self.prefix, lfn )
    value = lfc.lfc_access( fullLfn, 0 )
    if value == 0:
      return S_OK( True )
    else:
      errno = lfc.cvar.serrno
      if errno == 2:
        return S_OK( False )
      else:
        return S_ERROR( lfc.sstrerror( errno ) )

  def __existsGuid( self, guid ):
    """ Check if the guid exists
    """
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg( '', guid, fstat )
    if value == 0:
      return S_OK( True )
    else:
      errno = lfc.cvar.serrno
      if errno == 2:
        return S_OK( False )
      else:
        return S_ERROR( lfc.sstrerror( errno ) )

  def __getLfnForGUID( self, guid ):
    """ Resolve the LFN for a supplied GUID
    """
    linkList = lfc.lfc_list()
    lfnlist = []
    listlinks = lfc.lfc_listlinks( '', guid, lfc.CNS_LIST_BEGIN, linkList )
    while listlinks:
      ll = listlinks.path
      if re.search ( '^' + self.prefix, ll ):
        ll = listlinks.path.replace( self.prefix, "", 1 )
      lfnlist.append( ll )
      listlinks = lfc.lfc_listlinks( '', guid, lfc.CNS_LIST_CONTINUE, linkList )
    else:
      lfc.lfc_listlinks( '', guid, lfc.CNS_LIST_END, linkList )
    return S_OK( lfnlist[0] )

  def __getBasePath( self, path ):
    exists = False
    while not exists:
      res = self.__executeOperation( path, 'exists' )
      if not res['OK']:
        return res
      else:
        exists = res['Value']
        if not exists:
          path = os.path.dirname( path )
    return S_OK( path )

  def __getACLInformation( self, path ):
    fullLfn = '%s%s' % ( self.prefix, path )
    results, objects = lfc.lfc_getacl( fullLfn, 256 )#lfc.CNS_ACL_GROUP_OBJ)
    if results == -1:
      errStr = "LcgFileCatalogClient.__getACLInformation: Failed to obtain all path ACLs."
      gLogger.error( errStr, "%s %s" % ( path, lfc.sstrerror( lfc.cvar.serrno ) ) )
      return S_ERROR( errStr )
    permissionsDict = {}
    for obj in objects:
      if obj.a_type == lfc.CNS_ACL_USER_OBJ:
        res = self.__getDNFromUID( obj.a_id )
        if not res['OK']:
          return res
        permissionsDict['DN'] = res['Value']
        permissionsDict['user'] = obj.a_perm
      elif obj.a_type == lfc.CNS_ACL_GROUP_OBJ:
        res = self.__getRoleFromGID( obj.a_id )
        if not res['OK']:
          return res
        role = res['Value']
        permissionsDict['Role'] = role
        permissionsDict['group'] = obj.a_perm
      elif obj.a_type == lfc.CNS_ACL_OTHER:
        permissionsDict['world'] = obj.a_perm
      else:
        errStr = "LcgFileCatalogClient.__getACLInformation: ACL type not considered."
        gLogger.debug( errStr, obj.a_type )
    gLogger.verbose( "LcgFileCatalogClient.__getACLInformation: %s owned by %s:%s." % ( path,
                                                                                        permissionsDict['DN'],
                                                                                        permissionsDict['Role'] ) )
    return S_OK( permissionsDict )

  def __getPathStat( self, path = '', guid = '' ):
    if path:
      path = '%s%s' % ( self.prefix, path )
    fstat = lfc.lfc_filestatg()
    value = lfc.lfc_statg( path, guid, fstat )
    if value == 0:
      return S_OK( fstat )
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __getDNFromUID( self, userID ):
    buff = " " * ( lfc.CA_MAXNAMELEN + 1 )
    res = lfc.lfc_getusrbyuid( userID, buff )
    if res == 0:
      dn = buff[:buff.find( '\x00' )]
      gLogger.debug( "LcgFileCatalogClient.__getDNFromUID: UID %s maps to %s." % ( userID, dn ) )
      return S_OK( dn )
    else:
      errStr = "LcgFileCatalogClient.__getDNFromUID: Failed to get DN from UID"
      gLogger.error( errStr, "%s %s" % ( userID, lfc.sstrerror( lfc.cvar.serrno ) ) )
      return S_ERROR( errStr )

  def __getRoleFromGID( self, groupID ):
    buff = " " * ( lfc.CA_MAXNAMELEN + 1 )
    res = lfc.lfc_getgrpbygid( groupID, buff )
    if res == 0:
      role = buff[:buff.find( '\x00' )]
      if role == 'lhcb':
        role = 'lhcb/Role=user'
      gLogger.debug( "LcgFileCatalogClient.__getRoleFromGID: GID %s maps to %s." % ( groupID, role ) )
      return S_OK( role )
    else:
      errStr = "LcgFileCatalogClient:__getRoleFromGID: Failed to get role from GID"
      gLogger.error( errStr, "%s %s" % ( groupID, lfc.sstrerror( lfc.cvar.serrno ) ) )
      return S_ERROR()

  def __getFileReplicas( self, lfn, allStatus ):
    fullLfn = '%s%s' % ( self.prefix, lfn )
    value, replicaObjects = lfc.lfc_getreplica( fullLfn, '', '' )
    if value != 0:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )
    replicas = {}
    if not replicaObjects:
      return S_ERROR( 'File has zero replicas' )
    for replica in replicaObjects:
      status = replica.status
      if ( status != 'P' ) or allStatus:
        se = replica.host
        pfn = replica.sfn#.strip()
        replicas[se] = pfn
    return S_OK( replicas )

  def __getFileReplicaStatus( self, lfn, se ):
    fullLfn = '%s%s' % ( self.prefix, lfn )
    value, replicaObjects = lfc.lfc_getreplica( fullLfn, '', '' )
    if value != 0:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )
    for replica in replicaObjects:
      if se == replica.host:
        return S_OK( replica.status )
    return S_ERROR( "No replica at supplied site" )

  def __checkAddFile( self, lfn, pfn, size, se, guid, checksum ):
    res = self.__getPathStat( lfn )
    if not res['OK']:
      if res['Message'] != 'No such file or directory':
        return S_ERROR( "Failed to find pre-existance of LFN" )
    else:
      fstat = res['Value']
      if fstat.guid != guid:
        return S_ERROR( "This LFN %s is already registered with another GUID" % lfn )
      if fstat.filesize != size:
        return S_ERROR( "This LFN %s is already registered with another size" % lfn )
      if fstat.csumvalue.upper() != checksum.upper():
        return S_ERROR( "This LFN %s is already registered with another adler32" % lfn )
      res = self.__getFileReplicas( lfn, True )
      if not res['OK']:
        return S_ERROR( "Failed to obtain replicas for existing LFN %s" % lfn )
      replicas = res['Value']
      if not ( replicas.has_key( se ) and ( replicas[se] == pfn ) ):
        return S_ERROR( "This LFN %s is already registered with another SE/PFN" % lfn )
      return S_OK( False )
    try:
      size = long( size )
    except Exception:
      return S_ERROR( "The size of the file must be an 'int','long' or 'string'" )
    if not se:
      return S_ERROR( "The SE for the file was not supplied." )
    if not pfn:
      return S_ERROR( "The PFN for the file was not supplied." )
    if not lfn:
      return S_ERROR( "The LFN for the file was not supplied." )
    if not guid:
      return S_ERROR( "The GUID for the file was not supplied." )
    if not checksum:
      return S_ERROR( "The adler32 for the file was not supplied." )
    return S_OK( True )

  def __addFile( self, lfn, pfn, size, se, guid, checksum ):
    lfc.lfc_umask( 0000 )
    bdir = os.path.dirname( lfn )
    res = self.__executeOperation( bdir, 'exists' )
    # If we failed to find out whether the directory exists
    if not res['OK']:
      return S_ERROR( res['Message'] )
    # If the directory doesn't exist
    if not res['Value']:
      #Make the directories recursively if needed
      res = self.__makeDirs( bdir )
      # If we failed to make the directory for the file
      if not res['OK']:
        return S_ERROR( res['Message'] )
    #Create a new file
    fullLfn = '%s%s' % ( self.prefix, lfn )
    value = lfc.lfc_creatg( fullLfn, guid, 0664 )
    if value != 0:
      errStr = lfc.sstrerror( lfc.cvar.serrno )
      gLogger.error( "LcgFileCatalogClient__addFile: Failed to create GUID.", errStr )
      # Remove the file we just attempted to add
      res = self.__unlinkPath( lfn )
      if not res['OK']:
        gLogger.error( "LcgFileCatalogClient.__addFile: Failed to remove file after failure.", res['Message'] )
      return S_ERROR( "LcgFileCatalogClient__addFile: Failed to create GUID: %s" % errStr )
    #Set the checksum and size of the file
    if not checksum:
      checksum = ''
    value = lfc.lfc_setfsizeg( guid, size, 'AD', checksum )
    if value != 0:
      errStr = lfc.sstrerror( lfc.cvar.serrno )
      # Remove the file we just attempted to add
      res = self.__unlinkPath( lfn )
      if not res['OK']:
        gLogger.error( "LcgFileCatalogClient.__addFile: Failed to remove file after failure to add checksum and size.",
                        res['Message'] )
      return S_ERROR( "LcgFileCatalogClient.__addFile: Failed to set file size: %s" % errStr )
    return S_OK()

  def __addReplica( self, guid, pfn, se, master ):
    fid = lfc.lfc_fileid()
    status = 'U'
    f_type = 'D'
    poolname = ''
    # setname = ''
    fs = ''
    # r_type = 'S'
    if master:
      r_type = 'P' # S = secondary, P = primary
    # value = lfc.lfc_addreplica( guid, fid, se, pfn,
    #                             status, f_type, poolname, fs, r_type, setname ) # not really useful in the end.
    value = lfc.lfc_addreplica( guid, fid, se, pfn, status, f_type, poolname, fs )
    if value == 0:
      return S_OK()
    errStr = lfc.sstrerror( lfc.cvar.serrno )
    if errStr == "File exists":
      return S_OK()
    else:
      return S_ERROR( errStr )

  def __unlinkPath( self, lfn ):
    fullLfn = '%s%s' % ( self.prefix, lfn )
    value = lfc.lfc_unlink( fullLfn )
    if value == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __removeReplica( self, pfn ):
    fid = lfc.lfc_fileid()
    value = lfc.lfc_delreplica( '', fid, pfn )
    if value == 0:
      return S_OK()
    elif value == 2:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __setReplicaStatus( self, pfn, status ):
    value = lfc.lfc_setrstatus( pfn, status )
    if value == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __modReplica( self, pfn, newse ):
    value = lfc.lfc_modreplica( pfn, '', '', newse )
    if value == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __removeDirectory( self, path ):
    fullLfn = '%s%s' % ( self.prefix, path )
    value = lfc.lfc_rmdir( fullLfn )
    if value == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __removeDirs( self, path ):
    """ Black magic contained within...
    """
    res = self.__getDirectoryContents( path )
    if not res['OK']:
      return res
    subDirs = res['Value']['SubDirs']
    files = res['Value']['Files']
    for subDir in subDirs.keys():
      res = self.__removeDirs( subDir )
      if not res['OK']:
        return res
    if files:
      return S_ERROR( "Directory not empty" )
    return self.__removeDirectory( path )

  def __makeDirs( self, path, mode = 0775 ):
    """  Black magic contained within....
    """
    dirName = os.path.dirname( path )
    res = self.__executeOperation( path, 'exists' )
    if not res['OK']:
      return res
    if res['Value']:
      return S_OK()
    res = self.__executeOperation( dirName, 'exists' )
    if not res['OK']:
      return res
    if res['Value']:
      res = self.__makeDirectory( path, mode )
    else:
      res = self.__makeDirs( dirName, mode )
      res = self.__makeDirectory( path, mode )
    return res

  def __makeDirectory( self, path, mode ):
    fullLfn = '%s%s' % ( self.prefix, path )
    lfc.lfc_umask( 0000 )
    value = lfc.lfc_mkdir( fullLfn, mode )
    if value == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __openDirectory( self, path ):
    lfcPath = "%s%s" % ( self.prefix, path )
    value = lfc.lfc_opendirg( lfcPath, '' )
    if value:
      return S_OK( value )
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __closeDirectory( self, oDirectory ):
    value = lfc.lfc_closedir( oDirectory )
    if value == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __getDirectoryContents( self, path, verbose = False ):
    """ Returns a dictionary containing all of the contents of a directory.
        This includes the metadata associated to files (replicas, size, guid, status) and the subdirectories found.
    """
    # First check that the directory exists
    res = self.__executeOperation( path, 'exists' )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR( 'No such file or directory' )

    res = self.__getPathStat( path )
    if not res['OK']:
      return res
    nbfiles = res['Value'].nlink
    res = self.__openDirectory( path )
    if not res['OK']:
      return res
    oDirectory = res['Value']
    subDirs = {}
    links = {}
    files = {}
    for i in  range( nbfiles ):
      result = lfc.lfc_readdirxr( oDirectory, "" )
      if not result:
        # In some rare cases we reach the end of oDirectory, before nbfiles iterations (!!!)
        break
      entry, fileInfo = result
      pathMetadata = {}
      pathMetadata['Mode'] = S_IMODE( entry.filemode )
      subPath = '%s/%s' % ( path, entry.d_name )
      if verbose:
        statRes = self.__getPathStat( subPath )
        if statRes['OK']:
          oPath = statRes['Value']
          pathMetadata['Size'] = oPath.filesize
          pathMetadata['CheckSumType'] = oPath.csumtype
          pathMetadata['Checksum'] = oPath.csumvalue
          pathMetadata['GUID'] = oPath.guid
          pathMetadata['Status'] = oPath.status
          pathMetadata['CreationDate'] = fromEpoch( oPath.ctime )
          pathMetadata['ModificationDate'] = fromEpoch( oPath.mtime )
          pathMetadata['NumberOfLinks'] = oPath.nlink
          pathMetadata['LastAccess'] = oPath.atime
          res = self.__getDNFromUID( oPath.uid )
          if res['OK']:
            pathMetadata['OwnerDN'] = res['Value']
          else:
            pathMetadata['OwnerDN'] = None
          res = self.__getRoleFromGID( oPath.gid )
          if res['OK']:
            pathMetadata['OwnerRole'] = res['Value']
          else:
            pathMetadata['OwnerRole'] = None
      if S_ISDIR( entry.filemode ):
        subDirs[subPath] = pathMetadata
      else:
        replicaDict = {}
        if fileInfo:
          for replica in fileInfo:
            replicaDict[replica.host] = {'PFN':replica.sfn, 'Status':replica.status}
        pathMetadata['Size'] = entry.filesize
        pathMetadata['GUID'] = entry.guid
        if S_ISLNK( entry.filemode ):
          res = self.__executeOperation( subPath, 'readLink' )
          if res['OK']:
            pathMetadata['Target'] = res['Value']
          links[subPath] = {}
          links[subPath]['MetaData'] = pathMetadata
          links[subPath]['Replicas'] = replicaDict
        elif S_ISREG( entry.filemode ):
          files[subPath] = {}
          files[subPath]['Replicas'] = replicaDict
          files[subPath]['MetaData'] = pathMetadata
    pathDict = {}
    res = self.__closeDirectory( oDirectory )
    pathDict = {'Files': files, 'SubDirs':subDirs, 'Links':links}
    return S_OK( pathDict )

  def __getDirectorySize( self, path ):
    res = self.__executeOperation( path, 'exists' )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR( 'No such file or directory' )

    res = self.__getPathStat( path )
    if not res['OK']:
      return res
    nbfiles = res['Value'].nlink
    res = self.__openDirectory( path )
    if not res['OK']:
      return res
    oDirectory = res['Value']
    pathDict = {'SubDirs':{}, 'ClosedDirs':[], 'Files':0, 'TotalSize':0, 'SiteUsage':{}}
    for i in  range( nbfiles ):
      entry, fileInfo = lfc.lfc_readdirxr( oDirectory, "" )
      if S_ISDIR( entry.filemode ):
        subDir = '%s/%s' % ( path, entry.d_name )
        permissions = S_IMODE( entry.filemode )
        if ( not permissions & S_IWUSR ) and ( not permissions & S_IWGRP ) and ( not permissions & S_IWOTH ):
          pathDict['ClosedDirs'].append( subDir )
        modTime = time.ctime()
        statRes = self.__getPathStat( subDir )
        if statRes['OK']:
          modTime = fromEpoch( statRes['Value'].mtime )
        pathDict['SubDirs'][subDir] = modTime
      else:
        fileSize = entry.filesize
        pathDict['TotalSize'] += fileSize
        pathDict['Files'] += 1
        if not fileInfo:
          gLogger.error( "LcgFileCatalogClient.__getDirectorySize: File found with no replicas",
                         "%s/%s" % ( path, entry.d_name ) )
        else:
          for replica in fileInfo:
            if not pathDict['SiteUsage'].has_key( replica.host ):
              pathDict['SiteUsage'][replica.host] = {'Files':0, 'Size':0}
            pathDict['SiteUsage'][replica.host]['Size'] += fileSize
            pathDict['SiteUsage'][replica.host]['Files'] += 1
    res = self.__closeDirectory( oDirectory )
    return S_OK( pathDict )

  def __getLinkStat( self, link ):
    fullLink = '%s%s' % ( self.prefix, link )
    lstat = lfc.lfc_filestat()
    value = lfc.lfc_lstat( fullLink, lstat )
    if value == 0:
      return S_OK( lstat )
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __readLink( self, link ):
    fullLink = '%s%s' % ( self.prefix, link )
    buff = " " * ( lfc.CA_MAXPATHLEN + 1 )
    chars = lfc.lfc_readlink( fullLink, buff, lfc.CA_MAXPATHLEN )
    if chars > 0:
      return S_OK( buff[:chars].replace( self.prefix, '' ).replace( '\x00', '' ) )
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __makeLink( self, source, target ):
    fullLink = '%s%s' % ( self.prefix, source )
    fullLfn = '%s%s' % ( self.prefix, target )
    value = lfc.lfc_symlink( fullLfn, fullLink )
    if value == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __getLFNGuid( self, lfn ):
    """Get the GUID for the given lfn"""
    fstat = lfc.lfc_filestatg()
    fullLfn = '%s%s' % ( self.prefix, lfn )
    value = lfc.lfc_statg( fullLfn, '', fstat )
    if value == 0:
      return S_OK( fstat.guid )
    else:
      errStr = lfc.sstrerror( lfc.cvar.serrno )
      return S_ERROR( errStr )

  def __createDataset( self, datasetName, lfns ):
    res = self.__makeDirs( datasetName )
    if not res['OK']:
      return res
    links = {}
    for lfn in lfns:
      res = self.__getLFNGuid( lfn )
      if not res['OK']:
        return res
      else:
        link = "%s/%s" % ( datasetName, res['Value'] )
        links[link] = lfn
    res = self.createLink( links )
    if len( res['Value']['Successful'] ) == len( links.keys() ):
      return S_OK()
    totalError = ""
    for link, error in res['Value']['Failed'].items():
      gLogger.error( "LcgFileCatalogClient.__createDataset: Failed to create link for %s." % link, error )
      totalError = "%s\n %s : %s" % ( totalError, link, error )
    return S_ERROR( totalError )

  def __removeDataset( self, datasetName ):
    res = self.__getDirectoryContents( datasetName )
    if not res['OK']:
      return res
    #links = res['Value']['Links'].keys()
    links = res['Value']['Files'].keys()
    res = self.removeLink( links )
    if not res['OK']:
      return res
    elif len( res['Value']['Failed'].keys() ):
      return S_ERROR( "Failed to remove all links" )
    else:
      res = self.__executeOperation( datasetName, 'removeDirectory' )
      return res

  def __removeFilesFromDataset( self, datasetName, lfns ):
    links = []
    for lfn in lfns:
      res = self.__getLFNGuid( lfn )
      if not res['OK']:
        return res
      guid = res['Value']
      linkPath = "%s/%s" % ( datasetName, guid )
      links.append( linkPath )
    res = self.removeLink( links )
    if not res['OK']:
      return res
    if len( res['Value']['Successful'] ) == len( links ):
      return S_OK()
    totalError = ""
    for link, error in res['Value']['Failed'].items():
      gLogger.error( "LcgFileCatalogClient.__removeFilesFromDataset: Failed to remove link %s." % link, error )
      totalError = "%s %s : %s" % ( totalError, link, error )
    return S_ERROR( totalError )

  ####################################################################
  #
  # These are the methods required for the admin interface
  #

  def getUserDirectory( self, username ):
    """ Takes a list of users and determines whether their directories already exist
    """
    vo = getVO( 'lhcb' )
    res = self.__checkArgumentFormat( username )
    if not res['OK']:
      return res
    usernames = res['Value'].keys()
    usernameDict = {}
    for username in usernames:
      userDirectory = "/%s/user/%s/%s" % ( vo, username[0], username )
      usernameDict[userDirectory] = username
    res = self.exists( usernameDict.keys() )
    if not res['OK']:
      return res
    failed = {}
    for directory, reason in res['Value']['Failed'].items():
      failed[usernameDict[directory]] = reason
    successful = {}
    for directory, exists in res['Value']['Successful'].items():
      successful[usernameDict[directory]] = exists
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def createUserDirectory( self, username ):
    """ Creates the user directory
    """
    vo = getVO( 'lhcb' )
    res = self.__checkArgumentFormat( username )
    if not res['OK']:
      return res
    usernames = res['Value'].keys()
    successful = {}
    failed = {}
    created = self.__openSession()
    for username in usernames:
      userDirectory = "/%s/user/%s/%s" % ( vo, username[0], username )
      res = self.__makeDirs( userDirectory, 0755 )
      if res['OK']:
        successful[username] = userDirectory
      else:
        failed[username] = res['Message']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeUserDirectory( self, username ):
    """ Remove the user directory and remove the user mapping
    """
    created = self.__openSession()
    res = self.getUserDirectory( username )
    if not res['OK']:
      return res
    failed = {}
    for username, error in res['Value']['Failed'].items():
      failed[username] = error
    directoriesToRemove = {}
    successful = {}
    for username, directory in res['Value']['Successful'].items():
      if not directory:
        successful[username] = True
      else:
        directoriesToRemove[directory] = username
    res = self.removeDirectory( directoriesToRemove.keys() )
    if not res['OK']:
      return res
    for directory, error in res['Value']['Failed'].items():
      failed[directoriesToRemove[directory]] = error
    for directory in res['Value']['Successful'].keys():
      successful[directoriesToRemove[directory]] = True
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def changeDirectoryOwner( self, directory ):
    """ Change the ownership of the directory to the user associated to the supplied DN
    """
    res = self.__checkArgumentFormat( directory )
    if not res['OK']:
      return res
    directory = res['Value']
    successful = {}
    failed = {}
    created = self.__openSession()
    for dirPath, dn in directory.items():
      res = self.__getDNUserID( dn )
      if not res['OK']:
        failed[dirPath] = res['Message']
      else:
        userID = res['Value']
        res = self.__changeOwner( dirPath, userID )
        if not res['OK']:
          failed[dirPath] = res['Message']
        else:
          successful[dirPath] = True
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def createUserMapping( self, userDN ):
    """ Create a user with the supplied DN and return the userID
    """
    res = self.__checkArgumentFormat( userDN )
    if not res['OK']:
      return res
    userDNs = res['Value']
    successful = {}
    failed = {}
    created = self.__openSession()
    for userDN, uid in userDNs.items():
      if not uid:
        uid = -1
      res = self.__addUserDN( uid, userDN )
      if not res['OK']:
        failed[userDN] = res['Message']
      else:
        res = self.__getDNUserID( userDN )
        if not res['OK']:
          failed[userDN] = res['Message']
        else:
          successful[userDN] = res['Value']
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  ####################################################################
  #
  # These are the internal methods used for the admin interface
  #

  def __getUserDNs( self, userID ):
    value, users = lfc.lfc_getusrmap()
    if value != 0:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )
    else:
      dns = []
      for userMap in users:
        if userMap.userid == userID:
          dns.append( userMap.username )
      return S_OK( dns )

  def __getDNUserID( self, dn ):
    value, users = lfc.lfc_getusrmap()
    if value != 0:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )
    else:
      for userMap in users:
        if userMap.username == dn:
          return S_OK( userMap.userid )
      return S_ERROR( "DN did not exist" )

  def __rmUserDN( self, dn ):
    res = lfc.lfc_rmusrmap( 0, dn )
    if res == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __rmUserID( self, userID ):
    res = lfc.lfc_rmusrmap( userID, '' )
    if res == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __addUserDN( self, userID, dn ):
    res = lfc.lfc_enterusrmap( userID, dn )
    if res == 0:
      return S_OK()
    errorNo = lfc.cvar.serrno
    if errorNo == 17:
      # User DN already exists
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __changeOwner( self, lfn, userID ):
    fullLfn = '%s%s' % ( self.prefix, lfn )
    res = lfc.lfc_chown( fullLfn, userID, -1 )
    if res == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  def __changeMod( self, lfn, mode ):
    fullLfn = '%s%s' % ( self.prefix, lfn )
    res = lfc.lfc_chmod( fullLfn, mode )
    if res == 0:
      return S_OK()
    else:
      return S_ERROR( lfc.sstrerror( lfc.cvar.serrno ) )

  # THIS IS NOT YET WORKING
  def getReplicasNew( self, lfn, allStatus = False ):
    """ Returns replicas for an LFN or list of LFNs
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    lfnChunks = breakListIntoChunks( sortList( lfns.keys() ), 1000 )
    # If we have less than three groups to query a session doesn't make sense
    created = False
    if len( lfnChunks ) > 2:
      created = self.__openSession()
    failed = {}
    successful = {}
    for lfnList in lfnChunks:
      value, replicaList = lfc.lfc_getreplicasl( lfnList, '' )
      if value != 0:
        for lfn in lfnList:
          failed[lfn] = lfc.sstrerror( lfc.cvar.serrno )
        continue
      for oReplica in replicaList:
        #TODO WORK OUT WHICH LFN THIS CORRESPONDS
        status = oReplica.status
        if ( status != 'P' ) or allStatus:
          se = oReplica.host
          pfn = oReplica.sfn#.strip()
          # replicas[se] = pfn
    if created:
      self.__closeSession()
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )
