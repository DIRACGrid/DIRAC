# ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###  ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###
# ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###  ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###
#
# At the moment no one is writing a "integrity" Requests.
# It can be resurrected once we'll write those requests again.
# In any case, to be used it should be re-written in the spirit of the new RMS system,
# e.g. like /DataManagement/Agent/RequestOperations object



# """  LFCvsSEAgent takes data integrity checks from the RequestDB and verifies the integrity of the supplied directory.
# """
# from DIRAC                                                                  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
# from DIRAC.Core.Base.AgentModule                                            import AgentModule
# from DIRAC.Core.Utilities.Pfn                                               import pfnparse, pfnunparse
# from DIRAC.Core.DISET.RPCClient                                             import RPCClient
# from DIRAC.RequestManagementSystem.Client.RequestClient                     import RequestClient
# from DIRAC.RequestManagementSystem.Client.RequestContainer                  import RequestContainer
# from DIRAC.DataManagementSystem.Client.ReplicaManager                       import ReplicaManager
# from DIRAC.DataManagementSystem.Agent.NamespaceBrowser                      import NamespaceBrowser
#
# import time, os
# from types import *
#
# AGENT_NAME = "DataManagement/LFCvsSEAgent"
#
# __RCSID__ = "$Id$"
#
# class LFCvsSEAgent( AgentModule ):
#
#  def initialize( self ):
#
#    self.RequestDBClient = RequestClient()
#    self.ReplicaManager = ReplicaManager()
#    # This sets the Default Proxy to used as that defined under
#    # /Operations/Shifter/DataManager
#    # the shifterProxy option in the Configuration can be used to change this default.
#    self.am_setOption( 'shifterProxy', 'DataManager' )
#
#    return S_OK()
#
#  def execute( self ):
#
#    res = self.RequestDBClient.getRequest( 'integrity' )
#    if not res['OK']:
#      gLogger.info( "LFCvsSEAgent.execute: Failed to get request from database." )
#      return S_OK()
#    elif not res['Value']:
#      gLogger.info( "LFCvsSEAgent.execute: No requests to be executed found." )
#      return S_OK()
#    requestString = res['Value']['RequestString']
#    requestName = res['Value']['RequestName']
#    sourceServer = res['Value']['Server']
#    gLogger.info( "LFCvsSEAgent.execute: Obtained request %s" % requestName )
#    oRequest = RequestContainer( request = requestString )
#
#    ################################################
#    # Find the number of sub-requests from the request
#    res = oRequest.getNumSubRequests( 'integrity' )
#    if not res['OK']:
#      errStr = "LFCvsSEAgent.execute: Failed to obtain number of integrity subrequests."
#      gLogger.error( errStr, res['Message'] )
#      return S_OK()
#    gLogger.info( "LFCvsSEAgent.execute: Found %s sub requests." % res['Value'] )
#
#    ################################################
#    # For all the sub-requests in the request
#    for ind in range( res['Value'] ):
#      gLogger.info( "LFCvsSEAgent.execute: Processing sub-request %s." % ind )
#      subRequestAttributes = oRequest.getSubRequestAttributes( ind, 'integrity' )['Value']
#      if subRequestAttributes['Status'] == 'Waiting':
#        subRequestFiles = oRequest.getSubRequestFiles( ind, 'integrity' )['Value']
#        operation = subRequestAttributes['Operation']
#
#        ################################################
#        #  If the sub-request is a lfcvsse operation
#        if operation == 'LFCvsSE':
#          gLogger.info( "LFCvsSEAgent.execute: Attempting to execute %s sub-request." % operation )
#          for subRequestFile in subRequestFiles:
#            if subRequestFile['Status'] == 'Waiting':
#              lfn = subRequestFile['LFN']
#              oNamespaceBrowser = NamespaceBrowser( lfn )
#
#              # Loop over all the directories and sub-directories
#              while ( oNamespaceBrowser.isActive() ):
#                currentDir = oNamespaceBrowser.getActiveDir()
#                gLogger.info( "LFCvsSEAgent.execute: Attempting to get contents of %s." % currentDir )
#                res = self.ReplicaManager.getCatalogDirectoryContents( currentDir )
#                if not res['OK']:
#                  subDirs = [currentDir]
#                elif res['Value']['Failed'].has_key( currentDir ):
#                  subDirs = [currentDir]
#                else:
#                  subDirs = res['Value']['Successful'][currentDir]['SubDirs']
#                  files = res['Value']['Successful'][currentDir]['Files']
#
#                  lfnSizeDict = {}
#                  pfnLfnDict = {}
#                  pfnStatusDict = {}
#                  sePfnDict = {}
#                  for lfn, lfnDict in files.items():
#                    lfnSizeDict[lfn] = lfnDict['MetaData']['Size']
#                    for se in lfnDict['Replicas'].keys():
#                      pfn = lfnDict['Replicas'][se]['PFN']
#                      status = lfnDict['Replicas'][se]['Status']
#                      pfnStatusDict[pfn] = status
#                      pfnLfnDict[pfn] = lfn
#                      if not sePfnDict.has_key( se ):
#                        sePfnDict[se] = []
#                      sePfnDict[se].append( pfn )
#
#                  for storageElementName, physicalFiles in sePfnDict.items():
#                    gLogger.info( "LFCvsSEAgent.execute: Attempting to get metadata for files on %s." % storageElementName )
#                    res = self.ReplicaManager.getStorageFileMetadata( physicalFiles, storageElementName )
#                    if not res['OK']:
#                      gLogger.error( "LFCvsSEAgent.execute: Completely failed to get physical file metadata.", res['Message'] )
#                    else:
#                      for pfn in res['Value']['Failed'].keys():
#                        gLogger.error( "LFCvsSEAgent.execute: Failed to get metadata.", "%s %s" % ( pfn, res['Value']['Failed'][pfn] ) )
#                        lfn = pfnLfnDict[pfn]
#                        fileMetadata = {'Prognosis':'MissingSEPfn', 'LFN':lfn, 'PFN':pfn, 'StorageElement':storageElementName, 'Size':lfnSizeDict[lfn]}
#                        IntegrityDB = RPCClient( 'DataManagement/DataIntegrity' )
#                        resInsert = IntegrityDB.insertProblematic( AGENT_NAME, fileMetadata )
#                        if resInsert['OK']:
#                          gLogger.info( "LFCvsSEAgent.execute: Successfully added to IntegrityDB." )
#                          gLogger.error( "Change the status in the LFC,ProcDB...." )
#                        else:
#                          gLogger.error( "Shit, fuck, bugger. Add the failover." )
#                      for pfn, pfnDict in res['Value']['Successful'].items():
#                        lfn = pfnLfnDict[pfn]
#                        catalogSize = int( lfnSizeDict[lfn] )
#                        storageSize = int( pfnDict['Size'] )
#                        if int( catalogSize ) == int( storageSize ):
#                          gLogger.info( "LFCvsSEAgent.execute: Catalog and storage sizes match.", "%s %s" % ( pfn, storageElementName ) )
#                          gLogger.info( "Change the status in the LFC" )
#                        else:
#                          gLogger.error( "LFCvsSEAgent.execute: Catalog and storage size mis-match.", "%s %s" % ( pfn, storageElementName ) )
#                          fileMetadata = {'Prognosis':'PfnSizeMismatch', 'LFN':lfn, 'PFN':pfn, 'StorageElement':storageElementName}
#                          IntegrityDB = RPCClient( 'DataManagement/DataIntegrity' )
#                          resInsert = IntegrityDB.insertProblematic( AGENT_NAME, fileMetadata )
#                          if resInsert['OK']:
#                            gLogger.info( "LFCvsSEAgent.execute: Successfully added to IntegrityDB." )
#                            gLogger.error( "Change the status in the LFC,ProcDB...." )
#                          else:
#                            gLogger.error( "Shit, fuck, bugger. Add the failover." )
#                oNamespaceBrowser.updateDirs( subDirs )
#              oRequest.setSubRequestFileAttributeValue( ind, 'integrity', lfn, 'Status', 'Done' )
#
#        ################################################
#        #  If the sub-request is none of the above types
#        else:
#          gLogger.info( "LFCvsSEAgent.execute: Operation not supported.", operation )
#
#        ################################################
#        #  Determine whether there are any active files
#        if oRequest.isSubRequestEmpty( ind, 'integrity' )['Value']:
#          oRequest.setSubRequestStatus( ind, 'integrity', 'Done' )
#
#      ################################################
#      #  If the sub-request is already in terminal state
#      else:
#        gLogger.info( "LFCvsSEAgent.execute: Sub-request %s is status '%s' and  not to be executed." % ( ind, subRequestAttributes['Status'] ) )
#
#    ################################################
#    #  Generate the new request string after operation
#    requestString = oRequest.toXML()['Value']
#    res = self.RequestDBClient.updateRequest( requestName, requestString, sourceServer )
#
#    return S_OK()
