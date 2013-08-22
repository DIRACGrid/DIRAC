# ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###  ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###
# ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###  ABANDONWARE ### ABANDONWARE ### ABANDONWARE ###
#
# At the moment no one is writing a "integrity" Requests.
# It can be resurrected once we'll write those requests again.
# In any case, to be used it should be re-written in the spirit of the new RMS system,
# e.g. like /DataManagement/Agent/RequestOperations object


#
# """  SEvsLFCAgent takes data integrity checks from the RequestDB and verifies the integrity of the supplied directory.
# """
# from DIRAC                                                          import gLogger, gConfig, gMonitor, S_OK, S_ERROR
# from DIRAC.Core.Base.AgentModule                                    import AgentModule
# from DIRAC.Core.Utilities.Pfn                                       import pfnparse, pfnunparse
# from DIRAC.Core.DISET.RPCClient                                     import RPCClient
# from DIRAC.RequestManagementSystem.Client.RequestClient             import RequestClient
# from DIRAC.RequestManagementSystem.Client.RequestContainer          import RequestContainer
# from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
# from DIRAC.DataManagementSystem.Agent.NamespaceBrowser              import NamespaceBrowser
# from DIRAC.Resources.Storage.StorageElement                         import StorageElement
#
# import time, os
# from types import *
#
# __RCSID__ = "$Id$"
#
# AGENT_NAME = 'DataManagement/SEvsLFCAgent'
#
# class SEvsLFCAgent( AgentModule ):
#
#  def initialize( self ):
#
#    self.RequestDBClient = RequestClient()
#    self.ReplicaManager = ReplicaManager()
#
#    # This sets the Default Proxy to used as that defined under
#    # /Operations/Shifter/DataManager
#    # the shifterProxy option in the Configuration can be used to change this default.
#    self.am_setOption( 'shifterProxy', 'DataManager' )
#
#    return S_OK()
#
#  def execute( self ):
#
#    IntegrityDB = RPCClient( 'DataManagement/DataIntegrity' )
#
#    res = self.RequestDBClient.getRequest( 'integrity' )
#    if not res['OK']:
#      gLogger.info( "SEvsLFCAgent.execute: Failed to get request from database." )
#      return S_OK()
#    elif not res['Value']:
#      gLogger.info( "SEvsLFCAgent.execute: No requests to be executed found." )
#      return S_OK()
#    requestString = res['Value']['requestString']
#    requestName = res['Value']['requestName']
#    sourceServer = res['Value']['Server']
#    gLogger.info( "SEvsLFCAgent.execute: Obtained request %s" % requestName )
#    oRequest = RequestContainer( request = requestString )
#
#    ################################################
#    # Find the number of sub-requests from the request
#    res = oRequest.getNumSubRequests( 'integrity' )
#    if not res['OK']:
#      errStr = "SEvsLFCAgent.execute: Failed to obtain number of integrity subrequests."
#      gLogger.error( errStr, res['Message'] )
#      return S_OK()
#    gLogger.info( "SEvsLFCAgent.execute: Found %s sub requests." % res['Value'] )
#
#    ################################################
#    # For all the sub-requests in the request
#    for ind in range( res['Value'] ):
#      gLogger.info( "SEvsLFCAgent.execute: Processing sub-request %s." % ind )
#      subRequestAttributes = oRequest.getSubRequestAttributes( ind, 'integrity' )['Value']
#      if subRequestAttributes['Status'] == 'Waiting':
#        subRequestFiles = oRequest.getSubRequestFiles( ind, 'integrity' )['Value']
#        operation = subRequestAttributes['Operation']
#
#        ################################################
#        #  If the sub-request is a lfcvsse operation
#        if operation == 'SEvsLFC':
#          gLogger.info( "SEvsLFCAgent.execute: Attempting to execute %s sub-request." % operation )
#          storageElementName = subRequestAttributes['StorageElement']
#          for subRequestFile in subRequestFiles:
#            if subRequestFile['Status'] == 'Waiting':
#              lfn = subRequestFile['LFN']
#              storageElement = StorageElement( storageElementName )
#              res = storageElement.isValid()
#              if not res['OK']:
#                errStr = "SEvsLFCAgent.execute: Failed to instantiate destination StorageElement."
#                gLogger.error( errStr, storageElement )
#              else:
#                res = storageElement.getPfnForLfn( lfn )
#                if not res['OK']:
#                  gLogger.info( 'shit bugger do something.' )
#                else:
#                  oNamespaceBrowser = NamespaceBrowser( res['Value'] )
#                  # Loop over all the directories and sub-directories
#                  while ( oNamespaceBrowser.isActive() ):
#                    currentDir = oNamespaceBrowser.getActiveDir()
#
#                    gLogger.info( "SEvsLFCAgent.execute: Attempting to list the contents of %s." % currentDir )
#                    res = storageElement.listDirectory( currentDir )
#                    if not res['Value']['Successful'].has_key( currentDir ):
#                      gLogger.error( "SEvsLFCAgent.execute: Failed to list the directory contents.", "%s %s" % ( currentDir, res['Value']['Successful']['Failed'][currentDir] ) )
#                      subDirs = [currentDir]
#                    else:
#                      subDirs = []
#                      files = {}
#                      for surl, surlDict in res['Value']['Successful'][currentDir]['Files'].items():
#                        pfnRes = storageElement.getPfnForProtocol( surl, 'SRM2', withPort = False )
#                        surl = pfnRes['Value']
#                        files[surl] = surlDict
#                      for surl, surlDict in res['Value']['Successful'][currentDir]['SubDirs'].items():
#                        pfnRes = storageElement.getPfnForProtocol( surl, 'SRM2', withPort = False )
#                        surl = pfnRes['Value']
#                        subDirs.append( surl )
#
#                      #subDirs = res['Value']['Successful'][currentDir]['SubDirs']
#                      gLogger.info( "SEvsLFCAgent.execute: Successfully obtained %s sub-directories." % len( subDirs ) )
#                      #files = res['Value']['Successful'][currentDir]['Files']
#                      gLogger.info( "SEvsLFCAgent.execute: Successfully obtained %s files." % len( files ) )
#
#                      selectedLfns = []
#                      lfnPfnDict = {}
#                      pfnSize = {}
#
#                      for pfn, pfnDict in files.items():
#                        res = storageElement.getPfnPath( pfn )
#                        if not res['OK']:
#                          gLogger.error( "SEvsLFCAgent.execute: Failed to get determine LFN from pfn.", "%s %s" % ( pfn, res['Message'] ) )
#                          fileMetadata = {'Prognosis':'NonConventionPfn', 'LFN':'', 'PFN':pfn, 'StorageElement':storageElementName, 'Size':pfnDict['Size']}
#                          res = IntegrityDB.insertProblematic( AGENT_NAME, fileMetadata )
#                          if res['OK']:
#                            gLogger.info( "SEvsLFCAgent.execute: Successfully added to IntegrityDB." )
#                            gLogger.error( "Change the status in the LFC,ProcDB...." )
#                          else:
#                            gLogger.error( "Shit, fuck, bugger. Add the failover." )
#                        else:
#                          lfn = res['Value']
#                          selectedLfns.append( lfn )
#                          lfnPfnDict[lfn] = pfn
#                          pfnSize[pfn] = pfnDict['Size']
#
#                      res = self.ReplicaManager.getCatalogFileMetadata( selectedLfns )
#                      if not res['OK']:
#                        subDirs = [currentDir]
#                      else:
#                        for lfn in res['Value']['Failed'].keys():
#                          gLogger.error( "SEvsLFCAgent.execute: Failed to get metadata.", "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
#                          pfn = lfnPfnDict[lfn]
#                          fileMetadata = {'Prognosis':'SEPfnNoLfn', 'LFN':lfn, 'PFN':pfn, 'StorageElement':storageElementName, 'Size':pfnSize[pfn]}
#                          res = IntegrityDB.insertProblematic( AGENT_NAME, fileMetadata )
#                          if res['OK']:
#                            gLogger.info( "SEvsLFCAgent.execute: Successfully added to IntegrityDB." )
#                            gLogger.error( "Change the status in the LFC,ProcDB...." )
#                          else:
#                            gLogger.error( "Shit, fuck, bugger. Add the failover." )
#
#                        for lfn, lfnDict in res['Value']['Successful'].items():
#                          pfn = lfnPfnDict[lfn]
#                          storageSize = pfnSize[pfn]
#                          catalogSize = lfnDict['Size']
#                          if int( catalogSize ) == int( storageSize ):
#                            gLogger.info( "SEvsLFCAgent.execute: Catalog and storage sizes match.", "%s %s" % ( pfn, storageElementName ) )
#                            gLogger.info( "Change the status in the LFC" )
#                          elif int( storageSize ) == 0:
#                            gLogger.error( "SEvsLFCAgent.execute: Physical file size is 0.", "%s %s" % ( pfn, storageElementName ) )
#                            fileMetadata = {'Prognosis':'ZeroSizePfn', 'LFN':lfn, 'PFN':pfn, 'StorageElement':storageElementName}
#                            res = IntegrityDB.insertProblematic( AGENT_NAME, fileMetadata )
#                            if res['OK']:
#                              gLogger.info( "SEvsLFCAgent.execute: Successfully added to IntegrityDB." )
#                              gLogger.error( "Change the status in the LFC,ProcDB...." )
#                            else:
#                              gLogger.error( "Shit, fuck, bugger. Add the failover." )
#                          else:
#                            gLogger.error( "SEvsLFCAgent.execute: Catalog and storage size mis-match.", "%s %s" % ( pfn, storageElementName ) )
#                            fileMetadata = {'Prognosis':'PfnSizeMismatch', 'LFN':lfn, 'PFN':pfn, 'StorageElement':storageElementName}
#                            res = IntegrityDB.insertProblematic( AGENT_NAME, fileMetadata )
#                            if res['OK']:
#                              gLogger.info( "SEvsLFCAgent.execute: Successfully added to IntegrityDB." )
#                              gLogger.error( "Change the status in the LFC,ProcDB...." )
#                            else:
#                              gLogger.error( "Shit, fuck, bugger. Add the failover." )
#
#                        res = self.ReplicaManager.getCatalogReplicas( selectedLfns )
#                        if not res['OK']:
#                          subDirs = [currentDir]
#                        else:
#                          for lfn in res['Value']['Failed'].keys():
#                            gLogger.error( "SEvsLFCAgent.execute: Failed to get replica information.", "%s %s" % ( lfn, res['Value']['Failed'][lfn] ) )
#                            pfn = lfnPfnDict[lfn]
#                            fileMetadata = {'Prognosis':'PfnNoReplica', 'LFN':lfn, 'PFN':pfn, 'StorageElement':storageElementName, 'Size':pfnSize[pfn]}
#                            res = IntegrityDB.insertProblematic( AGENT_NAME, fileMetadata )
#                            if res['OK']:
#                              gLogger.info( "SEvsLFCAgent.execute: Successfully added to IntegrityDB." )
#                              gLogger.error( "Change the status in the LFC,ProcDB...." )
#                            else:
#                              gLogger.error( "Shit, fuck, bugger. Add the failover." )
#
#                          for lfn, repDict in res['Value']['Successful'].items():
#                            pfn = lfnPfnDict[lfn]
#                            registeredPfns = repDict.values()
#                            if not pfn in registeredPfns:
#                              gLogger.error( "SEvsLFCAgent.execute: SE PFN not registered.", "%s %s" % ( lfn, pfn ) )
#                              fileMetadata = {'Prognosis':'PfnNoReplica', 'LFN':lfn, 'PFN':pfn, 'StorageElement':storageElementName}
#                              res = IntegrityDB.insertProblematic( AGENT_NAME, fileMetadata )
#                              if res['OK']:
#                                gLogger.info( "SEvsLFCAgent.execute: Successfully added to IntegrityDB." )
#                                gLogger.error( "Change the status in the LFC,ProcDB...." )
#                              else:
#                                gLogger.error( "Shit, fuck, bugger. Add the failover." )
#                            else:
#                              gLogger.info( "SEvsLFCAgent.execute: SE Pfn verified.", pfn )
#
#                    oNamespaceBrowser.updateDirs( subDirs )
#                  oRequest.setSubRequestFileAttributeValue( ind, 'integrity', lfn, 'Status', 'Done' )
#
#        ################################################
#        #  If the sub-request is none of the above types
#        else:
#          gLogger.info( "SEvsLFCAgent.execute: Operation not supported.", operation )
#
#        ################################################
#        #  Determine whether there are any active files
#        if oRequest.isSubRequestEmpty( ind, 'integrity' )['Value']:
#          oRequest.setSubRequestStatus( ind, 'integrity', 'Done' )
#
#      ################################################
#      #  If the sub-request is already in terminal state
#      else:
#        gLogger.info( "SEvsLFCAgent.execute: Sub-request %s is status '%s' and  not to be executed." % ( ind, subRequestAttributes['Status'] ) )
#
#    ################################################
#    #  Generate the new request string after operation
#    requestString = oRequest.toXML()['Value']
#    res = self.RequestDBClient.updateRequest( requestName, requestString, sourceServer )
#
#    return S_OK()
