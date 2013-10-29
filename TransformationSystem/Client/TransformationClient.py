""" Class that contains client access to the transformation DB handler. """

from DIRAC                                          import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.Client                         import Client
from DIRAC.Core.Utilities.List                      import breakListIntoChunks
from DIRAC.Resources.Catalog.FileCatalogueBase      import FileCatalogueBase
import types

rpc = None
url = None

class TransformationClient( Client, FileCatalogueBase ):

  """ Exposes the functionality available in the DIRAC/TransformationHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).

      Transformation (table) manipulation

          deleteTransformation(transName)
          getTransformationParameters(transName,paramNames)
          getTransformationWithStatus(status)
          setTransformationParameter(transName,paramName,paramValue)
          deleteTransformationParameter(transName,paramName)

      TransformationFiles table manipulation

          addFilesToTransformation(transName,lfns)
          addTaskForTransformation(transName,lfns=[],se='Unknown')
          setFileUsedSEForTransformation(transName,usedSE,lfns)
          getTransformationStats(transName)

      TransformationTasks table manipulation

          setTaskStatus(transName, taskID, status)
          setTaskStatusAndWmsID(transName, taskID, status, taskWmsID)
          getTransformationTaskStats(transName)
          deleteTasks(transName, taskMin, taskMax)
          extendTransformation( transName, nTasks)
          getTasksToSubmit(transName,numTasks,site='')

      TransformationLogging table manipulation

          getTransformationLogging(transName)

      File/directory manipulation methods (the remainder of the interface can be found below)

          getFileSummary(lfns)
          exists(lfns)

      Web monitoring tools

          getDistinctAttributeValues(attribute, selectDict)
          getTransformationStatusCounters()
          getTransformationSummary()
          getTransformationSummaryWeb(selectDict, sortList, startItem, maxItems)
  """

  def __init__( self, **kwargs ):

    Client.__init__( self, **kwargs )
    self.setServer( 'Transformation/TransformationManager' )

  def setServer( self, url ):
    self.serverURL = url

  def getCounters( self, table, attrList, condDict, older = None, newer = None, timeStamp = None,
                   rpc = '', url = '', timeout = 120 ):
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient. getCounters( table, attrList, condDict, older, newer, timeStamp )

  def addTransformation( self, transName, description, longDescription, transType, plugin, agentType, fileMask,
                         transformationGroup = 'General',
                         groupSize = 1,
                         inheritedFrom = 0,
                         body = '',
                         maxTasks = 0,
                         eventsPerTask = 0,
                         addFiles = True,
                         rpc = '', url = '', timeout = 120 ):
    """ add a new transformation
    """
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.addTransformation( transName, description, longDescription, transType, plugin,
                                        agentType, fileMask, transformationGroup, groupSize, inheritedFrom,
                                        body, maxTasks, eventsPerTask, addFiles )

  def getTransformations( self, condDict = {}, older = None, newer = None, timeStamp = 'CreationDate',
                          orderAttribute = None, limit = 100, extraParams = False, rpc = '', url = '', timeout = 120 ):
    """ gets all the transformations in the system, incrementally. "limit" here is just used to determine the offset.
    """
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )

    transformations = []
    # getting transformations - incrementally
    offsetToApply = 0
    while True:
      res = rpcClient.getTransformations( condDict, older, newer, timeStamp, orderAttribute, limit,
                                          extraParams, offsetToApply )
      if not res['OK']:
        return res
      else:
        gLogger.verbose( "Result for limit %d, offset %d: %d" % ( limit, offsetToApply, len( res['Value'] ) ) )
        if res['Value']:
          transformations = transformations + res['Value']
          offsetToApply += limit
        if len( res['Value'] ) < limit:
          break
    return S_OK( transformations )

  def getTransformation( self, transName, extraParams = False, rpc = '', url = '', timeout = 120 ):
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getTransformation( transName, extraParams )

  def getTransformationFiles( self, condDict = {}, older = None, newer = None, timeStamp = 'LastUpdate',
                              orderAttribute = None, limit = 10000, rpc = '', url = '', timeout = 120 ):
    """ gets all the transformation files for a transformation, incrementally.
        "limit" here is just used to determine the offset.
    """
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    transformationFiles = []
    # getting transformationFiles - incrementally
    offsetToApply = 0
    while True:
      res = rpcClient.getTransformationFiles( condDict, older, newer, timeStamp, orderAttribute, limit, offsetToApply )
      if not res['OK']:
        return res
      else:
        gLogger.verbose( "Result for limit %d, offset %d: %d" % ( limit, offsetToApply, len( res['Value'] ) ) )
        if res['Value']:
          transformationFiles = transformationFiles + res['Value']
          offsetToApply += limit
        if len( res['Value'] ) < limit:
          break
    return S_OK( transformationFiles )


  def getTransformationTasks( self, condDict = {}, older = None, newer = None, timeStamp = 'CreationTime',
                              orderAttribute = None, limit = 10000, inputVector = False, rpc = '',
                              url = '', timeout = 120 ):
    """ gets all the transformation tasks for a transformation, incrementally.
        "limit" here is just used to determine the offset.
    """
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    transformationTasks = []
    # getting transformationFiles - incrementally
    offsetToApply = 0
    while True:
      res = rpcClient.getTransformationTasks( condDict, older, newer, timeStamp, orderAttribute, limit,
                                              inputVector, offsetToApply )
      if not res['OK']:
        return res
      else:
        gLogger.verbose( "Result for limit %d, offset %d: %d" % ( limit, offsetToApply, len( res['Value'] ) ) )
        if res['Value']:
          transformationTasks = transformationTasks + res['Value']
          offsetToApply += limit
        if len( res['Value'] ) < limit:
          break
    return S_OK( transformationTasks )


  def cleanTransformation( self, transID, pc = '', url = '', timeout = 120 ):
    """ Clean the transformation, and set the status parameter (doing it here, for easier extensibility)
    """
    # Cleaning
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    res = rpcClient.cleanTransformation( transID )
    if not res['OK']:
      return res
    # Setting the status
    return self.setTransformationParameter( transID, 'Status', 'TransformationCleaned' )

  def moveFilesToDerivedTransformation( self, transDict, resetUnused = True ):
    """ move files input to a transformation, to the derived one
    """
    prod = transDict['TransformationID']
    parentProd = int( transDict.get( 'InheritedFrom', 0 ) )
    movedFiles = {}
    if not parentProd:
      gLogger.warn( "[None] [%d] .moveFilesToDerivedTransformation: Transformation was not derived..." % prod )
      return S_OK( ( parentProd, movedFiles ) )
    statusToMove = [ 'Unused', 'MaxReset' ]
    selectDict = {'TransformationID': parentProd, 'Status': statusToMove}
    res = self.getTransformationFiles( selectDict )
    if not res['OK']:
      gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error getting Unused files from transformation %s:" % ( prod, parentProd ), res['Message'] )
      return res
    parentFiles = res['Value']
    lfns = [lfnDict['LFN'] for lfnDict in parentFiles]
    if not lfns:
      gLogger.info( "[None] [%d] .moveFilesToDerivedTransformation: No files found to be moved from transformation %d" % ( prod, parentProd ) )
      return S_OK( ( parentProd, movedFiles ) )
    selectDict = { 'TransformationID': prod, 'LFN': lfns}
    res = self.getTransformationFiles( selectDict )
    if not res['OK']:
      gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error getting files from derived transformation" % prod, res['Message'] )
      return res
    derivedFiles = res['Value']
    suffix = '-%d' % parentProd
    derivedStatusDict = dict( [( derivedDict['LFN'], derivedDict['Status'] ) for derivedDict in derivedFiles] )
    newStatusFiles = {}
    parentStatusFiles = {}
    force = False
    for parentDict in parentFiles:
      lfn = parentDict['LFN']
      derivedStatus = derivedStatusDict.get( lfn )
      if derivedStatus:
        parentStatus = parentDict['Status']
        if resetUnused and parentStatus == 'MaxReset':
          status = 'Unused'
          moveStatus = 'Unused from MaxReset'
          force = True
        else:
          status = parentStatus
          moveStatus = parentStatus
        if derivedStatus.endswith( suffix ):
          # This file is Unused or MaxReset while it was most likely Assigned at the time of derivation
          parentStatusFiles.setdefault( 'Moved-%s' % str( prod ), [] ).append( lfn )
          newStatusFiles.setdefault( ( status, parentStatus ), [] ).append( lfn )
          movedFiles[moveStatus] = movedFiles.setdefault( moveStatus, 0 ) + 1
        elif parentDict['Status'] == 'Unused':
          # If the file was Unused already at derivation time, set it NotProcessed
          parentStatusFiles.setdefault( 'NotProcessed', [] ).append( lfn )

    # Set the status in the parent transformation first
    for status, lfnList in parentStatusFiles.items():
      for lfnChunk in breakListIntoChunks( lfnList, 5000 ):
        res = self.setFileStatusForTransformation( parentProd, status, lfnChunk )
        if not res['OK']:
          gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error setting status %s for %d files in transformation %d "
                         % ( prod, status, len( lfnList ), parentProd ),
                         res['Message'] )

    # Set the status in the new transformation
    for ( status, oldStatus ), lfnList in newStatusFiles.items():
      for lfnChunk in breakListIntoChunks( lfnList, 5000 ):
        res = self.setFileStatusForTransformation( prod, status, lfnChunk, force = force )
        if not res['OK']:
          gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error setting status %s for %d files; resetting them %s in transformation %d"
                         % ( prod, status, len( lfnChunk ), oldStatus, parentProd ),
                         res['Message'] )
          res = self.setFileStatusForTransformation( parentProd, oldStatus, lfnChunk )
          if not res['OK']:
            gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error setting status %s for %d files in transformation %d"
                           % ( prod, oldStatus, len( lfnChunk ), parentProd ),
                           res['Message'] )


    return S_OK( ( parentProd, movedFiles ) )

  def setFileStatusForTransformation( self, transName, status, lfns, force = False, timeout = 120 ):
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.setFileStatusForTransformation( transName, status, lfns, force )

  #####################################################################
  #
  # These are the file catalog interface methods
  #

  def isOK( self ):
    return self.valid

  def getName( self, DN = '' ):
    """ Get the file catalog type name
    """
    return self.name

  def addDirectory( self, path, force = False, rpc = '', url = '', timeout = 120 ):
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.addDirectory( path, force )

  def getReplicas( self, lfn, rpc = '', url = '', timeout = 120 ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getReplicas( lfns )

  def addFile( self, lfn, force = False, rpc = '', url = '', timeout = 120 ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndicts = res['Value']
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.addFile( lfndicts, force )

  def addReplica( self, lfn, force = False, rpc = '', url = '', timeout = 120 ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndicts = res['Value']
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.addReplica( lfndicts, force )

  def removeFile( self, lfn, rpc = '', url = '', timeout = 120 ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    successful = {}
    failed = {}
    listOfLists = breakListIntoChunks( lfns, 100 )
    for fList in listOfLists:
      res = rpcClient.removeFile( fList )
      if not res['OK']:
        return res
      successful.update( res['Value']['Successful'] )
      failed.update( res['Value']['Failed'] )
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK( resDict )

  def removeReplica( self, lfn, rpc = '', url = '', timeout = 120 ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndicts = res['Value']
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    successful = {}
    failed = {}
    # as lfndicts is a dict, the breakListIntoChunks will fail. Fake it!
    listOfDicts = []
    localdicts = {}
    for lfn, info in lfndicts.items():
      localdicts.update( { lfn : info } )
      if len( localdicts.keys() ) % 100 == 0:
        listOfDicts.append( localdicts )
        localdicts = {}
    for fDict in listOfDicts:
      res = rpcClient.removeReplica( fDict )
      if not res['OK']:
        return res
      successful.update( res['Value']['Successful'] )
      failed.update( res['Value']['Failed'] )
    resDict = {'Successful': successful, 'Failed':failed}
    return S_OK( resDict )

  def getReplicaStatus( self, lfn, rpc = '', url = '', timeout = 120 ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndict = res['Value']
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getReplicaStatus( lfndict )

  def setReplicaStatus( self, lfn, rpc = '', url = '', timeout = 120 ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndict = res['Value']
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.setReplicaStatus( lfndict )

  def setReplicaHost( self, lfn, rpc = '', url = '', timeout = 120 ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndict = res['Value']
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.setReplicaHost( lfndict )

  def removeDirectory( self, lfn, rpc = '', url = '', timeout = 120 ):
    return self.__returnOK( lfn )

  def createDirectory( self, lfn, rpc = '', url = '', timeout = 120 ):
    return self.__returnOK( lfn )

  def createLink( self, lfn, rpc = '', url = '', timeout = 120 ):
    return self.__returnOK( lfn )

  def removeLink( self, lfn, rpc = '', url = '', timeout = 120 ):
    return self.__returnOK( lfn )

  def __returnOK( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    successful = {}
    for lfn in res['Value'].keys():
      successful[lfn] = True
    resDict = {'Successful':successful, 'Failed':{}}
    return S_OK( resDict )

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
      return S_ERROR( "TransformationClient.__checkArgumentFormat: Supplied path is not of the correct format." )
    return S_OK( urls )
