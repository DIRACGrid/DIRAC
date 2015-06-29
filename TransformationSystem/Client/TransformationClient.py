""" Class that contains client access to the transformation DB handler. """

__RCSID__ = "$Id$"

import types

from DIRAC                                                  import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.Client                                 import Client
from DIRAC.Core.Utilities.List                              import breakListIntoChunks
from DIRAC.Resources.Catalog.FileCatalogueBase              import FileCatalogueBase
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations

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
    """ Simple constructor
    """

    Client.__init__( self, **kwargs )
    opsH = Operations()
    self.maxResetCounter = opsH.getValue( 'Productions/ProductionFilesMaxResetCounter', 10 )

    self.setServer( 'Transformation/TransformationManager' )

  def setServer( self, url ):
    self.serverURL = url

  def getCounters( self, table, attrList, condDict, older = None, newer = None, timeStamp = None ):
    rpcClient = self._getRPC()
    return rpcClient. getCounters( table, attrList, condDict, older, newer, timeStamp )

  def addTransformation( self, transName, description, longDescription, transType, plugin, agentType, fileMask,
                         transformationGroup = 'General',
                         groupSize = 1,
                         inheritedFrom = 0,
                         body = '',
                         maxTasks = 0,
                         eventsPerTask = 0,
                         addFiles = True,
                         timeout = 1800 ):
    """ add a new transformation
    """
    rpcClient = self._getRPC( timeout = timeout )
    return rpcClient.addTransformation( transName, description, longDescription, transType, plugin,
                                        agentType, fileMask, transformationGroup, groupSize, inheritedFrom,
                                        body, maxTasks, eventsPerTask, addFiles )

  def getTransformations( self, condDict = {}, older = None, newer = None, timeStamp = 'CreationDate',
                          orderAttribute = None, limit = 100, extraParams = False ):
    """ gets all the transformations in the system, incrementally. "limit" here is just used to determine the offset.
    """
    rpcClient = self._getRPC()

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

  def getTransformation( self, transName, extraParams = False ):
    rpcClient = self._getRPC()
    return rpcClient.getTransformation( transName, extraParams )

  def getTransformationFiles( self, condDict = {}, older = None, newer = None, timeStamp = 'LastUpdate',
                              orderAttribute = None, limit = None,
                              timeout = 1800,
                              offset = 0, maxfiles = None ):
    """ gets all the transformation files for a transformation, incrementally.
        "limit" here is just used to determine the offset.
    """
    rpcClient = self._getRPC( timeout = timeout )
    transformationFiles = []
    # getting transformationFiles - incrementally
    offsetToApply = offset
    retries = 5
    limit = limit if limit else 10000
    transID = condDict.get( 'TransformationID', 'Unknown' )
    while True:
      res = rpcClient.getTransformationFiles( condDict, older, newer, timeStamp, orderAttribute, limit, offsetToApply )
      if not res['OK']:
        gLogger.error( "Error getting files for transformation %s (offset %d), %s" %
                       ( str( transID ), offsetToApply,
                        ( 'retry %d times' % retries ) if retries else 'give up' ), res['Message'] )
        retries -= 1
        if retries:
          continue
        return res
      else:
        retries = 5
        gLogger.verbose( "For conditions %s: result for limit %d, offset %d: %d files" %
                         ( str( condDict ), limit, offsetToApply, len( res['Value'] ) ) )
        if res['Value']:
          transformationFiles += res['Value']
          offsetToApply += limit
          if maxfiles and offsetToApply >= offset + maxfiles:
            break
        if len( res['Value'] ) < limit:
          break
    return S_OK( transformationFiles )


  def getTransformationTasks( self, condDict = {}, older = None, newer = None, timeStamp = 'CreationTime',
                              orderAttribute = None, limit = 10000, inputVector = False ):
    """ gets all the transformation tasks for a transformation, incrementally.
        "limit" here is just used to determine the offset.
    """
    rpcClient = self._getRPC()
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

  def cleanTransformation( self, transID ):
    """ Clean the transformation, and set the status parameter (doing it here, for easier extensibility)
    """
    # Cleaning
    rpcClient = self._getRPC()
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
    # get the lfns in status Unused/MaxReset of the parent production
    res = self.getTransformationFiles( condDict = {'TransformationID': parentProd, 'Status': [ 'Unused', 'MaxReset' ]} )
    if not res['OK']:
      gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error getting Unused files from transformation %s:" % ( prod, parentProd ), res['Message'] )
      return res
    parentFiles = res['Value']
    lfns = [lfnDict['LFN'] for lfnDict in parentFiles]
    if not lfns:
      gLogger.info( "[None] [%d] .moveFilesToDerivedTransformation: No files found to be moved from transformation %d" % ( prod, parentProd ) )
      return S_OK( ( parentProd, movedFiles ) )
    # get the lfns of the derived production that were Unused/MaxReset in the parent one
    res = self.getTransformationFiles( condDict = { 'TransformationID': prod, 'LFN': lfns} )
    if not res['OK']:
      gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error getting files from derived transformation" % prod, res['Message'] )
      return res
    derivedFiles = res['Value']
    derivedStatusDict = dict( [( derivedDict['LFN'], derivedDict['Status'] ) for derivedDict in derivedFiles] )
    newStatusFiles = {}
    parentStatusFiles = {}
    badStatusFiles = {}
    for parentDict in parentFiles:
      lfn = parentDict['LFN']
      derivedStatus = derivedStatusDict.get( lfn )
      if derivedStatus:
        parentStatus = parentDict['Status']
        # By default move to the parent status (which is Unused or MaxReset)
        status = parentStatus
        moveStatus = parentStatus
        # For MaxReset, set Unused if requested
        if parentStatus == 'MaxReset':
          if resetUnused:
            status = 'Unused'
            moveStatus = 'Unused from MaxReset'
          else:
            status = 'MaxReset-inherited'
        if derivedStatus.endswith( '-inherited' ):
          # This is the general case
          newStatusFiles.setdefault( ( status, parentStatus ), [] ).append( lfn )
          movedFiles[moveStatus] = movedFiles.setdefault( moveStatus, 0 ) + 1
        else:
          badStatusFiles[derivedStatus] = badStatusFiles.setdefault( derivedStatus, 0 ) + 1
        if parentStatus == 'Unused':
          # If the file was Unused, set it NotProcessed in parent
          parentStatusFiles.setdefault( 'NotProcessed', [] ).append( lfn )
        else:
          parentStatusFiles.setdefault( 'Moved', [] ).append( lfn )

    for status, count in badStatusFiles.items():
      gLogger.warn( '[None] [%d] .moveFilesToDerivedTransformation: Files found in an unexpected status in derived transformation' % prod, '%s: %d' % ( status, count ) )
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
        res = self.setFileStatusForTransformation( prod, status, lfnChunk )
        if not res['OK']:
          gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error setting status %s for %d files; resetting them %s in transformation %d"
                         % ( prod, status, len( lfnChunk ), oldStatus, parentProd ),
                         res['Message'] )
          res = self.setFileStatusForTransformation( parentProd, oldStatus, lfnChunk )
          if not res['OK']:
            gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error setting status %s for %d files in transformation %d"
                           % ( prod, oldStatus, len( lfnChunk ), parentProd ),
                           res['Message'] )
        else:
          gLogger.info( "[None] [%d] .moveFilesToDerivedTransformation: Successfully moved %d files from %s to %s" % ( prod, len( lfnChunk ), oldStatus, status ) )

    # If files were Assigned or Unused at the time of derivation, try and update them as jobs may have run since then
    res = self.getTransformationFiles( condDict = {'TransformationID': prod, 'Status': [ 'Assigned-inherited', 'Unused-inherited' ]} )
    if res['OK']:
      assignedFiles = res['Value']
      if assignedFiles:
        lfns = [lfnDict['LFN'] for lfnDict in assignedFiles]
        res = self.getTransformationFiles( condDict = { 'TransformationID':parentProd, 'LFN':lfns} )
        if res['OK']:
          parentFiles = res['Value']
          processedLfns = [lfnDict['LFN'] for lfnDict in parentFiles if lfnDict['Status'] == 'Processed']
          if processedLfns:
            res = self.setFileStatusForTransformation( prod, 'Processed-inherited', processedLfns )
            if res['OK']:
              gLogger.info( "[None] [%d] .moveFilesToDerivedTransformation: set %d files to status %s" % ( prod, len( processedLfns ), 'Processed-inherited' ) )
    if not res['OK']:
      gLogger.error( "[None] [%d] .moveFilesToDerivedTransformation: Error setting status for Assigned derived files" % prod, res['Message'] )

    return S_OK( ( parentProd, movedFiles ) )

  def setFileStatusForTransformation( self, transName, newLFNsStatus = {}, lfns = [], force = False ):
    """ Sets the file status for LFNs of a transformation

        For backward compatibility purposes, the status and LFNs can be passed in 2 ways:

          - newLFNsStatus is a dictionary with the form:
            {'/this/is/an/lfn1.txt': 'StatusA', '/this/is/an/lfn2.txt': 'StatusB',  ... }
            and at this point lfns is not considered
          - newLFNStatus is a string, that applies to all the LFNs in lfns

    """
    rpcClient = self._getRPC()

    # create dictionary in case newLFNsStatus is a string
    if type( lfns ) == type( '' ):
      lfns = [lfns]
    if type( newLFNsStatus ) == type( '' ):
      newLFNsStatus = dict( [( lfn, newLFNsStatus ) for lfn in lfns ] )

    # gets status as of today
    tsFiles = self.getTransformationFiles( {'TransformationID':transName, 'LFN': newLFNsStatus.keys()} )
    if not tsFiles['OK']:
      return tsFiles
    tsFiles = tsFiles['Value']
    newStatuses = {}
    if tsFiles:
      # for convenience, makes a small dictionary out of the tsFiles, with the lfn as key
      tsFilesAsDict = {}
      for tsFile in tsFiles:
        tsFilesAsDict[tsFile['LFN']] = [tsFile['Status'], tsFile['ErrorCount'], tsFile['FileID']]

      # applying the state machine to the proposed status
      newStatuses = self._applyTransformationFilesStateMachine( tsFilesAsDict, newLFNsStatus, force )

      if newStatuses:  # if there's something to update
        # must do it for the file IDs...
        newStatusForFileIDs = dict( [( tsFilesAsDict[lfn][2], newStatuses[lfn] ) for lfn in newStatuses.keys()] )
        res = rpcClient.setFileStatusForTransformation( transName, newStatusForFileIDs )
        if not res['OK']:
          return res

    return S_OK( newStatuses )

  def _applyTransformationFilesStateMachine( self, tsFilesAsDict, dictOfProposedLFNsStatus, force ):
    """ For easier extension, here we apply the state machine of the production files.
        VOs might want to replace the standard here with something they prefer.

        tsFiles is a dictionary with the lfn as key and as value a list of [Status, ErrorCount, FileID]
        dictOfNewLFNsStatus is a dictionary with the proposed status
        force is a boolean

        It returns a dictionary with the status updates
    """
    newStatuses = {}

    for lfn in dictOfProposedLFNsStatus.keys():
      if lfn not in tsFilesAsDict.keys():
        continue
      else:
        newStatus = dictOfProposedLFNsStatus[lfn]
        # Apply optional corrections
        if tsFilesAsDict[lfn][0].lower() == 'processed' and dictOfProposedLFNsStatus[lfn].lower() != 'processed':
          if not force:
            newStatus = 'Processed'
        elif tsFilesAsDict[lfn][0].lower() == 'maxreset':
          if not force:
            newStatus = 'MaxReset'
        elif dictOfProposedLFNsStatus[lfn].lower() == 'unused':
          errorCount = tsFilesAsDict[lfn][1]
          # every 10 retries (by default)
          if errorCount and ( ( errorCount % self.maxResetCounter ) == 0 ):
            if not force:
              newStatus = 'MaxReset'

        if tsFilesAsDict[lfn][0].lower() != newStatus:
          newStatuses[lfn] = newStatus

    return newStatuses

  def setTransformationParameter( self, transID, paramName, paramValue, force = False ):
    """ Sets a transformation parameter. There's a special case when coming to setting the status of a transformation.
    """
    rpcClient = self._getRPC()

    if paramName.lower() == 'status':
      # get transformation Type
      transformation = self.getTransformation( transID )
      if not transformation['OK']:
        return transformation
      transformationType = transformation['Value']['Type']

      # get status as of today
      originalStatus = self.getTransformationParameters( transID, 'Status' )
      if not originalStatus['OK']:
        return originalStatus
      originalStatus = originalStatus['Value']

      transIDAsDict = {transID: [originalStatus, transformationType]}
      dictOfProposedstatus = {transID: paramValue}
      # applying the state machine to the proposed status
      value = self._applyTransformationStatusStateMachine( transIDAsDict, dictOfProposedstatus, force )
    else:
      value = paramValue

    return rpcClient.setTransformationParameter( transID, paramName, value )

  def _applyTransformationStatusStateMachine( self, transIDAsDict, dictOfProposedstatus, force ):
    """ For easier extension, here we apply the state machine of the transformation status.
        VOs might want to replace the standard here with something they prefer.

        transIDAsDict is a dictionary with the transID as key and as value a list with [Status, Type]
        dictOfProposedstatus is a dictionary with the proposed status
        force is a boolean

        It returns the new status (the standard is just doing nothing: everything is possible)
    """
    return dictOfProposedstatus.values()[0]

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

  def addDirectory( self, path, force = False ):
    rpcClient = self._getRPC()
    return rpcClient.addDirectory( path, force )

  def getReplicas( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    rpcClient = self._getRPC()
    return rpcClient.getReplicas( lfns )

  def addFile( self, lfn, force = False ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndicts = res['Value']
    rpcClient = self._getRPC()
    return rpcClient.addFile( lfndicts, force )

  def addReplica( self, lfn, force = False ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndicts = res['Value']
    rpcClient = self._getRPC()
    return rpcClient.addReplica( lfndicts, force )

  def removeFile( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    rpcClient = self._getRPC()
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

  def removeReplica( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndicts = res['Value']
    rpcClient = self._getRPC()
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

  def getReplicaStatus( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndict = res['Value']
    rpcClient = self._getRPC()
    return rpcClient.getReplicaStatus( lfndict )

  def setReplicaStatus( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndict = res['Value']
    rpcClient = self._getRPC()
    return rpcClient.setReplicaStatus( lfndict )

  def setReplicaHost( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfndict = res['Value']
    rpcClient = self._getRPC()
    return rpcClient.setReplicaHost( lfndict )

  def removeDirectory( self, lfn ):
    return self.__returnOK( lfn )

  def createDirectory( self, lfn, ):
    return self.__returnOK( lfn )

  def createLink( self, lfn ):
    return self.__returnOK( lfn )

  def removeLink( self, lfn ):
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
