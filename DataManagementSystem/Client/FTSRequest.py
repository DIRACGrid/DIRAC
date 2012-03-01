# $HeadURL$
__RCSID__ = "$Id$"
from DIRAC                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Grid                          import executeGridCommand
from DIRAC.Core.Utilities.File                          import checkGuid
from DIRAC.Core.Utilities.Adler                         import compareAdler
from DIRAC.Core.Utilities.List                          import sortList
from DIRAC.Core.Utilities.SiteSEMapping                 import getSitesForSE
from DIRAC.Core.Utilities.Time                          import dateTime, fromString
from DIRAC.Resources.Storage.StorageElement             import StorageElement
from DIRAC.DataManagementSystem.Client.ReplicaManager   import CatalogInterface
from DIRAC.AccountingSystem.Client.Types.DataOperation  import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient      import gDataStoreClient
import re, os, time, sys, tempfile, types

class FTSRequest:

  def __init__( self ):

    self.finalStates = ['Canceled', 'Failed', 'Hold', 'Finished', 'FinishedDirty']
    self.failedStates = ['Canceled', 'Failed', 'Hold', 'FinishedDirty']
    self.successfulStates = ['Finished', 'Done']
    self.fileStates = ['Done', 'Active', 'Pending', 'Ready', 'Canceled', 'Failed', 'Finishing', 'Finished', 'Submitted', 'Hold', 'Waiting']

    self.newlyCompletedFiles = []
    self.newlyFailedFiles = []

    self.statusSummary = {}
    self.requestStatus = 'Unknown'

    self.fileDict = {}
    self.catalogReplicas = {}
    self.catalogMetadata = {}
    self.failedRegistrations = {}

    self.oCatalog = None

    self.submitTime = ''

    self.ftsGUID = ''
    self.ftsServer = ''
    self.priority = 3
    self.isTerminal = False
    self.percentageComplete = 0.0

    self.sourceSE = ''
    self.sourceValid = False
    self.sourceToken = ''

    self.targetSE = ''
    self.targetValid = False
    self.targetToken = ''

    self.dumpStr = ''

  ####################################################################
  #
  #  Methods for setting/getting/checking the SEs
  #

  def setSourceSE( self, se ):
    if se == self.targetSE:
      return S_ERROR( "SourceSE is TargetSE" )
    self.sourceSE = se
    self.oSourceSE = StorageElement( self.sourceSE )
    return self.__checkSourceSE()

  def getSourceSE( self ):
    if not self.sourceSE:
      return S_ERROR( "Source SE not defined" )
    return S_OK( self.sourceSE )

  def setSourceToken( self, token ):
    self.sourceToken = token
    return S_OK()

  def getSourceToken( self ):
    if not self.sourceToken:
      return S_ERROR( "Source token not defined" )
    return S_OK( self.sourceToken )

  def __checkSourceSE( self ):
    if not self.sourceSE:
      return S_ERROR( "SourceSE not set" )
    res = self.oSourceSE.isValid( 'Read' )
    if not res['OK']:
      return S_ERROR( "SourceSE not available for reading" )
    res = self.__getSESpaceToken( self.oSourceSE )
    if not res['OK']:
      gLogger.error( "FTSRequest failed to get SRM Space Token for SourceSE", res['Message'] )
      return S_ERROR( "SourceSE does not support FTS transfers" )
    self.sourceToken = res['Value']
    self.sourceValid = True
    return S_OK()

  def setTargetSE( self, se ):
    if se == self.sourceSE:
      return S_ERROR( "TargetSE is SourceSE" )
    self.targetSE = se
    self.oTargetSE = StorageElement( self.targetSE )
    return self.__checkTargetSE()

  def getTargetSE( self ):
    if not self.targetSE:
      return S_ERROR( "Target SE not defined" )
    return S_OK( self.targetSE )

  def setTargetToken( self, token ):
    self.targetToken = token
    return S_OK()

  def getTargetToken( self ):
    if not self.targetToken:
      return S_ERROR( "Target token not defined" )
    return S_OK( self.targetToken )

  def __checkTargetSE( self ):
    if not self.targetSE:
      return S_ERROR( "TargetSE not set" )
    res = self.oTargetSE.isValid( 'Write' )
    if not res['OK']:
      return S_ERROR( "TargetSE not available for writing" )
    res = self.__getSESpaceToken( self.oTargetSE )
    if not res['OK']:
      gLogger.error( "FTSRequest failed to get SRM Space Token for TargetSE", res['Message'] )
      return S_ERROR( "SourceSE does not support FTS transfers" )
    self.targetToken = res['Value']
    self.targetValid = True
    return S_OK()

  def __getSESpaceToken( self, oSE ):
    res = oSE.getStorageParameters( "SRM2" )
    if not res['OK']:
      return res
    return S_OK( res['Value'].get( 'SpaceToken' ) )

  ####################################################################
  #
  #  Methods for setting/getting FTS request parameters
  #

  def setFTSGUID( self, guid ):
    if not checkGuid( guid ):
      return S_ERROR( "Incorrect GUID format" )
    self.ftsGUID = guid
    return S_OK()

  def getFTSGUID( self ):
    if not self.ftsGUID:
      return S_ERROR( "FTSGUID not set" )
    return S_OK( self.ftsGUID )

  def setFTSServer( self, server ):
    self.ftsServer = server
    return S_OK()

  def getFTSServer( self ):
    if not self.ftsServer:
      return S_ERROR( "FTSServer not set" )
    return S_OK( self.ftsServer )

  def setPriority( self, priority ):
    if not type( priority ) in [types.IntType, types.LongType]:
      return S_ERROR( "Priority must be integer" )
    if priority < 0:
      priority = 0
    elif priority > 5:
      priority = 5
    self.priority = priority
    return S_OK( self.priority )

  def getPriority( self ):
    return S_OK( self.priority )

  def getPercentageComplete( self ):
    completedFiles = 0
    totalFiles = 0
    for state in ( self.statusSummary.keys() ):
      if state in self.successfulStates:
        completedFiles += self.statusSummary[state]
      totalFiles += self.statusSummary[state]
    self.percentageComplete = ( float( completedFiles ) * 100.0 ) / float( totalFiles )
    return S_OK( self.percentageComplete )

  def isRequestTerminal( self ):
    if self.requestStatus in self.finalStates:
      self.isTerminal = True
    return S_OK( self.isTerminal )

  def getStatus( self ):
    return S_OK( self.requestStatus )

  ####################################################################
  #
  #  Methods for setting/getting/checking files and their metadata
  #

  def setLFN( self, lfn ):
    if not self.fileDict.has_key( lfn ):
      self.fileDict[lfn] = {}
    return S_OK()

  def setSourceSURL( self, lfn, surl ):
    target = self.fileDict[lfn].get( 'Target' )
    if target == surl:
      return S_ERROR( "Source and target the same" )
    self.__setFileParameter( lfn, 'Source', surl )
    return S_OK()

  def getSourceSURL( self, lfn ):
    return self.__getFileParameter( lfn, 'Source' )

  def setTargetSURL( self, lfn, surl ):
    source = self.fileDict[lfn].get( 'Source' )
    if source == surl:
      return S_ERROR( "Source and target the same" )
    self.__setFileParameter( lfn, 'Target', surl )
    return S_OK()

  def getTargetSURL( self, lfn ):
    return self.__getFileParameter( lfn, 'Target' )

  def getFailReason( self, lfn ):
    return self.__getFileParameter( lfn, 'Reason' )

  def getRetries( self, lfn ):
    return self.__getFileParameter( lfn, 'Retries' )

  def getTransferTime( self, lfn ):
    return self.__getFileParameter( lfn, 'Duration' )

  def getFailed( self ):
    failed = []
    for lfn in self.fileDict.keys():
      status = self.fileDict[lfn].get( 'Status', '' )
      if status in self.failedStates:
        failed.append( lfn )
    return S_OK( failed )

  def getDone( self ):
    done = []
    for lfn in self.fileDict.keys():
      status = self.fileDict[lfn].get( 'Status', '' )
      if status in self.successfulStates:
        done.append( lfn )
    return S_OK( done )

  def __setFileParameter( self, lfn, paramName, paramValue ):
    self.setLFN( lfn )
    self.fileDict[lfn][paramName] = paramValue
    return S_OK()

  def __getFileParameter( self, lfn, paramName ):
    if not self.fileDict.has_key( lfn ):
      return S_ERROR( "Supplied file not set" )
    if not self.fileDict[lfn].has_key( paramName ):
      return S_ERROR( "%s not set for file" % paramName )
    return S_OK( self.fileDict[lfn][paramName] )

  ####################################################################
  #
  #  Methods for submission
  #

  def submit( self, monitor = False, printOutput = True ):
    res = self.__isSubmissionValid()
    if not res['OK']:
      return res
    res = self.__createSURLPairFile()
    if not res['OK']:
      return res
    res = self.__submitFTSTransfer()
    if not res['OK']:
      return res
    resDict = {'ftsGUID':self.ftsGUID, 'ftsServer':self.ftsServer}
    print "Submitted %s @ %s" % ( self.ftsGUID, self.ftsServer )
    if monitor:
      self.monitor( untilTerminal = True, printOutput = printOutput )
    return S_OK( resDict )

  def __isSubmissionValid( self ):
    if not self.fileDict:
      return S_ERROR( "No files set" )
    if not self.sourceValid:
      return S_ERROR( "SourceSE not valid" )
    if not self.targetValid:
      return S_ERROR( "TargetSE not valid" )
    if not self.ftsServer:
      res = self.__resolveFTSServer()
      if not res['OK']:
        return S_ERROR( "FTSServer not valid" )
    self.__resolveSource()
    self.__resolveTarget()
    res = self.__filesToSubmit()
    if not res['OK']:
      return S_ERROR( "No files to submit" )
    return S_OK()

  def __getCatalogObject( self ):
    try:
      if not self.oCatalog:
        self.oCatalog = CatalogInterface()
      return S_OK()
    except:
      return S_ERROR()

  def __updateReplicaCache( self, lfns = [], overwrite = False ):
    if not lfns:
      lfns = self.fileDict.keys()
    toUpdate = []
    for lfn in lfns:
      if ( not lfn in self.catalogReplicas.keys() ) or overwrite:
        toUpdate.append( lfn )
    if not toUpdate:
      return S_OK()
    res = self.__getCatalogObject()
    if not res['OK']:
      return res
    res = self.oCatalog.getCatalogReplicas( toUpdate )
    if not res['OK']:
      return S_ERROR( "Failed to update replica cache", res['Message'] )
    for lfn, error in res['Value']['Failed'].items():
      self.__setFileParameter( lfn, 'Reason', error )
      self.__setFileParameter( lfn, 'Status', 'Failed' )
    for lfn, replicas in res['Value']['Successful'].items():
      self.catalogReplicas[lfn] = replicas
    return S_OK()

  def __updateMetadataCache( self, lfns = [], overwrite = False ):
    if not lfns:
      lfns = self.fileDict.keys()
    toUpdate = []
    for lfn in lfns:
      if ( not lfn in self.catalogMetadata.keys() ) or overwrite:
        toUpdate.append( lfn )
    if not toUpdate:
      return S_OK()
    res = self.__getCatalogObject()
    if not res['OK']:
      return res
    res = self.oCatalog.getCatalogFileMetadata( toUpdate )
    if not res['OK']:
      return S_ERROR( "Failed to get source catalog metadata", res['Message'] )
    for lfn, error in res['Value']['Failed'].items():
      self.__setFileParameter( lfn, 'Reason', error )
      self.__setFileParameter( lfn, 'Status', 'Failed' )
    for lfn, metadata in res['Value']['Successful'].items():
      self.catalogMetadata[lfn] = metadata
    return S_OK()

  def __resolveSource( self ):
    toResolve = []
    for lfn in self.fileDict.keys():
      if ( not self.fileDict[lfn].has_key( 'Source' ) ) and ( self.fileDict[lfn].get( 'Status' ) != 'Failed' ):
        toResolve.append( lfn )
    if not toResolve:
      return S_OK()
    res = self.__updateMetadataCache( toResolve )
    if not res['OK']:
      return res
    res = self.__updateReplicaCache( toResolve )
    if not res['OK']:
      return res
    for lfn in toResolve:
      if self.fileDict[lfn].get( 'Status' ) == 'Failed':
        continue
      replicas = self.catalogReplicas.get( lfn, {} )
      if not replicas.has_key( self.sourceSE ):
        self.__setFileParameter( lfn, 'Reason', "No replica at SourceSE" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue
      res = self.oSourceSE.getPfnForProtocol( replicas[self.sourceSE], 'SRM2', withPort = True )
      if not res['OK']:
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue
      res = self.setSourceSURL( lfn, res['Value'] )
      if not res['OK']:
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue

    toResolve = {}
    for lfn in self.fileDict.keys():
      if self.fileDict[lfn].has_key( 'Source' ):
        toResolve[self.fileDict[lfn]['Source']] = lfn
    if not toResolve:
      return S_ERROR( "No eligible Source files" )
    res = self.oSourceSE.getFileMetadata( toResolve.keys() )
    if not res['OK']:
      return S_ERROR( "Failed to check source file metadata" )
    for pfn, error in res['Value']['Failed'].items():
      lfn = toResolve[pfn]
      if re.search( 'File does not exist', error ):
        self.__setFileParameter( lfn, 'Reason', "Source file does not exist" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      else:
        self.__setFileParameter( lfn, 'Reason', "Failed to get Source metadata" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
    for pfn, metadata in res['Value']['Successful'].items():
      lfn = toResolve[pfn]
      if metadata['Unavailable']:
        self.__setFileParameter( lfn, 'Reason', "Source file Unavailable" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      elif metadata['Lost']:
        self.__setFileParameter( lfn, 'Reason', "Source file Lost" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      elif not metadata['Cached']:
        self.__setFileParameter( lfn, 'Reason', "Source file not Cached" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      elif metadata['Size'] != self.catalogMetadata[lfn]['Size']:
        self.__setFileParameter( lfn, 'Reason', "Source size mismatch" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      elif self.catalogMetadata[lfn]['Checksum'] and metadata['Checksum'] and not ( compareAdler( metadata['Checksum'], self.catalogMetadata[lfn]['Checksum'] ) ):
        self.__setFileParameter( lfn, 'Reason', "Source checksum mismatch" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
    return S_OK()

  def __resolveTarget( self ):
    toResolve = []
    for lfn in self.fileDict.keys():
      if not self.fileDict[lfn].has_key( 'Target' ) and ( self.fileDict[lfn].get( 'Status' ) != 'Failed' ):
        toResolve.append( lfn )
    if not toResolve:
      return S_OK()
    res = self.__updateReplicaCache( toResolve )
    if not res['OK']:
      return res
    atTarget = []
    for lfn in sortList( toResolve ):
      if self.fileDict[lfn].get( 'Status' ) == 'Failed':
        continue
      replicas = self.catalogReplicas.get( lfn, {} )
      if replicas.has_key( self.targetSE ):
        self.__setFileParameter( lfn, 'Reason', "File already at Target" )
        self.__setFileParameter( lfn, 'Status', 'Done' )
        atTarget.append( lfn )
    for lfn in toResolve:
      if ( self.fileDict[lfn].get( 'Status' ) == 'Failed' ) or ( lfn in atTarget ):
        continue
      res = self.oTargetSE.getPfnForLfn( lfn )
      if not res['OK']:
        self.__setFileParameter( lfn, 'Reason', "Failed to create Target" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue
      res = self.oTargetSE.getPfnForProtocol( res['Value'], 'SRM2', withPort = True )
      if not res['OK']:
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue
      res = self.setTargetSURL( lfn, res['Value'] )
      if not res['OK']:
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue
    toResolve = {}
    for lfn in self.fileDict.keys():
      if self.fileDict[lfn].has_key( 'Target' ):
        toResolve[self.fileDict[lfn]['Target']] = lfn
    if not toResolve:
      return S_ERROR( "No eligible Target files" )
    res = self.oTargetSE.exists( toResolve.keys() )
    if not res['OK']:
      return S_ERROR( "Failed to check target existence" )
    for pfn, error in res['Value']['Failed'].items():
      lfn = toResolve[pfn]
      self.__setFileParameter( lfn, 'Reason', error )
      self.__setFileParameter( lfn, 'Status', 'Failed' )
    toRemove = []
    for pfn, exists in res['Value']['Successful'].items():
      if exists:
        lfn = toResolve[pfn]
        res = self.getSourceSURL( lfn )
        if not res['OK']:
          self.__setFileParameter( lfn, 'Reason', "Target exists" )
          self.__setFileParameter( lfn, 'Status', 'Failed' )
        elif res['Value'] == pfn:
          self.__setFileParameter( lfn, 'Reason', "Source and Target the same" )
          self.__setFileParameter( lfn, 'Status', 'Failed' )
        else:
          toRemove.append( pfn )
    if toRemove:
      self.oTargetSE.removeFile( toRemove )
    return S_OK()

  def __filesToSubmit( self ):
    for lfn in self.fileDict.keys():
      lfnStatus = self.fileDict[lfn].get( 'Status' )
      source = self.fileDict[lfn].get( 'Source' )
      target = self.fileDict[lfn].get( 'Target' )
      if ( lfnStatus != 'Failed' ) and ( lfnStatus != 'Done' ) and source and target:
        return S_OK()
    return S_ERROR()

  def __createSURLPairFile( self ):
    fd, fileName = tempfile.mkstemp()
    surlFile = os.fdopen( fd, 'w' )
    for lfn in self.fileDict.keys():
      lfnStatus = self.fileDict[lfn].get( 'Status' )
      source = self.fileDict[lfn].get( 'Source' )
      target = self.fileDict[lfn].get( 'Target' )
      if ( lfnStatus != 'Failed' ) and ( lfnStatus != 'Done' ) and source and target:
        surlString = '%s %s\n' % ( source, target )
        surlFile.write( surlString )
    surlFile.close()
    self.surlFile = fileName
    return S_OK()

  def __submitFTSTransfer( self ):
    comm = ['glite-transfer-submit', '-s', self.ftsServer, '-f', self.surlFile, '-o']
    if self.targetToken:
      comm.append( '-t' )
      comm.append( self.targetToken )
    if self.sourceToken:
      comm.append( '-S' )
      comm.append( self.sourceToken )
    res = executeGridCommand( '', comm )
    os.remove( self.surlFile )
    if not res['OK']:
      return res
    returnCode, output, errStr = res['Value']
    if not returnCode == 0:
      return S_ERROR( errStr )
    guid = output.replace( '\n', '' )
    if not checkGuid( guid ):
      return S_ERROR( 'Wrong GUID format returned' )
    self.ftsGUID = guid
    #if self.priority != 3:
    #  comm = ['glite-transfer-setpriority','-s', self.ftsServer,self.ftsGUID,str(self.priority)]
    #  executeGridCommand('',comm)
    return res

  def __resolveFTSServer( self ):
    if not self.sourceSE:
      return S_ERROR( "Source SE not set" )
    if not self.targetSE:
      return S_ERROR( "Target SE not set" )
    res = getSitesForSE( self.sourceSE, 'LCG' )
    if not res['OK'] or not res['Value']:
      return S_ERROR( "Could not determine source site" )
    sourceSite = res['Value'][0]
    res = getSitesForSE( self.targetSE, 'LCG' )
    if not res['OK'] or not res['Value']:
      return S_ERROR( "Could not determine target site" )
    targetSite = res['Value'][0]

    if ( sourceSite == 'LCG.CERN.ch' ) or ( targetSite == 'LCG.CERN.ch' ):
      ep = 'LCG.CERN.ch'
    else:
      # Target site FTS server should be used
      ep = targetSite

    try:
      configPath = '/Resources/FTSEndpoints/%s' % ep
      endpointURL = gConfig.getValue( configPath )
      if not endpointURL:
        errStr = "FTSRequest.__resolveFTSEndpoint: Failed to find FTS endpoint, check CS entry for '%s'." % ep
        return S_ERROR( errStr )
      self.ftsServer = endpointURL
      return S_OK( endpointURL )
    except Exception, x:
      return S_ERROR( 'FTSRequest.__resolveFTSEndpoint: Failed to obtain endpoint details from CS' )

  ####################################################################
  #
  #  Methods for monitoring
  #

  def summary( self, untilTerminal = False, printOutput = False ):
    while not self.isTerminal:
      res = self.__parseOutput()
      if not res['OK']:
        return res
      if untilTerminal:
        self.__print()
      self.isRequestTerminal()
      if res['Value'] or ( not untilTerminal ):
        break
      time.sleep( 1 )
    if untilTerminal:
      print ""
    if printOutput and ( not untilTerminal ):
      return self.dumpSummary( printOutput = printOutput )
    return S_OK()

  def monitor( self, untilTerminal = False, printOutput = False ):
    res = self.__isMonitorValid()
    if not res['OK']:
      return res
    if untilTerminal:
      res = self.summary( untilTerminal = untilTerminal, printOutput = printOutput )
      if not res['OK']:
        return res
    res = self.__parseOutput( True )
    if not res['OK']:
      return res
    if untilTerminal:
      self.finalize()
    if printOutput:
      self.dump()
    return res

  def dumpSummary( self, printOutput = False ):
    outStr = ''
    for status in sortList( self.statusSummary.keys() ):
      if self.statusSummary[status]:
        outStr = '%s\t%s : %s\n' % ( outStr, status.ljust( 10 ), str( self.statusSummary[status] ).ljust( 10 ) )
    outStr = outStr.rstrip( '\n' )
    if printOutput:
      print outStr
    return S_OK( outStr )

  def __print( self ):
    self.getPercentageComplete()
    width = 100
    bits = int( ( width * self.percentageComplete ) / 100 )
    outStr = "|%s>%s| %.1f%s %s %s" % ( "="*bits, " "*( width - bits ), self.percentageComplete, "%", self.requestStatus, " "*10 )
    sys.stdout.write( "%s\r" % ( outStr ) )
    sys.stdout.flush()

  def dump( self ):
    print "%s : %s" % ( "Status".ljust( 10 ), self.requestStatus.ljust( 10 ) )
    print "%s : %s" % ( "Source".ljust( 10 ), self.sourceSE.ljust( 10 ) )
    print "%s : %s" % ( "Target".ljust( 10 ), self.targetSE.ljust( 10 ) )
    print "%s : %s" % ( "Server".ljust( 10 ), self.ftsServer.ljust( 100 ) )
    print "%s : %s" % ( "GUID".ljust( 10 ), self.ftsGUID.ljust( 100 ) )
    for lfn in sortList( self.fileDict.keys() ):
      print "\n  %s : %s" % ( 'LFN'.ljust( 15 ), lfn.ljust( 128 ) )
      for key in ['Source', 'Target', 'Status', 'Reason', 'Duration']:
        print "  %s : %s" % ( key.ljust( 15 ), str( self.fileDict[lfn].get( key ) ).ljust( 128 ) )
    return S_OK()

  def __isSummaryValid( self ):
    if not self.ftsServer:
      return S_ERROR( "FTSServer not set" )
    if not self.ftsGUID:
      return S_ERROR( "FTSGUID not set" )
    return S_OK()

  def __isMonitorValid( self ):
    res = self.__isSummaryValid()
    if not res['OK']:
      return res
    if not self.fileDict:
      return S_ERROR( "Files not set" )
    return S_OK()

  def __parseOutput( self, full = False ):
    if full:
      res = self.__isMonitorValid()
    else:
      res = self.__isSummaryValid()
    if not res['OK']:
      return res
    comm = ['glite-transfer-status', '--verbose', '-s', self.ftsServer, self.ftsGUID]
    if full:
      comm.append( '-l' )
    res = executeGridCommand( '', comm )
    if not res['OK']:
      return res
    returnCode, output, errStr = res['Value']
    # Returns a non zero status if error
    if not returnCode == 0:
      return S_ERROR( errStr )
    toRemove = ["'", "<", ">"]
    for char in toRemove:
      output = output.replace( char, '' )
    regExp = re.compile( "Status:\s+(\S+)" )
    self.requestStatus = re.search( regExp, output ).group( 1 )
    regExp = re.compile( "Submit time:\s+(\S+ \S+)" )
    self.submitTime = re.search( regExp, output ).group( 1 )
    self.statusSummary = {}
    for state in self.fileStates:
      regExp = re.compile( "\s+%s:\s+(\d+)" % state )
      self.statusSummary[state] = int( re.search( regExp, output ).group( 1 ) )
    if not full:
      return S_OK()
    regExp = re.compile( "[ ]+Source:[ ]+(\S+)\n[ ]+Destination:[ ]+(\S+)\n[ ]+State:[ ]+(\S+)\n[ ]+Retries:[ ]+(\d+)\n[ ]+Reason:[ ]+([\S ]+).+?[ ]+Duration:[ ]+(\d+)", re.S )
    fileInfo = re.findall( regExp, output )
    for source, target, status, retries, reason, duration in fileInfo:
      lfn = ''
      for candidate in sortList( self.fileDict.keys() ):
        if re.search( candidate, source ):
          lfn = candidate
      if not lfn:
        continue
      self.__setFileParameter( lfn, 'Source', source )
      self.__setFileParameter( lfn, 'Target', target )
      self.__setFileParameter( lfn, 'Status', status )
      if reason == '(null)':
        reason = ''
      self.__setFileParameter( lfn, 'Reason', reason.replace( "\n", " " ) )
      self.__setFileParameter( lfn, 'Duration', int( duration ) )
    return S_OK()

  ####################################################################
  #
  #  Methods for finalization
  #

  def finalize( self ):
    transEndTime = dateTime()
    regStartTime = time.time()
    res = self.getTransferStatistics()
    transDict = res['Value']

    res = self.__registerSuccessful( transDict['transLFNs'] )

    regSuc, regTotal = res['Value']
    regTime = time.time() - regStartTime
    if self.sourceSE and self.targetSE:
      self.__sendAccounting( regSuc, regTotal, regTime, transEndTime, transDict )
    self.__removeFailedTargets()
    self.__determineMissingSource()
    return S_OK()

  def getTransferStatistics( self ):
    """
     Get information of Transfers, it can be used by Accounting
    """
    transDict = { 'transTotal': len( self.fileDict ),
                  'transLFNs': [],
                  'transOK': 0,
                  'transSize': 0 }

    for lfn in self.fileDict.keys():
      if self.fileDict[lfn].get( 'Status' ) in self.successfulStates:
        if self.fileDict[lfn].get( 'Duration', 0 ):
          transDict['transLFNs'].append( lfn )
          transDict['transOK'] += 1
          if lfn in self.catalogMetadata:
            transDict['transSize'] += self.catalogMetadata[lfn].get( 'Size', 0 )

    return S_OK( transDict )

  def getFailedRegistrations( self ):
    return S_OK( self.failedRegistrations )

  def __registerSuccessful( self, transLFNs ):
    self.failedRegistrations = {}
    toRegister = {}
    for lfn in transLFNs:
      res = self.oTargetSE.getPfnForProtocol( self.fileDict[lfn].get( 'Target' ), 'SRM2', withPort = False )
      if not res['OK']:
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      else:
        toRegister[lfn] = {'PFN':res['Value'], 'SE':self.targetSE}
    if not toRegister:
      return S_OK( ( 0, 0 ) )
    res = self.__getCatalogObject()
    if not res['OK']:
      for lfn in toRegister.keys():
        self.failedRegistrations = toRegister
        gLogger.error( 'Failed to get Catalog Object', res['Message'] )
        return S_OK( ( 0, len( toRegister ) ) )
    res = self.oCatalog.addCatalogReplica( toRegister )
    if not res['OK']:
      self.failedRegistrations = toRegister
      gLogger.error( 'Failed to get Catalog Object', res['Message'] )
      return S_OK( ( 0, len( toRegister ) ) )
    for lfn, error in res['Value']['Failed'].items():
      self.failedRegistrations[lfn] = toRegister[lfn]
      gLogger.error( 'Registration of Replica failed', '%s : %s' % ( lfn, str( error ) ) )
    return S_OK( ( len( res['Value']['Successful'] ), len( toRegister ) ) )

  def __sendAccounting( self, regSuc, regTotal, regTime, transEndTime, transDict ):

    submitTime = fromString( self.submitTime )
    oAccounting = DataOperation()
    oAccounting.setEndTime( transEndTime )
    oAccounting.setStartTime( submitTime )

    accountingDict = {}
    accountingDict['OperationType'] = 'replicateAndRegister'
    accountingDict['User'] = 'acsmith'
    accountingDict['Protocol'] = 'FTS'
    accountingDict['RegistrationTime'] = regTime
    accountingDict['RegistrationOK'] = regSuc
    accountingDict['RegistrationTotal'] = regTotal
    accountingDict['TransferOK'] = transDict['transOK']
    accountingDict['TransferTotal'] = transDict['transTotal']
    accountingDict['TransferSize'] = transDict['transSize']
    accountingDict['FinalStatus'] = self.requestStatus
    accountingDict['Source'] = self.sourceSE
    accountingDict['Destination'] = self.targetSE
    c = transEndTime - submitTime
    transferTime = c.days * 86400 + c.seconds
    accountingDict['TransferTime'] = transferTime
    oAccounting.setValuesFromDict( accountingDict )
    gLogger.verbose( "Attempting to commit accounting message..." )
    oAccounting.commit()
    gLogger.verbose( "...committed." )
    return S_OK()

  def __removeFailedTargets( self ):
    corruptTargetErrors = ['file exists',
                           'FILE_EXISTS',
                           'Device or resource busy',
                           'Marking Space as Being Used failed',
                           'Another prepareToPut/Update is ongoing for this file',
                           'Requested file is still in SRM_SPACE_AVAILABLE state!' ]
    corruptedTarget = []
    for lfn in sortList( self.fileDict.keys() ):
      if self.fileDict[lfn].get( 'Status', '' ) == 'Failed':
        reason = self.fileDict[lfn].get( 'Reason', '' )
        for error in corruptTargetErrors:
          if ( type( reason ) == type( '' ) ) and re.search( error, reason ):
            corruptedTarget.append( self.fileDict[lfn].get( 'Target' ) )
    if corruptedTarget:
      self.oTargetSE.removeFile( corruptedTarget )

  def __determineMissingSource( self ):
    missingSourceErrors = ['SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] Failed',
                           'SOURCE error during TRANSFER_PREPARATION phase: \[INVALID_PATH\] No such file or directory',
                           'SOURCE error during PREPARATION phase: \[INVALID_PATH\] Failed',
                           'SOURCE error during PREPARATION phase: \[INVALID_PATH\] The requested file either does not exist',
                           'TRANSFER error during TRANSFER phase: \[INVALID_PATH\] the server sent an error response: 500 500 Command failed. : open error: No such file or directory',
                           'SOURCE error during TRANSFER_PREPARATION phase: \[USER_ERROR\] source file doesnt exist']
    missingSource = []
    for lfn in sortList( self.fileDict.keys() ):
      if self.fileDict[lfn].get( 'Status', '' ) == 'Failed':
        reason = self.fileDict[lfn].get( 'Reason', '' )
        for error in missingSourceErrors:
          if ( type( reason ) == type( '' ) ) and re.search( error, reason ):
            missingSource.append( lfn )
    return missingSource
