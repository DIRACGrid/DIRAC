#############################################################################
# $HeadURL$
#############################################################################
""" ..mod: FTSRequest
    =================

    Helper class to perform FTS job submission and monitoring.

"""
# # imports
import sys
import re
import time
# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File import checkGuid
from DIRAC.Core.Utilities.Adler import compareAdler, intAdlerToHex, hexAdlerToInt
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.Core.Utilities.Time import dateTime
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Resources.Catalog.FileCatalog    import FileCatalog
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile

# # RCSID
__RCSID__ = "$Id$"

class FTSRequest( object ):
  """
  .. class:: FTSRequest

  Helper class for FTS job submission and monitoring.
  """

  # # default checksum type
  __defaultCksmType = "ADLER32"
  # # flag to disablr/enable checksum test, default: disabled
  __cksmTest = False

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    self.log = gLogger.getSubLogger( self.__class__.__name__, True )

    # # final states tuple
    self.finalStates = ( 'Canceled', 'Failed', 'Hold',
                         'Finished', 'FinishedDirty' )
    # # failed states tuple
    self.failedStates = ( 'Canceled', 'Failed',
                          'Hold', 'FinishedDirty' )
    # # successful states tuple
    self.successfulStates = ( 'Finished', 'Done' )
    # # all file states tuple
    self.fileStates = ( 'Done', 'Active', 'Pending', 'Ready', 'Canceled', 'Failed',
                        'Finishing', 'Finished', 'Submitted', 'Hold', 'Waiting' )

    self.statusSummary = {}

    # # request status
    self.requestStatus = 'Unknown'

    # # dict for FTS job files
    self.fileDict = {}
    # # dict for replicas information
    self.catalogReplicas = {}
    # # dict for metadata information
    self.catalogMetadata = {}
    # # dict for files that failed to register
    self.failedRegistrations = {}

    # # placehoder for FileCatalog reference
    self.oCatalog = None

    # # submit timestamp
    self.submitTime = ''

    # # placeholder FTS job GUID
    self.ftsGUID = ''
    # # placeholder for FTS server URL
    self.ftsServer = ''

    # # flag marking FTS job completness
    self.isTerminal = False
    # # completness percentage
    self.percentageComplete = 0.0

    # # source SE name
    self.sourceSE = ''
    # # flag marking source SE validity
    self.sourceValid = False
    # # source space token
    self.sourceToken = ''

    # # target SE name
    self.targetSE = ''
    # # flag marking target SE validity
    self.targetValid = False
    # # target space token
    self.targetToken = ''

    # # placeholder for target StorageElement
    self.oTargetSE = None
    # # placeholder for source StorageElement
    self.oSourceSE = None

    # # checksum type, set it to default
    self.__cksmType = self.__defaultCksmType
    # # disable checksum test by default
    self.__cksmTest = False

    # # statuses that prevent submitting to FTS
    self.noSubmitStatus = ( 'Failed', 'Done', 'Staging' )

    # # were sources resolved?
    self.sourceResolved = False

    # # Number of file transfers actually submitted
    self.submittedFiles = 0
    self.transferTime = 0

    self.submitCommand = Operations().getValue( 'DataManagement/FTSPlacement/FTS2/SubmitCommand', 'glite-transfer-submit' )
    self.monitorCommand = Operations().getValue( 'DataManagement/FTSPlacement/FTS2/MonitorCommand', 'glite-transfer-status' )
    self.ftsJob = None
    self.ftsFiles = []

  ####################################################################
  #
  #  Methods for setting/getting/checking the SEs
  #

  def setSourceSE( self, se ):
    """ set SE for source

    :param self: self reference
    :param str se: source SE name
    """
    if se == self.targetSE:
      return S_ERROR( "SourceSE is TargetSE" )
    self.sourceSE = se
    self.oSourceSE = StorageElement( self.sourceSE )
    return self.__checkSourceSE()

  def __checkSourceSE( self ):
    """ check source SE availability

    :param self: self reference
    """
    if not self.sourceSE:
      return S_ERROR( "SourceSE not set" )
    res = self.oSourceSE.isValid( 'Read' )
    if not res['OK']:
      return S_ERROR( "SourceSE not available for reading" )
    res = self.__getSESpaceToken( self.oSourceSE )
    if not res['OK']:
      self.log.error( "FTSRequest failed to get SRM Space Token for SourceSE", res['Message'] )
      return S_ERROR( "SourceSE does not support FTS transfers" )

    if self.__cksmTest:
      res = self.oSourceSE.getChecksumType()
      if not res["OK"]:
        self.log.error( "Unable to get checksum type for SourceSE", 
                        "%s: %s" % ( self.sourceSE, res["Message"] ) )
        cksmType = res["Value"]
        if cksmType in ( "NONE", "NULL" ):
          self.log.warn( "Checksum type set to %s at SourceSE %s, disabling checksum test" % ( cksmType,
                                                                                              self.sourceSE ) )
          self.__cksmTest = False
        elif cksmType != self.__cksmType:
          self.log.warn( "Checksum type mismatch, disabling checksum test" )
          self.__cksmTest = False

    self.sourceToken = res['Value']
    self.sourceValid = True
    return S_OK()

  def setTargetSE( self, se ):
    """ set target SE

    :param self: self reference
    :param str se: target SE name
    """
    if se == self.sourceSE:
      return S_ERROR( "TargetSE is SourceSE" )
    self.targetSE = se
    self.oTargetSE = StorageElement( self.targetSE )
    return self.__checkTargetSE()

  def setTargetToken( self, token ):
    """ target space token setter

    :param self: self reference
    :param str token: target space token
    """
    self.targetToken = token
    return S_OK()

  def __checkTargetSE( self ):
    """ check target SE availability

    :param self: self reference
    """
    if not self.targetSE:
      return S_ERROR( "TargetSE not set" )
    res = self.oTargetSE.isValid( 'Write' )
    if not res['OK']:
      return S_ERROR( "TargetSE not available for writing" )
    res = self.__getSESpaceToken( self.oTargetSE )
    if not res['OK']:
      self.log.error( "FTSRequest failed to get SRM Space Token for TargetSE", res['Message'] )
      return S_ERROR( "TargetSE does not support FTS transfers" )

    # # check checksum types
    if self.__cksmTest:
      res = self.oTargetSE.getChecksumType()
      if not res["OK"]:
        self.log.error( "Unable to get checksum type for TargetSE", 
                        "%s: %s" % ( self.targetSE, res["Message"] ) )
        cksmType = res["Value"]
        if cksmType in ( "NONE", "NULL" ):
          self.log.warn( "Checksum type set to %s at TargetSE %s, disabling checksum test" % ( cksmType,
                                                                                              self.targetSE ) )
          self.__cksmTest = False
        elif cksmType != self.__cksmType:
          self.log.warn( "Checksum type mismatch, disabling checksum test" )
          self.__cksmTest = False

    self.targetToken = res['Value']
    self.targetValid = True
    return S_OK()

  @staticmethod
  def __getSESpaceToken( oSE ):
    """ get space token from StorageElement instance

    :param self: self reference
    :param StorageElement oSE: StorageElement instance
    """
    res = oSE.getStorageParameters( "SRM2" )
    if not res['OK']:
      return res
    return S_OK( res['Value'].get( 'SpaceToken' ) )

  ####################################################################
  #
  #  Methods for setting/getting FTS request parameters
  #

  def setFTSGUID( self, guid ):
    """ FTS job GUID setter

    :param self: self reference
    :param str guid: string containg GUID
    """
    if not checkGuid( guid ):
      return S_ERROR( "Incorrect GUID format" )
    self.ftsGUID = guid
    return S_OK()


  def setFTSServer( self, server ):
    """ FTS server setter

    :param self: self reference
    :param str server: FTS server URL
    """
    self.ftsServer = server
    return S_OK()

  def isRequestTerminal( self ):
    """ check if FTS job has terminated

    :param self: self reference
    """
    if self.requestStatus in self.finalStates:
      self.isTerminal = True
    return S_OK( self.isTerminal )

  def setCksmTest( self, cksmTest = False ):
    """ set cksm test

    :param self: self reference
    :param bool cksmTest: flag to enable/disable checksum test
    """
    self.__cksmTest = bool( cksmTest )
    return S_OK( self.__cksmTest )

  ####################################################################
  #
  #  Methods for setting/getting/checking files and their metadata
  #

  def setLFN( self, lfn ):
    """ add LFN :lfn: to :fileDict:

    :param self: self reference
    :param str lfn: LFN to add to
    """
    self.fileDict.setdefault( lfn, {'Status':'Waiting'} )
    return S_OK()

  def setSourceSURL( self, lfn, surl ):
    """ source SURL setter

    :param self: self reference
    :param str lfn: LFN
    :param str surl: source SURL
    """
    target = self.fileDict[lfn].get( 'Target' )
    if target == surl:
      return S_ERROR( "Source and target the same" )
    return self.__setFileParameter( lfn, 'Source', surl )

  def getSourceSURL( self, lfn ):
    """ get source SURL for LFN :lfn:

    :param self: self reference
    :param str lfn: LFN
    """
    return self.__getFileParameter( lfn, 'Source' )

  def setTargetSURL( self, lfn, surl ):
    """ set target SURL for LFN :lfn:

    :param self: self reference
    :param str lfn: LFN
    :param str surl: target SURL
    """
    source = self.fileDict[lfn].get( 'Source' )
    if source == surl:
      return S_ERROR( "Source and target the same" )
    return self.__setFileParameter( lfn, 'Target', surl )

  def getFailReason( self, lfn ):
    """ get fail reason for file :lfn:

    :param self: self reference
    :param str lfn: LFN
    """
    return self.__getFileParameter( lfn, 'Reason' )

  def getRetries( self, lfn ):
    """ get number of attepmts made to transfer file :lfn:

    :param self: self reference
    :param str lfn: LFN
    """
    return self.__getFileParameter( lfn, 'Retries' )

  def getTransferTime( self, lfn ):
    """ get duration of transfer for file :lfn:

    :param self: self reference
    :param str lfn: LFN
    """
    return self.__getFileParameter( lfn, 'Duration' )

  def getFailed( self ):
    """ get list of wrongly transferred LFNs

    :param self: self reference
    """
    return S_OK( [ lfn for lfn in self.fileDict
                   if self.fileDict[lfn].get( 'Status', '' ) in self.failedStates ] )

  def getStaging( self ):
    """ get files set for prestaging """
    return S_OK( [lfn for lfn in self.fileDict
                  if self.fileDict[lfn].get( 'Status', '' ) == 'Staging'] )

  def getDone( self ):
    """ get list of succesfully transferred LFNs

    :param self: self reference
    """
    return S_OK( [ lfn for lfn in self.fileDict
                   if self.fileDict[lfn].get( 'Status', '' ) in self.successfulStates ] )

  def __setFileParameter( self, lfn, paramName, paramValue ):
    """ set :paramName: to :paramValue: for :lfn: file

    :param self: self reference
    :param str lfn: LFN
    :param str paramName: parameter name
    :param mixed paramValue: a new parameter value
    """
    self.setLFN( lfn )
    self.fileDict[lfn][paramName] = paramValue
    return S_OK()

  def __getFileParameter( self, lfn, paramName ):
    """ get value of :paramName: for file :lfn:

    :param self: self reference
    :param str lfn: LFN
    :param str paramName: parameter name
    """
    if lfn not in self.fileDict:
      return S_ERROR( "Supplied file not set" )
    if paramName not in self.fileDict[lfn]:
      return S_ERROR( "%s not set for file" % paramName )
    return S_OK( self.fileDict[lfn][paramName] )

  ####################################################################
  #
  #  Methods for submission
  #

  def submit( self, monitor = False, printOutput = True ):
    """ submit FTS job

    :param self: self reference
    :param bool monitor: flag to monitor progress of FTS job
    :param bool printOutput: flag to print output of execution to stdout
    """
    res = self.__prepareForSubmission()
    if not res['OK']:
      return res
    res = self.__submitFTSTransfer()
    if not res['OK']:
      return res
    resDict = { 'ftsGUID' : self.ftsGUID, 'ftsServer' : self.ftsServer, 'submittedFiles' : self.submittedFiles }
    if monitor or printOutput:
      gLogger.always( "Submitted %s@%s" % ( self.ftsGUID, self.ftsServer ) )
      if monitor:
        self.monitor( untilTerminal = True, printOutput = printOutput, full = False )
    return S_OK( resDict )

  def __prepareForSubmission( self ):
    """ check validity of job before submission

    :param self: self reference
    """
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
    self.resolveSource()
    self.resolveTarget()
    res = self.__filesToSubmit()
    if not res['OK']:
      return S_ERROR( "No files to submit" )
    return S_OK()

  def __getCatalogObject( self ):
    """ CatalogInterface instance facade

    :param self: self reference
    """
    try:
      if not self.oCatalog:
        self.oCatalog = FileCatalog()
      return S_OK()
    except:
      return S_ERROR()

  def __updateReplicaCache( self, lfns = None, overwrite = False ):
    """ update replica cache for list of :lfns:

    :param self: self reference
    :param mixed lfns: list of LFNs
    :param bool overwrite: flag to trigger cache clearing and updating
    """
    if not lfns:
      lfns = self.fileDict.keys()
    toUpdate = [ lfn for lfn in lfns if ( lfn not in self.catalogReplicas ) or overwrite ]
    if not toUpdate:
      return S_OK()
    res = self.__getCatalogObject()
    if not res['OK']:
      return res
    res = self.oCatalog.getReplicas( toUpdate )
    if not res['OK']:
      return S_ERROR( "Failed to update replica cache: %s" % res['Message'] )
    for lfn, error in res['Value']['Failed'].items():
      self.__setFileParameter( lfn, 'Reason', error )
      self.__setFileParameter( lfn, 'Status', 'Failed' )
    for lfn, replicas in res['Value']['Successful'].items():
      self.catalogReplicas[lfn] = replicas
    return S_OK()

  def __updateMetadataCache( self, lfns = None ):
    """ update metadata cache for list of LFNs

    :param self: self reference
    :param list lnfs: list of LFNs
    """
    if not lfns:
      lfns = self.fileDict.keys()
    toUpdate = [ lfn for lfn in lfns if lfn not in self.catalogMetadata ]
    if not toUpdate:
      return S_OK()
    res = self.__getCatalogObject()
    if not res['OK']:
      return res
    res = self.oCatalog.getFileMetadata( toUpdate )
    if not res['OK']:
      return S_ERROR( "Failed to get source catalog metadata: %s" % res['Message'] )
    for lfn, error in res['Value']['Failed'].items():
      self.__setFileParameter( lfn, 'Reason', error )
      self.__setFileParameter( lfn, 'Status', 'Failed' )
    for lfn, metadata in res['Value']['Successful'].items():
      self.catalogMetadata[lfn] = metadata
    return S_OK()

  def resolveSource( self ):
    """ resolve source SE eligible for submission

    :param self: self reference
    """

    # Avoid resolving sources twice
    if self.sourceResolved:
      return S_OK()
    # Only resolve files that need a transfer
    toResolve = [ lfn for lfn in self.fileDict if self.fileDict[lfn].get( "Status", "" ) != "Failed" ]
    if not toResolve:
      return S_OK()
    res = self.__updateMetadataCache( toResolve )
    if not res['OK']:
      return res
    res = self.__updateReplicaCache( toResolve )
    if not res['OK']:
      return res

    # Define the source URLs
    for lfn in toResolve:
      replicas = self.catalogReplicas.get( lfn, {} )
      if self.sourceSE not in replicas:
        gLogger.warn( "resolveSource: skipping %s - not replicas at SourceSE %s" % ( lfn, self.sourceSE ) )
        self.__setFileParameter( lfn, 'Reason', "No replica at SourceSE" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue

      res = returnSingleResult( self.oSourceSE.getURL( lfn, protocol = 'srm' ) )
      if not res['OK']:
        gLogger.warn( "resolveSource: skipping %s - %s" % ( lfn, res["Message"] ) )
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue
      res = self.setSourceSURL( lfn, res['Value'] )
      if not res['OK']:
        gLogger.warn( "resolveSource: skipping %s - %s" % ( lfn, res["Message"] ) )
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue

    toResolve = []
    for lfn in self.fileDict:
      if "Source" in self.fileDict[lfn]:
        toResolve.append( lfn )
    if not toResolve:
      return S_ERROR( "No eligible Source files" )

    # Get metadata of the sources, to check for existance, availability and caching
    res = self.oSourceSE.getFileMetadata( toResolve )
    if not res['OK']:
      return S_ERROR( "Failed to check source file metadata" )

    for lfn, error in res['Value']['Failed'].items():
      if re.search( 'File does not exist', error ):
        gLogger.warn( "resolveSource: skipping %s - source file does not exists" % lfn )
        self.__setFileParameter( lfn, 'Reason', "Source file does not exist" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      else:
        gLogger.warn( "resolveSource: skipping %s - failed to get source metadata" % lfn )
        self.__setFileParameter( lfn, 'Reason', "Failed to get Source metadata" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
    toStage = []

    nbStagedFiles = 0
    for lfn, metadata in res['Value']['Successful'].items():
      lfnStatus = self.fileDict.get( lfn, {} ).get( 'Status' )
      if metadata['Unavailable']:
        gLogger.warn( "resolveSource: skipping %s - source file unavailable" % lfn )
        self.__setFileParameter( lfn, 'Reason', "Source file Unavailable" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      elif metadata['Lost']:
        gLogger.warn( "resolveSource: skipping %s - source file lost" % lfn )
        self.__setFileParameter( lfn, 'Reason', "Source file Lost" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      elif not metadata['Cached']:
        if lfnStatus != 'Staging':
          toStage.append( lfn )
      elif metadata['Size'] != self.catalogMetadata[lfn]['Size']:
        gLogger.warn( "resolveSource: skipping %s - source file size mismatch" % lfn )
        self.__setFileParameter( lfn, 'Reason', "Source size mismatch" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      elif self.catalogMetadata[lfn]['Checksum'] and metadata['Checksum'] and \
            not compareAdler( metadata['Checksum'], self.catalogMetadata[lfn]['Checksum'] ):
        gLogger.warn( "resolveSource: skipping %s - source file checksum mismatch" % lfn )
        self.__setFileParameter( lfn, 'Reason', "Source checksum mismatch" )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      elif lfnStatus == 'Staging':
        # file that was staging is now cached
        self.__setFileParameter( lfn, 'Status', 'Waiting' )
        nbStagedFiles += 1

    # Some files were being staged
    if nbStagedFiles:
      self.log.info( 'resolveSource: %d files have been staged' % nbStagedFiles )

    # Launching staging of files not in cache
    if toStage:
      gLogger.warn( "resolveSource: %s source files not cached, prestaging..." % len( toStage ) )
      stage = self.oSourceSE.prestageFile( toStage )
      if not stage["OK"]:
        gLogger.error( "resolveSource: error is prestaging", stage["Message"] )
        for lfn in toStage:
          self.__setFileParameter( lfn, 'Reason', stage["Message"] )
          self.__setFileParameter( lfn, 'Status', 'Failed' )
      else:
        for lfn in toStage:
          if lfn in stage['Value']['Successful']:
            self.__setFileParameter( lfn, 'Status', 'Staging' )
          elif lfn in stage['Value']['Failed']:
            self.__setFileParameter( lfn, 'Reason', stage['Value']['Failed'][lfn] )
            self.__setFileParameter( lfn, 'Status', 'Failed' )

    self.sourceResolved = True
    return S_OK()

  def resolveTarget( self ):
    """ find target SE eligible for submission

    :param self: self reference
    """
    toResolve = [ lfn for lfn in self.fileDict
                 if self.fileDict[lfn].get( 'Status' ) not in self.noSubmitStatus ]
    if not toResolve:
      return S_OK()
    res = self.__updateReplicaCache( toResolve )
    if not res['OK']:
      return res
    for lfn in toResolve:
      res = returnSingleResult( self.oTargetSE.getURL( lfn, protocol = 'srm' ) )
      if not res['OK']:
        reason = res.get( 'Message', res['Message'] )
        gLogger.warn( "resolveTarget: skipping %s - %s" % ( lfn, reason ) )
        self.__setFileParameter( lfn, 'Reason', reason )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue

      res = self.setTargetSURL( lfn, res['Value'] )
      if not res['OK']:
        gLogger.warn( "resolveTarget: skipping %s - %s" % ( lfn, res["Message"] ) )
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
        continue
    toResolve = []
    for lfn in self.fileDict:
      if "Target" in self.fileDict[lfn]:
        toResolve.append( lfn )
    if not toResolve:
      return S_ERROR( "No eligible Target files" )
    res = self.oTargetSE.exists( toResolve )
    if not res['OK']:
      return S_ERROR( "Failed to check target existence" )
    for lfn, error in res['Value']['Failed'].items():
      self.__setFileParameter( lfn, 'Reason', error )
      self.__setFileParameter( lfn, 'Status', 'Failed' )
    toRemove = []
    for lfn, exists in res['Value']['Successful'].items():
      if exists:
        res = self.getSourceSURL( lfn )
        if not res['OK']:
          gLogger.warn( "resolveTarget: skipping %s - target exists" % lfn )
          self.__setFileParameter( lfn, 'Reason', "Target exists" )
          self.__setFileParameter( lfn, 'Status', 'Failed' )
        elif res['Value'] == self.fileDict[lfn]['Target']:
          gLogger.warn( "resolveTarget: skipping %s - source and target pfns are the same" % lfn )
          self.__setFileParameter( lfn, 'Reason', "Source and Target the same" )
          self.__setFileParameter( lfn, 'Status', 'Failed' )
        else:
          toRemove.append( lfn )
    if toRemove:
      self.oTargetSE.removeFile( toRemove )
    return S_OK()

  def __filesToSubmit( self ):
    """
    check if there is at least one file to submit

    :return: S_OK if at least one file is present, S_ERROR otherwise
    """
    for lfn in self.fileDict:
      lfnStatus = self.fileDict[lfn].get( 'Status' )
      source = self.fileDict[lfn].get( 'Source' )
      target = self.fileDict[lfn].get( 'Target' )
      if lfnStatus not in self.noSubmitStatus and source and target:
        return S_OK()
    return S_ERROR()

  def __createFTSFiles( self ):
    """ create LFNs file for glite-transfer-submit command

    This file consists one line for each fiel to be transferred:

    sourceSURL targetSURL [CHECKSUMTYPE:CHECKSUM]

    :param self: self reference
    """
    self.__updateMetadataCache()
    for lfn in self.fileDict:
      lfnStatus = self.fileDict[lfn].get( 'Status' )
      if lfnStatus not in self.noSubmitStatus:
        cksmStr = ""
        # # add chsmType:cksm only if cksmType is specified, else let FTS decide by itself
        if self.__cksmTest and self.__cksmType:
          checkSum = self.catalogMetadata.get( lfn, {} ).get( 'Checksum' )
          if checkSum:
            cksmStr = " %s:%s" % ( self.__cksmType, intAdlerToHex( hexAdlerToInt( checkSum ) ) )
        ftsFile = FTSFile()
        ftsFile.LFN = lfn
        ftsFile.SourceSURL = self.fileDict[lfn].get( 'Source' )
        ftsFile.TargetSURL = self.fileDict[lfn].get( 'Target' )
        ftsFile.SourceSE = self.sourceSE
        ftsFile.TargetSE = self.targetSE
        ftsFile.Status = self.fileDict[lfn].get( 'Status' )
        ftsFile.Checksum = cksmStr
        ftsFile.Size = self.catalogMetadata.get( lfn, {} ).get( 'Size' )
        self.ftsFiles.append( ftsFile )
        self.submittedFiles += 1
    return S_OK()

  def __createFTSJob( self, guid = None ):
    self.__createFTSFiles()
    ftsJob = FTSJob()
    ftsJob.RequestID = 0
    ftsJob.OperationID = 0
    ftsJob.SourceSE = self.sourceSE
    ftsJob.TargetSE = self.targetSE
    ftsJob.SourceToken = self.sourceToken
    ftsJob.TargetToken = self.targetToken
    ftsJob.FTSServer = self.ftsServer
    if guid:
      ftsJob.FTSGUID = guid

    for ftsFile in self.ftsFiles:
      ftsFile.Attempt += 1
      ftsFile.Error = ""
      ftsJob.addFile( ftsFile )
    self.ftsJob = ftsJob

  def __submitFTSTransfer( self ):
    """ create and execute glite-transfer-submit CLI command

    :param self: self reference
    """
    log = gLogger.getSubLogger( 'Submit' )
    self.__createFTSJob()

    submit = self.ftsJob.submitFTS2( command = self.submitCommand )
    if not submit["OK"]:
      log.error( "unable to submit FTSJob: %s" % submit["Message"] )
      return submit

    log.info( "FTSJob '%s'@'%s' has been submitted" % ( self.ftsJob.FTSGUID, self.ftsJob.FTSServer ) )

    # # update statuses for job files
    for ftsFile in self.ftsJob:
      ftsFile.FTSGUID = self.ftsJob.FTSGUID
      ftsFile.Status = "Submitted"
      ftsFile.Attempt += 1

    log.info( "FTSJob '%s'@'%s' has been submitted" % ( self.ftsJob.FTSGUID, self.ftsJob.FTSServer ) )
    self.ftsGUID = self.ftsJob.FTSGUID
    return S_OK()

  def __resolveFTSServer( self ):
    """
    resolve FTS server to use, it should be the closest one from target SE

    :param self: self reference
    """
    from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFTSServersForSites
    if not self.targetSE:
      return S_ERROR( "Target SE not set" )
    res = getSitesForSE( self.targetSE )
    if not res['OK'] or not res['Value']:
      return S_ERROR( "Could not determine target site" )
    targetSites = res['Value']

    targetSite = ''
    for targetSite in targetSites:
      targetFTS = getFTSServersForSites( [targetSite] )
      if targetFTS['OK']:
        ftsTarget = targetFTS['Value'][targetSite]
        if ftsTarget:
          self.ftsServer = ftsTarget
          return S_OK( self.ftsServer )
      else:
        return targetFTS
    return S_ERROR( 'No FTS server found for %s' % targetSite )

  ####################################################################
  #
  #  Methods for monitoring
  #

  def summary( self, untilTerminal = False, printOutput = False ):
    """ summary of FTS job

    :param self: self reference
    :param bool untilTerminal: flag to monitor FTS job to its final state
    :param bool printOutput: flag to print out monitoring information to the stdout
    """
    res = self.__isSummaryValid()
    if not res['OK']:
      return res
    while not self.isTerminal:
      res = self.__parseOutput( full = True )
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

  def monitor( self, untilTerminal = False, printOutput = False, full = True ):
    """ monitor FTS job

    :param self: self reference
    :param bool untilTerminal: flag to monitor FTS job to its final state
    :param bool printOutput: flag to print out monitoring information to the stdout
    """
    if not self.ftsJob:
      self.resolveSource()
      self.__createFTSJob( self.ftsGUID )
    res = self.__isSummaryValid()
    if not res['OK']:
      return res
    if untilTerminal:
      res = self.summary( untilTerminal = untilTerminal, printOutput = printOutput )
      if not res['OK']:
        return res
    res = self.__parseOutput( full = full )
    if not res['OK']:
      return res
    if untilTerminal:
      self.finalize()
    if printOutput:
      self.dump()
    return res

  def dumpSummary( self, printOutput = False ):
    """ get FTS job summary as str

    :param self: self reference
    :param bool printOutput: print summary to stdout
    """

    outStr = ''
    for status in sorted( self.statusSummary ):
      if self.statusSummary[status]:
        outStr = '%s\t%-10s : %-10s\n' % ( outStr, status, str( self.statusSummary[status] ) )
    outStr = outStr.rstrip( '\n' )
    if printOutput:
      print outStr
    return S_OK( outStr )

  def __print( self ):
    """ print progress bar of FTS job completeness to stdout

    :param self: self reference
    """
    width = 100
    bits = int( ( width * self.percentageComplete ) / 100 )
    outStr = "|%s>%s| %.1f%s %s %s" % ( "="*bits, " "*( width - bits ),
                                        self.percentageComplete, "%",
                                        self.requestStatus, " "*10 )
    sys.stdout.write( "%s\r" % ( outStr ) )
    sys.stdout.flush()

  def dump( self ):
    """ print FTS job parameters and files to stdout

    :param self: self reference
    """
    print "%-10s : %-10s" % ( "Status", self.requestStatus )
    print "%-10s : %-10s" % ( "Source", self.sourceSE )
    print "%-10s : %-10s" % ( "Target", self.targetSE )
    print "%-10s : %-128s" % ( "Server", self.ftsServer )
    print "%-10s : %-128s" % ( "GUID", self.ftsGUID )
    for lfn in sorted( self.fileDict ):
      print "\n  %-15s : %-128s" % ( 'LFN', lfn )
      for key in ['Source', 'Target', 'Status', 'Reason', 'Duration']:
        print "  %-15s : %-128s" % ( key, str( self.fileDict[lfn].get( key ) ) )
    return S_OK()

  def __isSummaryValid( self ):
    """ check validity of FTS job summary report

    :param self: self reference
    """
    if not self.ftsServer:
      return S_ERROR( "FTSServer not set" )
    if not self.ftsGUID:
      return S_ERROR( "FTSGUID not set" )
    return S_OK()

  def __parseOutput( self, full = False ):
    """ execute glite-transfer-status command and parse its output

    :param self: self reference
    :param bool full: glite-transfer-status verbosity level, when set, collect information of files as well
    """
    monitor = self.ftsJob.monitorFTS2( command = self.monitorCommand, full = full )
    if not monitor['OK']:
      return monitor
    self.percentageComplete = self.ftsJob.Completeness
    self.requestStatus = self.ftsJob.Status
    self.submitTime = self.ftsJob.SubmitTime

    statusSummary = monitor['Value']
    if statusSummary:
      for state in statusSummary:
        self.statusSummary[state] = statusSummary[state]

    self.transferTime = 0
    for ftsFile in self.ftsJob:
      lfn = ftsFile.LFN
      self.__setFileParameter( lfn, 'Status', ftsFile.Status )
      self.__setFileParameter( lfn, 'Reason', ftsFile.Error )
      self.__setFileParameter( lfn, 'Duration', ftsFile._duration )
      targetURL = self.__getFileParameter( lfn, 'Target' )
      if not targetURL['OK']:
        self.__setFileParameter( lfn, 'Target', ftsFile.TargetSURL )
      self.transferTime += int( ftsFile._duration )
    return S_OK()

  ####################################################################
  #
  #  Methods for finalization
  #

  def finalize( self ):
    """ finalize FTS job

    :param self: self reference
    """
    self.__updateMetadataCache()
    transEndTime = dateTime()
    regStartTime = time.time()
    res = self.getTransferStatistics()
    transDict = res['Value']

    res = self.__registerSuccessful( transDict['transLFNs'] )

    regSuc, regTotal = res['Value']
    regTime = time.time() - regStartTime
    if self.sourceSE and self.targetSE:
      self.__sendAccounting( regSuc, regTotal, regTime, transEndTime, transDict )
    return S_OK()

  def getTransferStatistics( self ):
    """ collect information of Transfers that can be used by Accounting

    :param self: self reference
    """
    transDict = { 'transTotal': len( self.fileDict ),
                  'transLFNs': [],
                  'transOK': 0,
                  'transSize': 0 }

    for lfn in self.fileDict:
      if self.fileDict[lfn].get( 'Status' ) in self.successfulStates:
        if self.fileDict[lfn].get( 'Duration', 0 ):
          transDict['transLFNs'].append( lfn )
          transDict['transOK'] += 1
          if lfn in self.catalogMetadata:
            transDict['transSize'] += self.catalogMetadata[lfn].get( 'Size', 0 )

    return S_OK( transDict )

  def getFailedRegistrations( self ):
    """ get failed registrations dict

    :param self: self reference
    """
    return S_OK( self.failedRegistrations )

  def __registerSuccessful( self, transLFNs ):
    """ register successfully transferred files to the catalogs,
    fill failedRegistrations dict for files that failed to register

    :param self: self reference
    :param list transLFNs: LFNs in FTS job
    """
    self.failedRegistrations = {}
    toRegister = {}
    for lfn in transLFNs:
      res = returnSingleResult( self.oTargetSE.getURL( self.fileDict[lfn].get( 'Target' ), protocol = 'srm' ) )
      if not res['OK']:
        self.__setFileParameter( lfn, 'Reason', res['Message'] )
        self.__setFileParameter( lfn, 'Status', 'Failed' )
      else:
        toRegister[lfn] = { 'PFN' : res['Value'], 'SE' : self.targetSE }
    if not toRegister:
      return S_OK( ( 0, 0 ) )
    res = self.__getCatalogObject()
    if not res['OK']:
      for lfn in toRegister:
        self.failedRegistrations = toRegister
        self.log.error( 'Failed to get Catalog Object', res['Message'] )
        return S_OK( ( 0, len( toRegister ) ) )
    res = self.oCatalog.addReplica( toRegister )
    if not res['OK']:
      self.failedRegistrations = toRegister
      self.log.error( 'Failed to get Catalog Object', res['Message'] )
      return S_OK( ( 0, len( toRegister ) ) )
    for lfn, error in res['Value']['Failed'].items():
      self.failedRegistrations[lfn] = toRegister[lfn]
      self.log.error( 'Registration of Replica failed', '%s : %s' % ( lfn, str( error ) ) )
    return S_OK( ( len( res['Value']['Successful'] ), len( toRegister ) ) )

  def __sendAccounting( self, regSuc, regTotal, regTime, transEndTime, transDict ):
    """ send accounting record

    :param self: self reference
    :param regSuc: number of files successfully registered
    :param regTotal: number of files attepted to register
    :param regTime: time stamp at the end of registration
    :param transEndTime: time stamp at the end of FTS job
    :param dict transDict: dict holding couters for files being transerred, their sizes and successfull transfers
    """

    oAccounting = DataOperation()
    oAccounting.setEndTime( transEndTime )
    oAccounting.setStartTime( self.submitTime )

    accountingDict = {}
    accountingDict['OperationType'] = 'replicateAndRegister'
    result = getProxyInfo()
    if not result['OK']:
      userName = 'system'
    else:
      userName = result['Value'].get( 'username', 'unknown' )
    accountingDict['User'] = userName
    accountingDict['Protocol'] = 'FTS' if 'fts3' not in self.ftsServer else 'FTS3'
    accountingDict['RegistrationTime'] = regTime
    accountingDict['RegistrationOK'] = regSuc
    accountingDict['RegistrationTotal'] = regTotal
    accountingDict['TransferOK'] = transDict['transOK']
    accountingDict['TransferTotal'] = transDict['transTotal']
    accountingDict['TransferSize'] = transDict['transSize']
    accountingDict['FinalStatus'] = self.requestStatus
    accountingDict['Source'] = self.sourceSE
    accountingDict['Destination'] = self.targetSE
    accountingDict['TransferTime'] = self.transferTime
    oAccounting.setValuesFromDict( accountingDict )
    self.log.verbose( "Attempting to commit accounting message..." )
    oAccounting.commit()
    self.log.verbose( "...committed." )
    return S_OK()

