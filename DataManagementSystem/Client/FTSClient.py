########################################################################
# $HeadURL $
# File: FTSClient.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 14:29:43
########################################################################

""" :mod: FTSClient
    ===============

    .. module: FTSClient
    :synopsis: FTS client
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    FTS client
"""

__RCSID__ = "$Id $"

# #
# @file FTSClient.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 14:29:47
# @brief Definition of FTSClient class.

# # imports
from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.Client import Client
# # from RMS
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
# # from DMS
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob
from DIRAC.DataManagementSystem.Client.FTSFile import FTSFile
from DIRAC.DataManagementSystem.Client.FTSSite import FTSSite
from DIRAC.DataManagementSystem.private.FTSHistoryView import FTSHistoryView
from DIRAC.DataManagementSystem.private.FTSValidator import FTSValidator

########################################################################
class FTSClient( Client ):
  """
  .. class:: FTSClient

  DISET client for FTS
  """
  # # placeholder for FTSValidator
  __ftsValidator = None
  # # placeholder for FTSManager
  __ftsManager = None
  # # placeholder for request manager
  __requestClient = None

  def __init__( self, useCertificates = False ):
    """c'tor

    :param self: self reference
    :param bool useCertificates: flag to enable/disable certificates
    """
    Client.__init__( self )
    self.log = gLogger.getSubLogger( "DataManagement/FTSClient" )
    self.setServer( "DataManagement/FTSManager" )

  @classmethod
  def ftsValidator( cls ):
    """ get FTSValidator instance """
    if not cls.__ftsValidator:
      cls.__ftsValidator = FTSValidator()
    return cls.__ftsValidator

  @classmethod
  def ftsManager( cls, timeout = 300 ):
    """ get FTSManager instance """
    if not cls.__ftsManager:
      url = PathFinder.getServiceURL( "DataManagement/FTSManager" )
      if not url:
        raise RuntimeError( "CS option DataManagement/FTSManager URL is not set!" )
      cls.__ftsManager = RPCClient( url, timeout = timeout )
    return cls.__ftsManager

  @classmethod
  def requestClient( cls ):
    """ request client getter """
    if not cls.__requestClient:
      cls.__requestClient = RequestClient()
    return cls.__requestClient

  def getFTSFileList( self, statusList = None, limit = None ):
    """ get list of FTSFiles with status in statusList """
    statusList = statusList if statusList else [ "Waiting" ]
    limit = limit if limit else 1000
    getFTSFileList = self.ftsManager().getFTSFileList( statusList, limit )
    if not getFTSFileList["OK"]:
      self.log.error( "getFTSFileList: %s" % getFTSFileList["Message"] )
      return getFTSFileList
    getFTSFileList = getFTSFileList["Value"]
    return S_OK( [ FTSFile( ftsFile ) for ftsFile in getFTSFileList ] )

  def getFTSJobList( self, statusList = None, limit = None ):
    """ get FTSJobs wit statues in :statusList: """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    limit = limit if limit else 500
    getFTSJobList = self.ftsManager().getFTSJobList( statusList, limit )
    if not getFTSJobList["OK"]:
      self.log.error( "getFTSJobList: %s" % getFTSJobList["Message"] )
      return getFTSJobList
    getFTSJobList = getFTSJobList["Value"]
    return S_OK( [ FTSJob( ftsJobDict ) for ftsJobDict in getFTSJobList ] )

  def getFTSFilesForRequest( self, requestID, operationID = None ):
    """ read FTSFiles for a given :requestID:

    :param int requestID: ReqDB.Request.RequestID
    :param int operationID: ReqDB.Operation.OperationID
    """
    ftsFiles = self.ftsManager().getFTSFilesForRequest( requestID, operationID )
    if not ftsFiles["OK"]:
      self.log.error( "getFTSFilesForRequest: %s" % ftsFiles["Message"] )
      return ftsFiles
    return S_OK( [ FTSFile( ftsFileDict ) for ftsFileDict in ftsFiles["Value"] ] )

  def getFTSJobsForRequest( self, requestID, statusList = None ):
    """ get list of FTSJobs with statues in :statusList: given requestID

    :param int requestID: ReqDB.Request.RequestID
    :param list statusList: list with FTSJob statuses

    :return: [ FTSJob, FTSJob, ... ]
    """
    statusList = statusList if statusList else list( FTSJob.INITSTATES + FTSJob.TRANSSTATES )
    getJobs = self.ftsManager().getFTSJobsForRequest( requestID, statusList )
    if not getJobs["OK"]:
      self.log.error( "getFTSJobsForRequest: %s" % getJobs["Message"] )
      return getJobs
    return S_OK( [ FTSJob( ftsJobDict ) for ftsJobDict in getJobs["Value"] ] )

  def putFTSFile( self, ftsFile ):
    """ put FTSFile into FTSDB

    :param FTSFile ftsFile: FTSFile instance
    """
    isValid = self.ftsValidator().validate( ftsFile )
    if not isValid["OK"]:
      self.log.error( isValid["Message"] )
      return isValid
    ftsFileJSON = ftsFile.toJSON()
    if not ftsFileJSON["OK"]:
      self.log.error( ftsFileJSON["Message"] )
      return ftsFileJSON
    ftsFileJSON = ftsFileJSON["Value"]
    return self.ftsManager().putFTSFile( ftsFileJSON )

  def getFTSFile( self, ftsFileID = None ):
    """ get FTSFile

    :param int fileID: FileID
    :param int ftsFileID: FTSFileID
    """
    getFile = self.ftsManager().getFTSFile( ftsFileID )
    if not getFile["OK"]:
      self.log.error( getFile["Message"] )
    # # de-serialize
    if getFile["Value"]:
      getFile = FTSFile( getFile["Value"] )
      if not getFile["OK"]:
        self.log.error( getFile["Message"] )
    return getFile

  def deleteFTSFile( self, ftsFileID = None ):
    """ get FTSFile

    :param int ftsFileID: FTSFileID
    """
    deleteFile = self.ftsManager().deleteFTSFile( ftsFileID )
    if not deleteFile["OK"]:
      self.log.error( deleteFile["Message"] )
      return deleteFile
    return S_OK()

  def putFTSJob( self, ftsJob ):
    """ put FTSJob into FTSDB

    :param FTSJob ftsJob: FTSJob instance
    """
    isValid = self.ftsValidator().validate( ftsJob )
    if not isValid["OK"]:
      self.log.error( isValid["Message"] )
      return isValid
    ftsJobJSON = ftsJob.toJSON()
    if not ftsJobJSON["OK"]:
      self.log.error( ftsJobJSON["Message"] )
      return ftsJobJSON
    return self.ftsManager().putFTSJob( ftsJobJSON["Value"] )

  def getFTSJob( self, ftsJobID ):
    """ get FTS job

    :param int ftsJobID: FTSJobID
    """
    getJob = self.ftsManager().getFTSJob( ftsJobID )
    if not getJob["OK"]:
      self.log.error( getJob["Message"] )
      return getJob
    # # de-serialize
    if getJob["Value"]:
      getJob = FTSJob( getJob["Value"] )
    return getJob

  def deleteFTSJob( self, ftsJobID ):
    """ delete FTSJob into FTSDB

    :param int ftsJob: FTSJobID
    """
    deleteJob = self.ftsManager().deleteFTSJob( ftsJobID )
    if not deleteJob["OK"]:
      self.log.error( deleteJob["Message"] )
    return deleteJob

  def getFTSJobIDs( self, statusList = None ):
    """ get list of FTSJobIDs for a given status list """
    statusList = statusList if statusList else [ "Submitted", "Ready", "Active" ]
    ftsJobIDs = self.ftsManager().getFTSJobIDs( statusList )
    if not ftsJobIDs["OK"]:
      self.log.error( ftsJobIDs["Message"] )
    return ftsJobIDs

  def getFTSFileIDs( self, statusList = None ):
    """ get list of FTSFileIDs for a given status list """
    statusList = statusList if statusList else [ "Waiting" ]
    ftsFileIDs = self.ftsManager().getFTSFileIDs( statusList )
    if not ftsFileIDs["OK"]:
      self.log.error( ftsFileIDs["Message"] )
    return ftsFileIDs

  def getFTSHistory( self ):
    """ get FTS history snapshot """
    getFTSHistory = self.ftsManager().getFTSHistory()
    if not getFTSHistory["OK"]:
      self.log.error( getFTSHistory["Message"] )
      return getFTSHistory
    getFTSHistory = getFTSHistory["Value"]
    return S_OK( [ FTSHistoryView( ftsHistory ) for ftsHistory in getFTSHistory ] )

  def getDBSummary( self ):
    """ get FTDB summary """
    dbSummary = self.ftsManager().getDBSummary()
    if not dbSummary["OK"]:
      self.log.error( "getDBSummary: %s" % dbSummary["Message"] )
    return dbSummary

  def setFTSFilesWaiting( self, operationID, sourceSE, opFileIDList ):
    """ update status for waiting FTSFiles from 'Waiting#SourceSE' to 'Waiting'

    :param int operationID: ReqDB.Operation.OperationID
    :param str sourceSE: source SE name
    :param opFileIDList: [ ReqDB.File.FileID, ... ]
    """
    return self.ftsManager().setFTSFilesWaiting( operationID, sourceSE, opFileIDList )

  def deleteFTSFiles( self, operationID, opFileIDList ):
    """ delete FTSFiles for rescheduling

    :param int operationID: ReqDB.Operation.OperationID
    :param list opFileIDList: [ ReqDB.File.FileID, ... ]
    """
    return self.ftsManager().deleteFTSFiles( operationID, opFileIDList )

  def ftsSchedule( self, requestID, operationID, opFileList ):
    """ schedule lfn for FTS job

    :param int requestID: RequestDB.Request.RequestID
    :param int operationID: RequestDB.Operation.OperationID
    :param list opFileList: list of tuples ( File.toJSON()["Value"], sourcesList, targetList )
    """
    return self.ftsManager().ftsSchedule( requestID, operationID, opFileList )

