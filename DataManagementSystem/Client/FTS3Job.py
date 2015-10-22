import datetime

# Requires at least version 3.3.3
import fts3.rest.client.easy as fts3
from fts3.rest.client.exceptions import FTS3ClientException

from DIRAC.Resources.Storage.StorageElement import StorageElement

from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File

from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.DErrno import DError
from DIRAC.DataManagementSystem.private.FTS3Utilities import FTS3Serializable


class FTS3Job( FTS3Serializable ):
  
  # States from FTS doc
  ALL_STATES = ['Submitted',  # Initial state of a job as soon it's dropped into the database
                'Ready', # One of the files within a job went to Ready state
                'Active', # One of the files within a job went to Active state
                'Finished', # All files Finished gracefully
                'Canceled', # Job canceled
                'Failed', # All files Failed
                'Finisheddirty', # All files Failed
               ]
  
  FINAL_STATES = ['Canceled', 'Failed', 'Finished', 'Finisheddirty']
  INIT_STATE = 'Submitted'
  
  _attrToSerialize = ['jobID', 'operationID', 'status', 'error', 'submitTime',
                      'lastUpdate', 'ftsServer', 'ftsGUID', 'completeness',
                      'username', 'userGroup']

  def __init__(self):
    

    self.submitTime = None
    self.lastUpdate = None
    
    self.ftsGUID = None
    self.ftsServer = None
    
    self.error = None
    self.status = FTS3Job.INIT_STATE


    self.completeness = None

    self.operationID = None

    self.username = None
    self.userGroup = None


    # temporary used only for submission
    # Set by FTS Operation when preparing
    self.type = None  # Transfer, Staging, Removal

    self.sourceSE = None
    self.targetSE = None
    self.filesToSubmit = []
    self.activity = None
    self.priority = None

  
    
    
  def monitor( self, context = None, ftsServer = None, ucert = None ):
    """ Queries the fts server to monitor the job

        This method assumes that the attribute self.ftsGUID is set

        :param context: fts3 context. If not given, it is created (see ftsServer & ucert param)
        :param ftsServer: the address of the fts server to submit to. Used only if context is
                          not given. if not given either, use the ftsServer object attribute

        :param ucert: path to the user certificate/proxy. Might be infered by the fts cli (see its doc)

        :returns {FileID: { status, error } }
    """
#     log = gLogger.getSubLogger( "monitor/%s/%s" % ( self.ftsOperationID, self.ftsGUID ) , True )

    if not self.ftsGUID:
      return S_ERROR( "FTSGUID not set, FTS job not submitted?" )

    if not context:
      if not ftsServer:
        ftsServer = self.ftsServer
      context = fts3.Context( endpoint = ftsServer, ucert = ucert )


    jobStatusDict = None
    try:
      jobStatusDict = fts3.get_job_status( context, self.ftsGUID, list_files = True )
    except FTS3ClientException, e:
      return S_ERROR( "Error getting the job status %s" % e )

    newStatus = jobStatusDict['job_state'].capitalize()
    if newStatus != self.status:
      self.status = newStatus
      self.lastUpdate = datetime.datetime.utcnow().replace( microsecond = 0 )
      self.error = jobStatusDict['reason']

    filesInfoList = jobStatusDict['files']

    filesStatus = {}

    statusSummary = {}
    for fileDict in filesInfoList:
      file_state = fileDict['file_state'].capitalize()
      file_id = fileDict['file_metadata']
      file_error = fileDict['reason']
      filesStatus[file_id] = {'status' : file_state, 'error' : file_error}


      statusSummary[file_state] = statusSummary.get( file_state, 0 ) + 1

    total = len( filesInfoList )
    completed = sum( [ statusSummary.get( state, 0 ) for state in FTS3File.FTS_FINAL_STATES ] )
    self.completeness = 100 * completed / total

    return S_OK( filesStatus )
  
  @staticmethod
  def __fetchSpaceToken( seName ):
    """ Fetch the space token of storage element
        :param seName name of the storageElement
        :returns space token
    """
    seToken = None
    if seName:
      seObj = StorageElement( seName )


      res = seObj.getStorageParameters( "SRM2" )
      if not res['OK']:
        return res

      seToken = res["Value"].get( "SpaceToken" )

    return S_OK( seToken )


  @staticmethod
  def __isTapeSE( seName ):
    """ Check whether a given SE is a tape storage
        :param seName name of the storageElement

        :returns True/False
                 In case of error, returns True.
                 It is better to loose a bit of time on the FTS
                 side, rather than failing jobs because the FTS default
                 pin time is too short
    """
    isTape = StorageElement( seName ).getStatus()\
                                     .get( 'Value', {} )\
                                     .get( 'TapeSE', True )

    return isTape


  
  def submit( self, pinTime = 36000, context = None, ftsServer = None, ucert = None ):
    """ submit the job to the FTS server

        Some attributes are expected to be defined for the submission to work:
          * sourceSE (only for Transfer jobs)
          * targetSE
          * activity (optional)
          * priority (optional)
          * username
          * userGroup
          * filesToSubmit
          * operationID (optional, used as metadata for the job)

        We also expect the FTSFiles have an ID defined, as it is given as transfer metadata

        :param pinTime: Time the file should be pinned on disk (used for transfers and staging)
                        Used only if he source SE is a tape storage
        :param context: fts3 context. If not given, it is created (see ftsServer & ucert param)
        :param ftsServer: the address of the fts server to submit to. Used only if context is 
                          not given. if not given either, use the ftsServer object attribute

        :param ucert: path to the user certificate/proxy. Might be infered by the fts cli (see its doc)

        :returns S_OK([FTSFiles ids of files submitted])
    """






    log = gLogger.getSubLogger( "submit/%s/%s_%s" % ( self.operationID, self.sourceSE, self.targetSE ) , True )

    if not context:
      if not ftsServer:
        ftsServer = self.ftsServer
      context = fts3.Context( endpoint = ftsServer, ucert = ucert )



    res = self.__fetchSpaceToken( self.sourceSE )
    if not res['OK']:
      return res
    source_spacetoken = res['Value']

    res = self.__fetchSpaceToken( self.targetSE )
    if not res['OK']:
      return res
    dest_spacetoken = res['Value']


    allLFNs = [ftsFile.lfn for ftsFile in self.filesToSubmit]

    failedLFNs = set()

    # getting all the source surls
    res = StorageElement( self.sourceSE ).getURL( allLFNs, protocol = 'srm' )
    if not res['OK']:
      return res
    
    for lfn, reason in res['Value']['Failed']:
      failedLFNs.add( lfn )
      log.error( "Could not get source SURL", "%s %s" % ( lfn, reason ) )

    allSourceSURLs = res['Value']['Successful']

    # getting all the target surls
    res = StorageElement( self.targetSE ).getURL( allLFNs, protocol = 'srm' )
    if not res['OK']:
      return res

    for lfn, reason in res['Value']['Failed']:
      failedLFNs.add( lfn )
      log.error( "Could not get target SURL", "%s %s" % ( lfn, reason ) )

    allTargetSURLs = res['Value']['Successful']

    transfers = []

    fileIdsSubmitted = []

    for ftsFile in self.filesToSubmit:

      if ftsFile.lfn in failedLFNs:
        log.debug( "Not preparing transfer for file %s" % lfn )
        continue


      sourceSURL = allSourceSURLs[ftsFile.lfn]
      targetSURL = allTargetSURLs[ftsFile.lfn]
      trans = fts3.new_transfer( sourceSURL,
                                targetSURL,
                                checksum = ftsFile.checksum,
                                filesize = ftsFile.size,
                                metadata = getattr( ftsFile, 'fileID' ),
                                activity = self.activity )

      transfers.append( trans )
      fileIdsSubmitted.append( getattr( ftsFile, 'fileID' ) )


    # If the source is not an tape SE, we should set the
    # copy_pin_lifetime and bring_online params to None,
    # otherwise they will do an extra useless queue in FTS
    sourceIsTape = self.__isTapeSE( self.sourceSE )
    copy_pin_lifetime = pinTime if sourceIsTape else None
    bring_online = 86400 if sourceIsTape else None

    job = fts3.new_job( transfers = transfers,
                        overwrite = True,
                        source_spacetoken = source_spacetoken,
                        spacetoken = dest_spacetoken,
                        bring_online = bring_online,
                        copy_pin_lifetime = copy_pin_lifetime,
                        retry = 3,
                        metadata = self.operationID,
                        priority = self.priority )


    try:

      self.ftsGUID = fts3.submit( context, job )
      log.info( "Got GUID %s" % self.ftsGUID )

      # Only increase the amount of attempt
      # if we succeeded in submitting
      for ftsFile in self.filesToSubmit:
        ftsFile.attempt += 1
        ftsFile.status = 'Submitted'
      
      now = datetime.datetime.utcnow().replace( microsecond = 0 )
      self.submitTime = now
      self.lastUpdate = now

    except FTS3ClientException as e:
      return S_ERROR( "Error at submission: %s" % e )


    return S_OK( fileIdsSubmitted )


  




