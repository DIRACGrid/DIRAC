import datetime

# Requires at least version 3.3.3
import fts3.rest.client.easy as fts3
from fts3.rest.client.exceptions import FTS3ClientException

from DIRAC.Resources.Storage.StorageElement import StorageElement

from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File

from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno, DError


class FTS3Job(object):
  
  FINAL_STATES = ['Canceled', 'Failed', 'Finished', 'Finisheddirty']
  
  def __init__(self):
    
    now = datetime.datetime.utcnow().replace( microsecond = 0 )

    self.creationTime = now
    self.submitTime = None
    self.lastUpdate = None
    
    self.ftsGUID = None
    self.ftsServer = None
    
    self.error = None
    self.owner = None
    self.ownerGroup = None
    self.status = None

    self.completeness = None

    self.ftsOperationID = None


    # temporary used only for submission
    # Set by FTS Operation when preparing
    self.jobType = None  # Transfer, Stage, Remove

    self.sourceSE = None
    self.targetSE = None
    self.filesToSubmit = []
    self.activity = None
    self.priority = None

  
    
    
  def monitor( self, context = None, ftsServer = None ):
    """ queries the fts server to get the state list

    """
    log = gLogger.getSubLogger( "monitor/%s/%s" % ( self.ftsOperationID, self.ftsGUID ) , True )

    if not self.ftsGUID:
      return S_ERROR( "FTSGUID not set, FTS job not submitted?" )

    if not context:
      if not ftsServer:
        ftsServer = self.ftsServer
      context = fts3.Context( endpoint = ftsServer )



    jobStatusDict = None
    try:
      jobStatusDict = fts3.get_job_status( context, self.ftsGUID, list_files = True )
    except FTS3ClientException, e:
      return S_ERROR( "Error getting the job status %s" % e )

    self.status = jobStatusDict['job_state'].capitalize()

    filesInfoList = jobStatusDict['files']

    filesStatus = {}

    statusSummary = {}
    for fileDict in filesInfoList:
      file_state = fileDict['file_state'].capitalize()
      file_id = fileDict['file_metadata']
      file_error = fileDict['reason']
      filesStatus[file_id] = {'Status' : file_state, 'Error' : file_error}


      statusSummary[file_state] = statusSummary.get( file_state, 0 ) + 1

    total = len( filesInfoList )
    completed = sum( [ statusSummary.get( state, 0 ) for state in FTS3File.FINAL_STATES ] )
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

  
  def submit( self, pinTime = 36000, context = None, ftsServer = None ):
    """ submit the job to the FTS server

        Some attributes are expected to be defined for the submission to work:
          * sourceSE (only for Transfer jobs)
          * targetSE
          * activity (optional)
          * priority (optional)
          * owner
          * ownerGroup
          * filesToSubmit
          * ftsOperationID (optional, used as metadata for the job)

        We also expect the FTSFiles have an ID defined, as it is given as transfer metadata

        :param pinTime: Time the file should be pinned on disk (used for transfers and staging)
        :param context: fts3 context. If not given, it is created (see ftsServer param)
        :param ftsServer: the address of the fts server to submit to. Used only if context is 
                          not given. if not given either, use the ftsServer object attribute

        :returns S_OK([FTSFiles ids of files submitted])
    """






    log = gLogger.getSubLogger( "submit/%s/%s_%s" % ( self.ftsOperationID, self.sourceSE, self.targetSE ) , True )

    if not context:
      if not ftsServer:
        ftsServer = self.ftsServer
      context = fts3.Context( endpoint = ftsServer )



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
                                metadata = getattr( ftsFile, 'ftsFileID' ),
                                activity = self.activity )

      transfers.append( trans )
      fileIdsSubmitted.append( getattr( ftsFile, 'ftsFileID' ) )


    copy_pin_lifetime = pinTime
    bring_online = 86400 if pinTime else None

    job = fts3.new_job( transfers = transfers,
                        overwrite = True,
                        source_spacetoken = source_spacetoken,
                        spacetoken = dest_spacetoken,
                        bring_online = bring_online,
                        copy_pin_lifetime = copy_pin_lifetime,
                        retry = 3,
                        metadata = self.ftsOperationID,
                        priority = self.priority )

    try:

      self.ftsGUID = fts3.submit( context, job )

      # Only increase the amount of attempt
      # if we succeeded in submitting
      for ftsFile in self.filesToSubmit:
        ftsFile.attempt += 1

    except FTS3ClientException as e:
      return S_ERROR( "Error at submission: %s" % e )


    return S_OK( fileIdsSubmitted )


  




