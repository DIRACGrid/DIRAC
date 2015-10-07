from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.private import FTS3Utilities

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.FrameworkSystem.Client.Logger import gLogger

from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Utilities.List import breakListIntoChunks



class FTS3Operation(object):
  
  def __init__(self):
    
    # persistant
    self.owner = None
    self.ownerGroup = None

    self.rmsReqID = 0
    self.rmsOpID = 0
    self.sourceSEs = []

    self.ftsFiles = []
    
    self.activity = None
    self.priority = None

    self.ftsJobs = []

    ########################


    self.dManager = DataManager()

    
    self._log = gLogger.getSubLogger( "req_%s" % self.rmsReqID , True )

    self.MAX_FILES_PER_JOB = 100
    

  def isTotallyExecuted( self ):
    """ Returns True if and only if there is nothing
        else to be done by FTS for this operation.
        All files are successful or definitely failed
    """
    return False

    
  def __getFilesToSubmit(self):
    toSubmit = [ftsFile for ftsFile in self.ftsFiles]

    return S_OK( toSubmit )

      


  def __checkSEAccess( self, seName, accessType ):
    # Check that the target is writable
    access = self.rssClient.getStorageElementStatus( seName, accessType )
    if not access["OK"]:
      return access

    if access["Value"][seName][accessType] not in ( "Active", "Degraded" ):
      return S_ERROR( "%s does not have %s in Active or Degraded" % ( seName, accessType ) )

    return S_OK()
        


  def prepareNewJobs( self ):

    log = gLogger.getSubLogger( "_prepareNewJobs", True )

    filesToSubmit = self.__getFilesToSubmit()
    log.debug( "%s ftsFiles to submit" % len( filesToSubmit ) )


    newJobs = []


    # {targetSE : [FTS3Files] }
    filesGroupedByTarget = FTS3Utilities.groupFilesByTarget( filesToSubmit )

    for targetSE, ftsFiles in filesGroupedByTarget.iteritems():
      
      res = self.__checkSEAccess( targetSE, 'WriteAccess' )
      
      if not res['OK']:
        log.error( res )
        continue
      

      # { sourceSE : [FTSFiles] }
      possibleTransfersBySource = FTS3Utilities.generatePossibleTransfersBySources( ftsFiles, allowedSources = self.sourceSEs )

      # Pick a unique source for each transfer
      uniqueTransfersBySource = FTS3Utilities.selectUniqueSourceforTransfers( possibleTransfersBySource )

      # We don't need to check the source, since it is already filtered by the DataManager
      for sourceSE, ftsFiles in uniqueTransfersBySource.iteritems():


        for ftsFilesChunk in breakListIntoChunks( ftsFiles, self.MAX_FILES_PER_JOB ):

          newJob = FTS3Job()
          newJob.jobType = 'Transfer'
          newJob.sourceSE = sourceSE
          newJob.targetSE = targetSE
          newJob.activity = self.activity
          newJob.priority = self.priority
          newJob.owner = self.owner
          newJob.ownerGroup = self.ownerGroup
          newJob.filesToSubmit = ftsFilesChunk
          newJob.ftsOperationID = getattr( self, 'ftsOperationID' )


          newJobs.append( newJob )


    return S_OK( newJobs )
















       



