from DIRAC.FrameworkSystem.Client.Logger import gLogger

from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job

from DIRAC.DataManagementSystem.private import FTS3Utilities

from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.List import breakListIntoChunks

from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus



class FTS3Agent(object):
  

  def getFTS3Context( self, owner, group, ftsServer ):
    pass

  def getFTS3Server( self ):
    pass
  
  def monitorActiveJobs( self ):
    """
        * fetch the active FTSJobs from the DB
        * monitor each of them
        * update the FTSFile status
        * update the FTSJob status
    """



    # get jobs from DB
    activeJobs = db.getAllActiveJobs()
    
    for ftsJob in activeJobs:
      context = self.getFTS3Context( ftsJob.owner, ftsJob.ownerGroup, ftsJob.ftsServer )
      res = ftsJob.monitor( context = context )

      if not res['OK']:
        log.error( res )
        continue

      # { fileID : { Status, Error } }
      filesStatus = res['Value']

      db.updateFileStatus( filesStatus )

      db.updateJobStatus( {ftsJob.ftsJobID :  {'Status' : ftsJob.status, 'Error' : ftsJob.error}} )


  def treatOperations( self ):
    """ * Fetch all the FTSOperations which are not finished
        * Do the call back of finished operation
        * Generate the nwe jobs and submit them
    """
    
    allOperations = db.getAllNonFinishedOperation()
    
    for operation in allOperations:
      
      if operation.isTotallyExecuted():
        res = operation.callback()
        
        if not res['OK']:
          log.error( res )
          continue
        
      else:
        newJobs = operation.prepareNewJobs()

        for ftsJob in newJobs:
          res = self.getFTS3Server()
          if not res['OK']:
            log.error( res )
            continue
          ftsServer = res['Value']

          context = self.getFTS3Context( ftsJob.owner, ftsJob.ownerGroup, ftsServer )
          res = ftsJob.submit( context = context )
          
          if not res['OK']:
            log.error( res )
            continue

          operation.ftsJobs.append( ftsJob )

          submittedFileIds = res['Value']

          # If this fails, it's okay we will anyway update them at the next loop
          db.updateFileStatus( dict.fromKeys( submittedFileIds, {'Status' : 'Submitted'} ) )

      # new jobs are put in the DB at the same time
      db.putOperation( operation )





      



        



