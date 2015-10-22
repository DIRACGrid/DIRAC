from DIRAC.FrameworkSystem.Client.Logger import gLogger

from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job

from DIRAC.DataManagementSystem.private import FTS3Utilities

from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB



class FTS3Agent(object):
  

  def initialize( self ):
    """ agent's initialization """
    self.fts3db = FTS3DB()
    pass

  def getFTS3Context( self, username, group, ftsServer ):
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

    log = gLogger.getSubLogger( "monitorActiveJobs", child = True )


    # get jobs from DB
    ret = self.fts3db.getAllActiveJobs()

    activeJobs = ret['Value']
    
    for ftsJob in activeJobs:
      context = self.getFTS3Context( ftsJob.username, ftsJob.userGroup, ftsJob.ftsServer )
      res = ftsJob.monitor( context = context )

      if not res['OK']:
        log.error( res )
        continue

      # { fileID : { Status, Error } }
      filesStatus = res['Value']

      res = self.fts3db.updateFileStatus( filesStatus )

      if not res['OK']:
        log.error( "Error updating file fts status", "%s, %s" % ( ftsJob.ftsGUID, res ) )
        continue


      res = self.fts3db.updateJobStatus( {ftsJob.ftsJobID :  {'status' : ftsJob.status,
                                                              'error' : ftsJob.error,
                                                              'completeness' : ftsJob.completeness,
                                                              }
                                          } )

      if not res['OK']:
        log.error( "Error updating job status", "%s, %s" % ( ftsJob.ftsGUID, res ) )


  def treatOperations( self ):
    """ * Fetch all the FTSOperations which are not finished
        * Do the call back of finished operation
        * Generate the new jobs and submit them
        * Fetch the operation for which we need to perform the callback
    """
    
    log = gLogger.getSubLogger( "treatOperations", child = True )

    res = self.fts3db.getOperationsWithFilesToSubmit()

    if not res['OK']:
      log.error( "Could not get incomplete operations", res )
      return res
    
    incompleteOperations = res['Value']

    log.info( "Treating %s incomplete operations" % len( incompleteOperations ) )

    for operation in incompleteOperations:
      
      if operation.isTotallyProcessed():
        res = operation.callback()
        
        if not res['OK']:
          log.error( res )
          continue
        

      else:
        res = operation.prepareNewJobs()

        if not res['OK']:
          log.error( "Cannot prepare new Jobs", res )

        newJobs = res['Value']

        for ftsJob in newJobs:
          res = self.getFTS3Server()
          if not res['OK']:
            log.error( res )
            continue
          ftsServer = res['Value']

          ftsJob.ftsServer = ftsServer

          context = self.getFTS3Context( ftsJob.username, ftsJob.userGroup, ftsServer )
          res = ftsJob.submit( context = context )
          
          if not res['OK']:
            log.error( res )
            continue

          operation.ftsJobs.append( ftsJob )

          submittedFileIds = res['Value']
          log.info( "Submitted job for %s transfers" % ( len( submittedFileIds ) ) )


      # new jobs are put in the DB at the same time
      self.fts3db.persistOperation( operation )




    res = self.fts3db.getProcessedOperations()

    if not res['OK']:
      log.error( "Could not get processed operations", res )
      return res

    processedOperations = res['Value']

    log.info( "Treating %s processed operations" % len( processedOperations ) )

    for operation in processedOperations:

      res = operation.callback()

      if not res['OK']:
        log.error( res )
        continue



      



        



