from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job

from DIRAC.DataManagementSystem.private import FTS3Utilities
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFTS3Servers

from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations as opHelper
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.Core.Utilities.DictCache import DictCache

from threading import current_thread
from multiprocessing.pool import ThreadPool




__RCSID__ = "$Id: $"
AGENT_NAME = "DataManagement/FTS3Agent"

class FTS3Agent( AgentModule ):
  

  def initialize( self ):
    """ agent's initialization """
    self.fts3db = FTS3DB()

    # Getting all the possible servers
    res = getFTS3Servers()
    if not res['OK']:
      gLogger.error( res['Message'] )
      return res

    srvList = res['Value']
    serverPolicyType = opHelper().getValue( 'DataManagement/FTSPlacement/FTS3/ServerPolicy' )
    
    
    self._serverPolicy = FTS3Utilities.FTS3ServerPolicy( srvList, serverPolicy = serverPolicyType )

    self._globalContextCache = {}

    self.maxNumberOfThreads = 10

    return S_OK()



  def getFTS3Context( self, username, group, ftsServer ):
    """ Returns an fts3 context for a given user, group and fts server

        The context pool is per thread, and there is one context
        per tuple (user, group, server).
        We dump the proxy of a user to a file (shared by all the threads),
        and use it to make the context.
        The proxy needs a lifetime of at least 2h, is cached for 1.5h, and
        the lifetime of the context is 45mn

        :param username: name of the user
        :param group: group of the user
        :param ftsServer: address of the server
        
        :returns: S_OK with the context object


    """

    threadID = current_thread().ident
    contextes = self._globalContextCache.setdefault( threadID, DictCache() )
    
    idTuple =(username, group, ftsServer) 
    if not contextes.exists( idTuple, 2700 ):
      res = getDNForUsername(username)
      if not res['OK']:
        return res
      # We take the first DN returned
      userDN = res['Value'][0]
      
      # We dump the proxy to a file.
      # It has to have a lifetime of at least 2 hours
      # and we cache it for 1.5 hours
      res = gProxyManager.downloadVOMSProxyToFile(userDN,
                                                  group,
                                                  requiredTimeLeft = 7200,
                                                  cacheTime = 5400 )
      if not res['OK']:
        return res
      
      proxyFile = res['Value']
      
      # We generate the context
      res = FTS3Job.generateContext( ftsServer, proxyFile )
      if not res['OK']:
        return res
      context = res['Value']

      # we add it to the cache for this thread for 1h
      contextes[idTuple].add( idTuple, 3600, context )

    return S_OK( contextes[idTuple] )


  
  
  def _monitorJob( self, ftsJob ):
    """
        * query the FTS servers
        * update the FTSFile status
        * update the FTSJob status
    """
    log = gLogger.getSubLogger( "_monitorJob/%s" % ftsJob.jobID, child = True )

    res = self.getFTS3Context( ftsJob.username, ftsJob.userGroup, ftsJob.ftsServer )

    if not res['OK']:
      log.error( "Error getting context", res )
      return ftsJob, res

    context = res['Value']


    res = ftsJob.monitor( context = context )

    if not res['OK']:
      log.error( "Error monitoring job", res )
      return ftsJob, res

    # { fileID : { Status, Error } }
    filesStatus = res['Value']

    res = self.fts3db.updateFileStatus( filesStatus )

    if not res['OK']:
      log.error( "Error updating file fts status", "%s, %s" % ( ftsJob.ftsGUID, res ) )
      return ftsJob, res


    res = self.fts3db.updateJobStatus( {ftsJob.ftsJobID :  {'status' : ftsJob.status,
                                                            'error' : ftsJob.error,
                                                            'completeness' : ftsJob.completeness,
                                                            'operationID' : ftsJob.operationID,
                                                            }
                                        }
                                     )

    return ftsJob, res
  
  
  @staticmethod
  def _monitorJobCallback( returnedValue ):
    """ Callback when a job has been monitored
        :param returnedValue: value returned by the _monitorJob method
                              (ftsJob, standard dirac return struct)
    """

    ftsJob, res = returnedValue
    log = gLogger.getSubLogger( "_monitorJobCallback/%s"%ftsJob.jobID, child = True )
    if not res['OK']:
      log.error( "Error updating job status", res )
    else:
      log.debug( "Successfully updated job status" )
      

  
  def monitorJobsLoop( self ):
    """
        * fetch the active FTSJobs from the DB
        * spawn a thread to monitor each of them
    """

    log = gLogger.getSubLogger( "monitorJobs", child = True )

    thPool = ThreadPool( self.maxNumberOfThreads )

    log.debug( "Getting active jobs" )
    # get jobs from DB
    res = self.fts3db.getActiveJobs()

    if not res['OK']:
      log.error( "Could not retrieve ftsJobs from the DB", res )
      return res

    activeJobs = res['Value']
    log.info( "%s jobs to queue for monitoring" % len( activeJobs ) )

    # Starting the monitoring threads
    for ftsJob in activeJobs:
      log.debug( "Queuing executing of ftsJob %s" % ftsJob.jobID )
      # queue the execution of self._monitorJob( ftsJob ) in the thread pool
      # The returned value is passed to _monitorJobCallback
      thPool.apply_async( self._monitorJob, ( ftsJob, ), callback = self._monitorJobCallback )

    log.debug( "All execution queued" )

    # Waiting for all the monitoring to finish
    thPool.close()
    thPool.join()
    log.debug( "thPool joined" )
    return S_OK()





  @staticmethod
  def _treatOperationCallback(returnedValue):
    """ Callback when an operation has been treated
        :param returnedValue: value returned by the _treatOperation method
                              (ftsOperation, standard dirac return struct)
    """

    operation, res = returnedValue
    log = gLogger.getSubLogger( "_treatOperationCallback/%s" % operation.operationID, child = True )
    if not res['OK']:
      log.error( "Error treating operation", res )
    else:
      log.debug( "Successfully treated operation" )


  def _treatOperation(self, operation):
    """ Treat one operation:
          * does the callback if the operation is finished
          * generate new jobs and submits them
    """

    log = gLogger.getSubLogger( "treatOperation/%s" % operation.operationID, child = True )


    # If the operation is totally processed
    # we perform the callback
    if operation.isTotallyProcessed():
      log.debug( "FTS3Operation %s is totally processed" % operation.operationID )
      res = operation.callback()

      if not res['OK']:
        log.error( "Error performing the callback", res )
        return operation, res


    else:
      log.debug( "FTS3Operation %s is not totally processed yet" % operation.operationID )

      res = operation.prepareNewJobs()

      if not res['OK']:
        log.error( "Cannot prepare new Jobs", "FTS3Operation %s : %s" % ( operation.operationID, res ) )
        return operation, res

      newJobs = res['Value']

      log.debug( "FTS3Operation %s: %s new jobs to be submitted" % ( operation.operationID, len( newJobs ) ) )

      for ftsJob in newJobs:
        res = self._serverPolicy.chooseFTS3Server()
        if not res['OK']:
          log.error( res )
          continue

        ftsServer = res['Value']
        log.debug( "Use %s server" % ftsServer )

        ftsJob.ftsServer = ftsServer

        context = self.getFTS3Context( ftsJob.username, ftsJob.userGroup, ftsServer )
        res = ftsJob.submit( context = context )

        if not res['OK']:
          log.error( "Could not submit FTS3Job", "FTS3Operation %s : %s" % ( operation.operationID, res ) )
          continue

        operation.ftsJobs.append( ftsJob )

        submittedFileIds = res['Value']
        log.info( "FTS3Operation %s: Submitted job for %s transfers" % ( operation.operationID, len( submittedFileIds ) ) )


      # new jobs are put in the DB at the same time
    res = self.fts3db.persistOperation( operation )

    if not res['OK']:
      log.error( "Could not persist operation", res )

    return operation, res

  def treatOperationsLoop( self ):
    """ * Fetch all the FTSOperations which are not finished
        * Spawn a thread to treat each operation
    """
    
    log = gLogger.getSubLogger( "treatOperations", child = True )

    thPool = ThreadPool( self.maxNumberOfThreads )

    log.info( "Getting non finished operations" )

    res = self.fts3db.getNonFinishedOperations()

    if not res['OK']:
      log.error( "Could not get incomplete operations", res )
      return res

    incompleteOperations = res['Value']
    
    log.info( "Treating %s incomplete operations" % len( incompleteOperations ) )

    for operation in incompleteOperations:
      log.debug( "Queuing executing of operation %s" % operation.operationID )
      # queue the execution of self._treatOperation( operation ) in the thread pool
      # The returned value is passed to _treatOperationCallback
      thPool.apply_async( self._treatOperation, ( operation, ), callback = self._treatOperationCallback )


    log.debug( "All execution queued" )

    # Waiting for all the treatments to finish
    thPool.close()
    thPool.join()
    log.debug( "thPool joined" )
    return S_OK()



  def finalize( self ):
    """ finalize processing """
    return S_OK()

  def execute( self ):
    """ one cycle execution """

    log = gLogger.getSubLogger( "execute", child = True )

    log.info( "Monitoring job" )
    res = self.monitorJobsLoop()

    if not res['OK']:
      log.error( "Error monitoring jobs", res )
      return res

    log.info( "Treating operations" )
    res = self.treatOperationsLoop()
    
    if not res['OK']:
      log.error( "Error treating operations", res )
      return res

    return S_OK()

      



        



