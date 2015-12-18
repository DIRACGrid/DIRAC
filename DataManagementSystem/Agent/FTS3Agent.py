from DIRAC.FrameworkSystem.Client.Logger import gLogger

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



class FTS3Agent(object):
  

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

    maxNumberOfThreads = 10
    self._threadPool = ThreadPool( maxNumberOfThreads )



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
    log = gLogger.getSubLogger( "monitorJobs", child = True )

    context = self.getFTS3Context( ftsJob.username, ftsJob.userGroup, ftsJob.ftsServer )
    res = ftsJob.monitor( context = context )

    if not res['OK']:
      return res

    # { fileID : { Status, Error } }
    filesStatus = res['Value']

    res = self.fts3db.updateFileStatus( filesStatus )

    if not res['OK']:
      log.error( "Error updating file fts status", "%s, %s" % ( ftsJob.ftsGUID, res ) )
      return res


    res = self.fts3db.updateJobStatus( {ftsJob.ftsJobID :  {'status' : ftsJob.status,
                                                            'error' : ftsJob.error,
                                                            'completeness' : ftsJob.completeness,
                                                            'operationID' : ftsJob.operationID,
                                                            }
                                        } )   
  
  def monitorJobs( self ):
    """
        * fetch the active FTSJobs from the DB
        * monitor each of them
    """

    log = gLogger.getSubLogger( "monitorJobs", child = True )


    # get jobs from DB
    ret = self.fts3db.getActiveJobs()

    activeJobs = ret['Value']
    
    for ftsJob in activeJobs:

      res = self._monitorJob( ftsJob )

      if not res['OK']:
        log.error( "Error updating job status", "%s, %s" % ( ftsJob.ftsGUID, res ) )




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
        return res


    else:
      log.debug( "FTS3Operation %s is not totally processed yet" % operation.operationID )

      res = operation.prepareNewJobs()

      if not res['OK']:
        log.error( "Cannot prepare new Jobs", "FTS3Operation %s : %s" % ( operation.operationID, res ) )
        return res

      newJobs = res['Value']

      log.debug( "FTS3Operation %s: %s new jobs to be submitted" % ( operation.operationID, len( newJobs ) ) )

      for ftsJob in newJobs:
        res = self._serverPolicy.chooseFTS3Server()
        if not res['OK']:
          log.error( res )
          continue
        ftsServer = res['Value']

        ftsJob.ftsServer = ftsServer

        context = self.getFTS3Context( ftsJob.username, ftsJob.userGroup, ftsServer )
        res = ftsJob.submit( context = context )

        if not res['OK']:
          log.error( "Could not submit FTS3Job", "FTS3Operation %s : %s" % ( operation.operationID, res ) )
          continue

        operation.ftsJobs.append( ftsJob )

        submittedFileIds = res['Value']
        log.info( "FTS3Operation %s: Submitted job for %s transfers" % ( operation.operationID, len( submittedFileIds ) ) )



  def treatOperations( self ):
    """ * Fetch all the FTSOperations which are not finished
        * Do the call back of finished operation
        * Generate the new jobs and submit them
    """
    
    log = gLogger.getSubLogger( "treatOperations", child = True )

    res = self.fts3db.getNonFinishedOperations()

    if not res['OK']:
      log.error( "Could not get incomplete operations", res )
      return res
    
    incompleteOperations = res['Value']

    log.info( "Treating %s incomplete operations" % len( incompleteOperations ) )

    for operation in incompleteOperations:
      
      res = self._treatOperation( operation )

      if not res['OK']:
        log.error( "Error treating Operation", "OperationID %s: %s" % ( operation.operationID, res ) )
        continue
    
      # new jobs are put in the DB at the same time
      self.fts3db.persistOperation( operation )







      



        



