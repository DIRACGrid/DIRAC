################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/StElInspectorAgent'

import Queue, time

from DIRAC                                                  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.Utilities.ThreadPool                        import ThreadPool

from DIRAC.ResourceStatusSystem                             import CheckingFreqs
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command                     import knownAPIs
from DIRAC.ResourceStatusSystem.PolicySystem.PEP            import PEP
from DIRAC.ResourceStatusSystem.Utilities.Utils             import where

class StElInspectorAgent( AgentModule ):
  """
    The StElInspector agent ( StorageElementInspectorAgent ) is one of the four
    InspectorAgents of the RSS.

    This Agent takes care of the StorageElements. In order to do so, it gathers
    the eligible ones and then evaluates their statuses with the PEP.

    If you want to know more about the StElInspectorAgent, scroll down to the
    end of the file.
  """

  def initialize( self ):

    try:
      self.rsClient                    = ResourceStatusClient()
      self.StorageElementsFreqs        = CheckingFreqs[ 'StorageElementsFreqs' ]
      self.StorageElementsToBeChecked  = Queue.Queue()
      self.StorageElementsNamesInCheck = []

      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
      self.threadPool         = ThreadPool( self.maxNumberOfThreads,
                                            self.maxNumberOfThreads )
      if not self.threadPool:
        self.log.error( 'Can not create Thread Pool' )
        return S_ERROR( 'Can not create Thread Pool' )

      for _i in xrange( self.maxNumberOfThreads ):
        self.threadPool.generateJobAndQueueIt( self._executeCheck, args = ( None, ) )

      return S_OK()

    except Exception:
      errorStr = "StElInspectorAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
################################################################################

  def execute( self ):

    try:

#      kwargs = { 'meta' : { 'columns' : [ 'StorageElementName', 'StatusType', 'Status', \
#                              'FormerStatus', 'SiteType', 'TokenOwner' ] } }

      kwargs = { 'meta' : {} }
      kwargs['meta']['columns'] = [ 'StorageElementName', 'StatusType',
                                    'Status', 'FormerStatus', 'SiteType', \
                                    'TokenOwner' ]
      kwargs[ 'tokenOwner' ]    = 'RS_SVC'

      resQuery = self.rsClient.getStuffToCheck( 'StorageElement', self.StorageElementsFreqs, **kwargs )

      gLogger.info( 'Found %d candidates to be checked.' % len( resQuery[ 'Value' ] ) )

      for seTuple in resQuery[ 'Value' ]:

        #THIS IS IMPORTANT !!
        #Ignore all elements with token != RS_SVC
#        if seTuple[ 5 ] != 'RS_SVC':
#          continue

        if ( seTuple[ 0 ], seTuple[ 1 ] ) in self.StorageElementsNamesInCheck:
          gLogger.info( '%s(%s) discarded, already on the queue' % ( seTuple[ 0 ], seTuple[ 1 ] ) )
          continue

        resourceL = [ 'StorageElement' ] + seTuple

        # the tuple consists on ( SEName, SEStatusType )
        self.StorageElementsNamesInCheck.insert( 0, ( resourceL[ 1 ], resourceL[ 2 ] ) )
        self.StorageElementsToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      gLogger.exception( errorStr, lException = x )
      return S_ERROR( errorStr )

################################################################################
################################################################################

  def finalize( self ):
    if self.StorageElementsNamesInCheck:
      _msg = "Wait for queue to get empty before terminating the agent (%d tasks)"
      _msg = _msg % len( self.StorageElementsNamesInCheck )
      gLogger.info( _msg )
      while self.StorageElementsNamesInCheck:
        time.sleep( 2 )
      gLogger.info( "Queue is empty, terminating the agent..." )
    return S_OK()

################################################################################
################################################################################

  def _executeCheck( self, _arg ):

    # Init the APIs beforehand, and reuse them.
    __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient', 'SLSClient' ]
    clients = knownAPIs.initAPIs( __APIs__, {} )

    pep = PEP( clients = clients )

    while True:

      toBeChecked = self.StorageElementsToBeChecked.get()

      pepDict = { 'granularity'  : toBeChecked[ 0 ],
                  'name'         : toBeChecked[ 1 ],
                  'statusType'   : toBeChecked[ 2 ],
                  'status'       : toBeChecked[ 3 ],
                  'formerStatus' : toBeChecked[ 4 ],
                  'siteType'     : toBeChecked[ 5 ],
                  'tokenOwner'   : toBeChecked[ 6 ] }

      try:

        gLogger.info( "Checking StorageElement %s, with type/status: %s/%s" % \
                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )

        pepRes = pep.enforce( **pepDict )
        if pepRes.has_key( 'PolicyCombinedResult' ) and pepRes[ 'PolicyCombinedResult' ].has_key( 'Status' ):
          pepStatus = pepRes[ 'PolicyCombinedResult' ][ 'Status' ]
          if pepStatus != pepDict[ 'status' ]:
            gLogger.info( 'Updated Site %s (%s) from %s to %s' %
                          ( pepDict['name'], pepDict['statusType'], pepDict['status'], pepStatus ))

        # remove from InCheck list
        self.StorageElementsNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )

      except Exception:
        gLogger.exception( 'StElInspector._executeCheck' )
        try:
          self.StorageElementsNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )
        except IndexError:
          pass

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
