# $HeadURL:  $
''' StElInspectorAgent

  This agent inspect Sites, and evaluates policies that apply.

'''

import Queue
import time

from DIRAC                                                  import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.Utilities.ThreadPool                        import ThreadPool
from DIRAC.ResourceStatusSystem.Utilities                   import CS
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command                     import knownAPIs
from DIRAC.ResourceStatusSystem.PolicySystem.PEP            import PEP
from DIRAC.ResourceStatusSystem.Utilities.Utils             import where

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/StElInspectorAgent'

class StElInspectorAgent( AgentModule ):
  '''
    The StElInspector agent ( StorageElementInspectorAgent ) is one of the four
    InspectorAgents of the RSS.

    This Agent takes care of the StorageElements. In order to do so, it gathers
    the eligible ones and then evaluates their statuses with the PEP.

    If you want to know more about the StElInspectorAgent, scroll down to the
    end of the file.
  '''

  def initialize( self ):

    # pylint: disable-msg=W0201

    try:
      self.rsClient                    = ResourceStatusClient()
      self.storageElementsFreqs        = CS.getTypedDictRootedAt( 'CheckingFreqs/StorageElementsFreqs' )
      self.storageElementsToBeChecked  = Queue.Queue()
      self.storageElementsNamesInCheck = []

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
      self.log.exception( errorStr )
      return S_ERROR( errorStr )

  def execute( self ):

    try:

      kwargs = { 'meta' : {} }
      kwargs['meta']['columns'] = [ 'StorageElementName', 'StatusType',
                                    'Status', 'FormerStatus', 'SiteType', \
                                    'TokenOwner' ]
      kwargs[ 'tokenOwner' ]    = 'RS_SVC'

      resQuery = self.rsClient.getStuffToCheck( 'StorageElement', self.storageElementsFreqs, **kwargs )
      if not resQuery[ 'OK' ]:
        self.log.error( resQuery[ 'Message' ] )
        return resQuery

      resQuery = resQuery[ 'Value' ]  
      self.log.info( 'Found %d candidates to be checked.' % len( resQuery ) )

      for seTuple in resQuery:

        if ( seTuple[ 0 ], seTuple[ 1 ] ) in self.storageElementsNamesInCheck:
          self.log.info( '%s(%s) discarded, already on the queue' % ( seTuple[ 0 ], seTuple[ 1 ] ) )
          continue

        resourceL = [ 'StorageElement' ] + seTuple

        # the tuple consists on ( SEName, SEStatusType )
        self.storageElementsNamesInCheck.insert( 0, ( resourceL[ 1 ], resourceL[ 2 ] ) )
        self.storageElementsToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      self.log.exception( errorStr, lException = x )
      return S_ERROR( errorStr )

  def finalize( self ):
    '''
      Method executed at the end of the last cycle. It waits until the queue
      is empty.
    '''
    if self.storageElementsNamesInCheck:
      _msg = "Wait for queue to get empty before terminating the agent (%d tasks)"
      _msg = _msg % len( self.storageElementsNamesInCheck )
      self.log.info( _msg )
      while self.storageElementsNamesInCheck:
        time.sleep( 2 )
      self.log.info( "Queue is empty, terminating the agent..." )
    return S_OK()

################################################################################

  def _executeCheck( self, _arg ):
    '''
      Method executed by the threads in the pool. Picks one element from the
      common queue, and enforces policies on that element.
    '''
    # Init the APIs beforehand, and reuse them.
    __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]
    clients = knownAPIs.initAPIs( __APIs__, {} )

    pep = PEP( clients = clients )

    while True:

      toBeChecked = self.storageElementsToBeChecked.get()

      pepDict = { 'granularity'  : toBeChecked[ 0 ],
                  'name'         : toBeChecked[ 1 ],
                  'statusType'   : toBeChecked[ 2 ],
                  'status'       : toBeChecked[ 3 ],
                  'formerStatus' : toBeChecked[ 4 ],
                  'siteType'     : toBeChecked[ 5 ],
                  'tokenOwner'   : toBeChecked[ 6 ] }

      try:

        self.log.info( "Checking StorageElement %s, with type/status: %s/%s" % \
                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )

        pepRes = pep.enforce( **pepDict )
        if pepRes.has_key( 'PolicyCombinedResult' ) and pepRes[ 'PolicyCombinedResult' ].has_key( 'Status' ):
          pepStatus = pepRes[ 'PolicyCombinedResult' ][ 'Status' ]
          if pepStatus != pepDict[ 'status' ]:
            self.log.info( 'Updated Site %s (%s) from %s to %s' %
                          ( pepDict['name'], pepDict['statusType'], pepDict['status'], pepStatus ))

        # remove from InCheck list
        self.storageElementsNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )

      except Exception:
        self.log.exception( 'StElInspector._executeCheck' )
        try:
          self.storageElementsNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )
        except IndexError:
          pass

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF