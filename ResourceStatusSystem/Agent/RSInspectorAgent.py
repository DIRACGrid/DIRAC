################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/RSInspectorAgent'

import Queue, time

from DIRAC                                                  import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.Utilities.ThreadPool                        import ThreadPool

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command                     import knownAPIs
from DIRAC.ResourceStatusSystem.PolicySystem.PEP            import PEP
from DIRAC.ResourceStatusSystem.Utilities.Utils             import where
from DIRAC.ResourceStatusSystem.Utilities                   import CS

class RSInspectorAgent( AgentModule ):
  """
    The RSInspector agent ( ResourceInspectorAgent ) is one of the four
    InspectorAgents of the RSS.

    This Agent takes care of the Resources. In order to do so, it gathers
    the eligible ones and then evaluates their statuses with the PEP.

    If you want to know more about the RSInspectorAgent, scroll down to the
    end of the file.
  """

  def initialize( self ):

    try:
      self.rsClient             = ResourceStatusClient()
      self.ResourcesFreqs       = CS.getTypedDictRootedAt( 'CheckingFreqs/ResourcesFreqs' )
      self.ResourcesToBeChecked = Queue.Queue()
      self.ResourceNamesInCheck = []

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
      errorStr = "RSInspectorAgent initialization"
      self.log.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
################################################################################

  def execute( self ):

    try:

      kwargs = { 'meta' : {} }
      kwargs['meta']['columns'] = [ 'ResourceName', 'StatusType', 'Status',
                                    'FormerStatus', 'SiteType', 'ResourceType', \
                                    'TokenOwner' ]
      kwargs[ 'tokenOwner' ]    = 'RS_SVC'

      resQuery = self.rsClient.getStuffToCheck( 'Resource', self.ResourcesFreqs, **kwargs )
      if not resQuery[ 'OK' ]:
        self.log.error( resQuery[ 'Message' ] )
        return resQuery

      resQuery = resQuery[ 'Value' ]  
      self.log.info( 'Found %d candidates to be checked.' % len( resQuery ) )

      for resourceTuple in resQuery:

        if ( resourceTuple[ 0 ], resourceTuple[ 1 ] ) in self.ResourceNamesInCheck:
          self.log.info( '%s(%s) discarded, already on the queue' % ( resourceTuple[ 0 ], resourceTuple[ 1 ] ) )
          continue

        resourceL = [ 'Resource' ] + resourceTuple

        self.ResourceNamesInCheck.insert( 0, ( resourceTuple[ 0 ], resourceTuple[ 1 ] ) )
        self.ResourcesToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      self.log.exception( errorStr,lException=x )
      return S_ERROR( errorStr )

################################################################################
################################################################################

  def finalize( self ):
    if self.ResourceNamesInCheck:
      _msg = "Wait for queue to get empty before terminating the agent (%d tasks)"
      _msg = _msg % len( self.ResourceNamesInCheck )
      self.log.info( _msg )
      while self.ResourceNamesInCheck:
        time.sleep( 2 )
      self.log.info( "Queue is empty, terminating the agent..." )
    return S_OK()

################################################################################
################################################################################

  def _executeCheck( self, _arg ):

    # Init the APIs beforehand, and reuse them.
    __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]
    clients = knownAPIs.initAPIs( __APIs__, {} )

    pep = PEP( clients = clients )

    while True:

      toBeChecked  = self.ResourcesToBeChecked.get()

      pepDict = { 'granularity'  : toBeChecked[ 0 ],
                  'name'         : toBeChecked[ 1 ],
                  'statusType'   : toBeChecked[ 2 ],
                  'status'       : toBeChecked[ 3 ],
                  'formerStatus' : toBeChecked[ 4 ],
                  'siteType'     : toBeChecked[ 5 ],
                  'resourceType' : toBeChecked[ 6 ],
                  'tokenOwner'   : toBeChecked[ 7 ] }

      try:

        gLogger.info( "Checking Resource %s, with type/status: %s/%s" % \
                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )

        pepRes =  pep.enforce( **pepDict )
        if pepRes.has_key( 'PolicyCombinedResult' ) and pepRes[ 'PolicyCombinedResult' ].has_key( 'Status' ):
          pepStatus = pepRes[ 'PolicyCombinedResult' ][ 'Status' ]
          if pepStatus != pepDict[ 'status' ]:
            gLogger.info( 'Updated Site %s (%s) from %s to %s' %
                          ( pepDict['name'], pepDict['statusType'], pepDict['status'], pepStatus ))

        # remove from InCheck list
        self.ResourceNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )

      except Exception:
        self.log.exception( "RSInspector._executeCheck Checking Resource %s, with type/status: %s/%s" % \
                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )
        try:
          self.ResourceNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )
        except IndexError:
          pass

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF