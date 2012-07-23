# $HeadURL:  $
''' RSInspectorAgent

  This agent inspect Resources, and evaluates policies that apply.

'''

import datetime
import Queue

from DIRAC                                                      import S_OK
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.Utilities.ThreadPool                            import ThreadPool

from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.PolicySystem.PEP                import PEP

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/ElementInspectorAgent'

class ElementInspectorAgent( AgentModule ):
  '''
    The ElementInspector agent is a generic agent used to check the elements
    of one of the elementTypes ( e.g. Site, Resource, Node ).

    This Agent takes care of the Elements. In order to do so, it gathers
    the eligible ones and then evaluates their statuses with the PEP.
  '''

  def initialize( self ):
    
    
    self.maxNumberOfThreads  = 5
#    self.maxNumberOfThreads  = self.am_getOption( 'maxThreadsInPool', 5 )
    self.elementType         = 'Site'
#    self.elementType         = self.am_getOption( 'elementType' )
    #self.inspectionFreqs     = self.am_getOption( 'inspectionFreqs' )
    self.inspectionFreqs = { '' : { 'Active'  : 2, 
                                    'Bad'     : 2, 
                                    'Probing' : 2, 
                                    'Banned'  : 2 } }
    
    
    self.elementsToBeChecked = Queue.Queue()
    self.threadPool          = ThreadPool( self.maxNumberOfThreads,
                                           self.maxNumberOfThreads )

    self.rsClient            = ResourceStatusClient()

    self.clients             = { 
                                 'ResourceStatusClient'     : self.rsClient,
                                 'ResourceManagementClient' : ResourceManagementClient() 
                               }

    # Do we really need multi-threading ?, most of the times there are few
    # entries to be checked !
    #for _i in xrange( self.maxNumberOfThreads ):
    #  self.threadPool.generateJobAndQueueIt( self._executeCheck, args = ( None, ) )

    return S_OK()
  
  def execute( self ):
    
    # We get all the elements, then we filter.
    elements = self.rsClient.selectStatusElement( self.elementType, 'Status' )
    if not elements[ 'OK' ]:
      self.log.error( elements[ 'Message' ] )
      return elements
      
    utcnow = datetime.datetime.utcnow().replace( microsecond = 0 )  
       
    # filter elements by Type
    for element in elements[ 'Value' ]:
      
      # Maybe an overkill, but this way I have never again to worry about order
      elemDict = dict( zip( elements[ 'Columns' ], element ) )
      
      timeToNextCheck = self.inspectionFreqs[ elemDict[ 'ElementType' ] ][ elemDict[ 'Status' ] ]      
      if utcnow - datetime.timedelta( minutes = timeToNextCheck ) > elemDict[ 'LastCheckTime' ]:
        
        
        # We are not checking if the item is already on the queue or not. It may
        # be there, but in any case, it is not a big problem.
        
        lowerElementDict = { 'element' : 'Site' }
        for key, value in elemDict.items():
          lowerElementDict[ key[0].lower() + key[1:] ] = value
        
        self.elementsToBeChecked.put( lowerElementDict )
        self.log.info( '"%s"-"%s"-"%s"-"%s"-"%s"' % ( elemDict[ 'Name' ], 
                                                      elemDict[ 'ElementType' ],
                                                      elemDict[ 'StatusType' ],
                                                      elemDict[ 'Status' ],
                                                      elemDict[ 'LastCheckTime' ]) )
       
    
    # Measure size of the queue, more or less, to know how many threads should
    # we start !
    queueSize      = self.elementsToBeChecked.qsize()
    # 30, could have been other number.. but it works reasonably well.
    threadsToStart = min( self.maxNumberOfThreads, queueSize / 30 ) 
    threadsRunning = self.threadPool.numWorkingThreads()
    
    self.log.info( 'Starting %d jobs to process %d elements' % ( threadsToStart, queueSize ) )
    if threadsRunning:
      self.log.info( 'Already %d jobs running' % threadsRunning )
      threadsToStart = max( 0, threadsToStart - threadsRunning )
      self.log.info( 'Starting %d jobs to process %d elements' % ( threadsToStart, queueSize ) )
    
    for _x in xrange( threadsToStart ):
      self.threadPool.generateJobAndQueueIt( self._execute, args = ( _x, ) )
       
    return S_OK()

  def finalize( self ):
    
    self.log.info( 'draining queue... blocking until empty' )
    # block until all tasks are done
    self.elementsToBeChecked.join()  
    
    return S_OK()
        
  def _execute( self, threadNumber ):

    tHeader = 'Job%d' % threadNumber
    
    self.log.info( '%s UP' % tHeader )
    
    pep = PEP( clients = self.clients )
    
    while True:
    
      try:
        element = self.elementsToBeChecked.get_nowait()
      except Queue.Empty:
        self.log.info( '%s DOWN' % tHeader )
        return S_OK()
      
      self.log.info( '%s processed' % element[ 'name' ] )
      resEnforce = pep.enforce( element )
      
      oldStatus  = resEnforce[ 'decissionParams' ][ 'status' ]
      statusType = resEnforce[ 'decissionParams' ][ 'statusType' ]
      newStatus  = resEnforce[ 'policyCombinedResult' ][ 'Status' ]
      reason     = resEnforce[ 'policyCombinedResult' ][ 'Reason' ]
      
      if oldStatus != newStatus:
        self.log.info( '%s (%s) is now %s ( %s ), before %s' % ( element[ 'name' ], statusType,
                                                                 newStatus, reason, oldStatus ) )
        
      # Used together with join !
      self.elementsToBeChecked.task_done()   

    return S_OK()

#
#  def initialize( self ):
#
#    # Attribute defined outside __init__ 
#    # pylint: disable-msg=W0201
#
#    try:
#      self.rsClient             = ResourceStatusClient()
#      self.resourcesFreqs       = CS.getTypedDictRootedAtOperations( 'CheckingFreqs/ResourcesFreqs' )
#      self.resourcesToBeChecked = Queue.Queue()
#      self.resourceNamesInCheck = []
#
#      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
#      self.threadPool         = ThreadPool( self.maxNumberOfThreads,
#                                            self.maxNumberOfThreads )
#      if not self.threadPool:
#        self.log.error( 'Can not create Thread Pool' )
#        return S_ERROR( 'Can not create Thread Pool' )
#
#      for _i in xrange( self.maxNumberOfThreads ):
#        self.threadPool.generateJobAndQueueIt( self._executeCheck, args = ( None, ) )
#
#      return S_OK()
#
#    except Exception:
#      errorStr = "RSInspectorAgent initialization"
#      self.log.exception( errorStr )
#      return S_ERROR( errorStr )
#
#  def execute( self ):
#
#    try:
#
#      kwargs = { 'meta' : {} }
#      kwargs['meta']['columns'] = [ 'ResourceName', 'StatusType', 'Status',
#                                    'FormerStatus', 'SiteType', 'ResourceType', \
#                                    'TokenOwner' ]
#      kwargs[ 'tokenOwner' ]    = 'RS_SVC'
#
#      resQuery = self.rsClient.getStuffToCheck( 'Resource', self.resourcesFreqs, **kwargs )
#      if not resQuery[ 'OK' ]:
#        self.log.error( resQuery[ 'Message' ] )
#        return resQuery
#
#      resQuery = resQuery[ 'Value' ]  
#      self.log.info( 'Found %d candidates to be checked.' % len( resQuery ) )
#
#      for resourceTuple in resQuery:
#
#        if ( resourceTuple[ 0 ], resourceTuple[ 1 ] ) in self.resourceNamesInCheck:
#          self.log.info( '%s(%s) discarded, already on the queue' % ( resourceTuple[ 0 ], resourceTuple[ 1 ] ) )
#          continue
#
#        resourceL = [ 'Resource' ] + resourceTuple
#
#        self.resourceNamesInCheck.insert( 0, ( resourceTuple[ 0 ], resourceTuple[ 1 ] ) )
#        self.resourcesToBeChecked.put( resourceL )
#
#      return S_OK()
#
#    except Exception, x:
#      errorStr = where( self, self.execute )
#      self.log.exception( errorStr, lException = x )
#      return S_ERROR( errorStr )
#
#  def finalize( self ):
#    '''
#      Method executed at the end of the last cycle. It waits until the queue
#      is empty.
#    '''   
#    if self.resourceNamesInCheck:
#      _msg = "Wait for queue to get empty before terminating the agent (%d tasks)"
#      _msg = _msg % len( self.resourceNamesInCheck )
#      self.log.info( _msg )
#      while self.resourceNamesInCheck:
#        time.sleep( 2 )
#      self.log.info( "Queue is empty, terminating the agent..." )
#    return S_OK()
#
#################################################################################
#
#  def _executeCheck( self, _arg ):
#    '''
#      Method executed by the threads in the pool. Picks one element from the
#      common queue, and enforces policies on that element.
#    '''
#    # Init the APIs beforehand, and reuse them.
#    __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]
#    clients = knownAPIs.initAPIs( __APIs__, {} )
#
#    pep = PEP( clients = clients )
#
#    while True:
#
#      toBeChecked  = self.resourcesToBeChecked.get()
#
#      pepDict = { 'granularity'  : toBeChecked[ 0 ],
#                  'name'         : toBeChecked[ 1 ],
#                  'statusType'   : toBeChecked[ 2 ],
#                  'status'       : toBeChecked[ 3 ],
#                  'formerStatus' : toBeChecked[ 4 ],
#                  'siteType'     : toBeChecked[ 5 ],
#                  'resourceType' : toBeChecked[ 6 ],
#                  'tokenOwner'   : toBeChecked[ 7 ] }
#
#      try:
#
#        self.log.info( "Checking Resource %s, with type/status: %s/%s" % \
#                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )
#
#        pepRes =  pep.enforce( **pepDict )
#        if pepRes.has_key( 'PolicyCombinedResult' ) and pepRes[ 'PolicyCombinedResult' ].has_key( 'Status' ):
#          pepStatus = pepRes[ 'PolicyCombinedResult' ][ 'Status' ]
#          if pepStatus != pepDict[ 'status' ]:
#            self.log.info( 'Updated Site %s (%s) from %s to %s' %
#                          ( pepDict['name'], pepDict['statusType'], pepDict['status'], pepStatus ))
#
#        # remove from InCheck list
#        self.resourceNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )
#
#      except Exception:
#        self.log.exception( "RSInspector._executeCheck Checking Resource %s, with type/status: %s/%s" % \
#                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )
#        try:
#          self.resourceNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )
#        except IndexError:
#          pass

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF