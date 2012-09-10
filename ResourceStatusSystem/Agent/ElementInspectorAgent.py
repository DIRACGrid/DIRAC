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

__RCSID__  = '$Id:  $'
AGENT_NAME = 'ResourceStatus/ElementInspectorAgent'

class ElementInspectorAgent( AgentModule ):
  '''
    The ElementInspector agent is a generic agent used to check the elements
    of one of the elementTypes ( e.g. Site, Resource, Node ).

    This Agent takes care of the Elements. In order to do so, it gathers
    the eligible ones and then evaluates their statuses with the PEP.
  '''

  # Max number of worker threads by default
  __maxNumberOfThreads = 5
  # ElementType, to be defined among Site, Resource or Node
  __elementType = None
  # Inspection freqs, defaults, the lower, the higher priority to be checked.
  # Error state usually means there is a glitch somewhere, so it has the highest
  # priority.
  __checkingFreqs = { 'Default' : 
                       { 
                         'Active' : 60, 'Degraded' : 30,  'Probing' : 30, 
                         'Banned' : 30, 'Unknown'  : 15,  'Error'   : 15 
                         } 
                     }
  # queue size limit to stop feeding
  __limitQueueFeeder = 15
  
  def __init__( self, agentName, loadName, baseAgentName = False, properties = {} ):
    
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties ) 

    # members initialization

    self.maxNumberOfThreads = self.__maxNumberOfThreads
    self.elementType        = self.__elementType
    self.checkingFreqs      = self.__checkingFreqs
    self.limitQueueFeeder   = self.__limitQueueFeeder
    
    self.elementsToBeChecked = None
    self.threadPool          = None
    self.rsClient            = None
    self.clients             = {}

  def initialize( self ):
    
    self.maxNumberOfThreads = self.am_getOption( 'maxNumberOfThreads', self.maxNumberOfThreads )   
    self.elementType        = self.am_getOption( 'elementType',        self.elementType )
    self.checkingFreqs      = self.am_getOption( 'checkingFreqs',      self.checkingFreqs )
    self.limitQueueFeeder   = self.am_getOption( 'limitQueueFeeder',   self.limitQueueFeeder )      
    
    self.elementsToBeChecked = Queue.Queue()
    self.threadPool          = ThreadPool( self.maxNumberOfThreads,
                                           self.maxNumberOfThreads )

    self.rsClient = ResourceStatusClient()

    self.clients[ 'ResourceStatusClient' ]     = self.rsClient
    self.clients[ 'ResourceManagementClient' ] = ResourceManagementClient() 

    return S_OK()
  
  def execute( self ):
    
    # If there are elements in the queue to be processed, we wait ( we know how
    # many elements in total we can have, so if there are more than 15% of them
    # on the queue, we do not add anything ), but the threads are running and
    # processing items from the queue on background.    
    
    qsize = self.elementsToBeChecked.qsize() 
    if qsize > self.limitQueueFeeder:
      self.log.warn( 'Queue not empty ( %s > %s ), skipping feeding loop' % ( qsize, self.limitQueueFeeder ) )
      return S_OK()
    
    # We get all the elements, then we filter.
    elements = self.rsClient.selectStatusElement( self.elementType, 'Status' )
    if not elements[ 'OK' ]:
      self.log.error( elements[ 'Message' ] )
      return elements
      
    utcnow = datetime.datetime.utcnow().replace( microsecond = 0 )  
       
    # filter elements by Type
    for element in elements[ 'Value' ]:
      
      # Maybe an overkill, but this way I have NEVER again to worry about order
      # of elements returned by mySQL on tuples
      elemDict = dict( zip( elements[ 'Columns' ], element ) )
      
      if not elemDict[ 'ElementType' ] in self.checkingFreqs:
        #self.log.warn( '"%s" not in inspectionFreqs, getting default' % elemDict[ 'ElementType' ] )
        timeToNextCheck = self.checkingFreqs[ 'Default' ][ elemDict[ 'Status' ] ]
      else:
        timeToNextCheck = self.checkingFreqs[ elemDict[ 'ElementType' ] ][ elemDict[ 'Status' ] ]
              
      if utcnow - datetime.timedelta( minutes = timeToNextCheck ) > elemDict[ 'LastCheckTime' ]:
               
        # We are not checking if the item is already on the queue or not. It may
        # be there, but in any case, it is not a big problem.
        
        lowerElementDict = { 'element' : self.elementType }
        for key, value in elemDict.items():
          lowerElementDict[ key[0].lower() + key[1:] ] = value
        
        # We add lowerElementDict to the queue
        self.elementsToBeChecked.put( lowerElementDict )
        self.log.info( '%s # "%s" # "%s" # %s # %s' % ( elemDict[ 'Name' ], 
                                                        elemDict[ 'ElementType' ],
                                                        elemDict[ 'StatusType' ],
                                                        elemDict[ 'Status' ],
                                                        elemDict[ 'LastCheckTime' ]) )
       
    # Measure size of the queue, more or less, to know how many threads should
    # we start !
    queueSize      = self.elementsToBeChecked.qsize()
    # 30, could have been other number.. but it works reasonably well. ( +1 to get ceil )
    threadsToStart = max( min( self.maxNumberOfThreads, ( queueSize / 30 ) + 1 ), 1 ) 
    threadsRunning = self.threadPool.numWorkingThreads()
    
    self.log.info( 'Needed %d threads to process %d elements' % ( threadsToStart, queueSize ) )
    if threadsRunning:
      self.log.info( 'Already %d threads running' % threadsRunning )
      threadsToStart = max( 0, threadsToStart - threadsRunning )
      self.log.info( 'Starting %d threads to process %d elements' % ( threadsToStart, queueSize ) )
    
    for _x in xrange( threadsToStart ):
      jobUp = self.threadPool.generateJobAndQueueIt( self._execute, args = ( _x, ) )
      if not jobUp[ 'OK' ]:
        self.log.error( jobUp[ 'Message' ] )
        
    return S_OK()

  def finalize( self ):
    
    self.log.info( 'draining queue... blocking until empty' )
    # block until all tasks are done
    self.elementsToBeChecked.join()  
    
    return S_OK()
        
## Private methods #############################################################        
        
  def _execute( self, threadNumber ):
    '''
      Method run by the thread pool. It enters a loop until there are no elements
      on the queue. On each iteration, it evaluates the policies for such element
      and enforces the necessary actions. If there are no more elements in the
      queue, the loop is finished.
    '''

    tHeader = '%sJob%d' % ( '* '*30, threadNumber )
    
    self.log.info( '%s UP' % tHeader )
    
    pep = PEP( clients = self.clients )
    
    while True:
    
      try:
        element = self.elementsToBeChecked.get_nowait()
      except Queue.Empty:
        self.log.info( '%s DOWN' % tHeader )
        return S_OK()
      
      self.log.info( '%s ( %s ) being processed' % ( element[ 'name' ], element[ 'status' ] ) )
      
      resEnforce = pep.enforce( element )
      if not resEnforce[ 'OK' ]:
        self.log.error( resEnforce[ 'Message' ] )
        self.elementsToBeChecked.task_done()
        continue
      
      resEnforce = resEnforce[ 'Value' ]  
      
      oldStatus  = resEnforce[ 'decissionParams' ][ 'status' ]
      statusType = resEnforce[ 'decissionParams' ][ 'statusType' ]
      newStatus  = resEnforce[ 'policyCombinedResult' ][ 'Status' ]
      reason     = resEnforce[ 'policyCombinedResult' ][ 'Reason' ]
      
      if oldStatus != newStatus:
        self.log.info( '%s (%s) is now %s ( %s ), before %s' % ( element[ 'name' ], 
                                                                 statusType,
                                                                 newStatus, 
                                                                 reason, 
                                                                 oldStatus ) )
        
      # Used together with join !
      self.elementsToBeChecked.task_done()   

    self.log.info( '%s DOWN' % tHeader )

    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF