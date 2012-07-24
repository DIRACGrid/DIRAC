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
    self.inspectionFreqs = { '' : { 'Active'  : 8, 
                                    'Bad'     : 6, 
                                    'Probing' : 4, 
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
      
      if not elemDict[ 'ElementType' ] in self.inspectionFreqs:
        self.log.error( '"%s" not in inspectionFreqs' % elemDict[ 'ElementType' ] )
        continue
      
      timeToNextCheck = self.inspectionFreqs[ elemDict[ 'ElementType' ] ][ elemDict[ 'Status' ] ]      
      if utcnow - datetime.timedelta( minutes = timeToNextCheck ) > elemDict[ 'LastCheckTime' ]:
        
        
        # We are not checking if the item is already on the queue or not. It may
        # be there, but in any case, it is not a big problem.
        
        lowerElementDict = { 'element' : 'Site' }
        for key, value in elemDict.items():
          lowerElementDict[ key[0].lower() + key[1:] ] = value
        
        self.elementsToBeChecked.put( lowerElementDict )
        self.log.info( '%s # "%s" # "%s" # %s # %s' % ( elemDict[ 'Name' ], 
                                                        elemDict[ 'ElementType' ],
                                                        elemDict[ 'StatusType' ],
                                                        elemDict[ 'Status' ],
                                                        elemDict[ 'LastCheckTime' ]) )
       
    
    # Measure size of the queue, more or less, to know how many threads should
    # we start !
    queueSize      = self.elementsToBeChecked.qsize()
    # 30, could have been other number.. but it works reasonably well.
    threadsToStart = max( min( self.maxNumberOfThreads, queueSize / 30 ), 1 ) 
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

    tHeader = '%sJob%d' % ( '* '*30, threadNumber )
    
    self.log.info( '%s UP' % tHeader )
    
    pep = PEP( clients = self.clients )
    
    while True:
    
      try:
        element = self.elementsToBeChecked.get_nowait()
      except Queue.Empty:
        self.log.info( '%s DOWN' % tHeader )
        return S_OK()
      
      self.log.info( '%s being processed' % element[ 'name' ] )
      
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