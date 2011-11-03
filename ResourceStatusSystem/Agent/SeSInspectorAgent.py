################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id: $"
AGENT_NAME = 'ResourceStatus/SeSInspectorAgent'

import Queue, time

from DIRAC                                            import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                      import AgentModule
from DIRAC.Core.Utilities.ThreadPool                  import ThreadPool

from DIRAC.ResourceStatusSystem                       import CheckingFreqs
from DIRAC.ResourceStatusSystem.API.ResourceStatusAPI import ResourceStatusAPI
from DIRAC.ResourceStatusSystem.Command.knownAPIs     import initAPIs
from DIRAC.ResourceStatusSystem.PolicySystem.PEP      import PEP
from DIRAC.ResourceStatusSystem.Utilities.CS          import getSetup, getExt
from DIRAC.ResourceStatusSystem.Utilities.Utils       import where

class SeSInspectorAgent( AgentModule ):
  """ 
    The SeSInspector agent ( ServiceInspectorAgent ) is one of the four
    InspectorAgents of the RSS. 
    
    This Agent takes care of the Service. In order to do so, it gathers
    the eligible ones and then evaluates their statuses with the PEP. 
  
    If you want to know more about the SeSInspectorAgent, scroll down to the 
    end of the file.
  """

  def initialize( self ):

    try:
      
      self.VOExtension = getExt()
      self.setup       = getSetup()[ 'Value' ]
      
      self.rsAPI               = ResourceStatusAPI()
      self.ServicesFreqs       = CheckingFreqs[ 'ServicesFreqs' ]
      self.ServicesToBeChecked = Queue.Queue()
      self.ServiceNamesInCheck = []

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
      errorStr = "SeSInspectorAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
################################################################################

  def execute( self ):

    try:

      kwargs = { 'columns' : [ 'ServiceName', 'StatusType', 'Status', 'FormerStatus', \
                              'SiteType', 'ServiceType', 'TokenOwner' ] }
      resQuery = self.rsAPI.getStuffToCheck( 'Service', self.ServicesFreqs, **kwargs )

      for serviceTuple in resQuery[ 'Value' ]:
          
        #THIS IS IMPORTANT !!
        #Ignore all elements with token != RS_SVC  
        if serviceTuple[ 6 ] != 'RS_SVC':
          continue
          
        if ( serviceTuple[ 0 ], serviceTuple[ 1 ] ) in self.ServiceNamesInCheck:
          continue
        
        resourceL = [ 'Service' ] + serviceTuple
          
        self.ServiceNamesInCheck.insert( 0, ( serviceTuple[ 0 ], serviceTuple[ 1 ] ) )
        self.ServicesToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      gLogger.exception( errorStr, lException = x )
      return S_ERROR( errorStr )

################################################################################
################################################################################

  def finalize( self ):
    if self.ServiceNamesInCheck:
      _msg = "Wait for queue to get empty before terminating the agent (%d tasks)" 
      _msg = _msg % len( self.ServiceNamesInCheck )
      self.log.info( _msg )
      while self.ServiceNamesInCheck:
        time.sleep( 2 )
      self.log.info( "Queue is empty, terminating the agent..." )
    return S_OK()

################################################################################
################################################################################

  def _executeCheck( self, _arg ):

    # Init the APIs beforehand, and reuse them. 
    __APIs__ = [ 'ResourceStatusAPI', 'ResourceManagementAPI', 'SLSClient' ]
    clients = initAPIs( __APIs__, {} )
    
    pep = PEP( self.VOExtension, setup = self.setup, clients = clients )

    while True:

      try:

        toBeChecked = self.ServicesToBeChecked.get()

        pepDict = { 'granularity'  : toBeChecked[ 0 ],
                    'name'         : toBeChecked[ 1 ],
                    'statusType'   : toBeChecked[ 2 ],
                    'status'       : toBeChecked[ 3 ],
                    'formerStatus' : toBeChecked[ 4 ],
                    'siteType'     : toBeChecked[ 5 ],
                    'serviceType'  : toBeChecked[ 6 ],
                    'tokenOwner'   : toBeChecked[ 7 ] }

        gLogger.info( "Checking Service %s, with type/status: %s/%s" % \
                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )

        pep.enforce( **pepDict )     

        # remove from InCheck list
        self.ServiceNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )

      except Exception:
        gLogger.exception( 'SeSInspector._executeCheck' )
        try:
          self.ServiceNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )
        except IndexError:
          pass

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF