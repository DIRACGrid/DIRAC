################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/RSInspectorAgent'

import Queue

from DIRAC                                            import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                      import AgentModule
from DIRAC.Core.Utilities.ThreadPool                  import ThreadPool

from DIRAC.ResourceStatusSystem                       import CheckingFreqs
from DIRAC.ResourceStatusSystem.API.ResourceStatusAPI import ResourceStatusAPI
from DIRAC.ResourceStatusSystem.Command.knownAPIs     import initAPIs
from DIRAC.ResourceStatusSystem.PolicySystem.PEP      import PEP
from DIRAC.ResourceStatusSystem.Utilities.CS          import getSetup, getExt
from DIRAC.ResourceStatusSystem.Utilities.Utils       import where

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
      
      self.VOExtension = getExt()
      self.setup       = getSetup()[ 'Value' ]
      
      self.rsAPI             = ResourceStatusAPI()
      self.ResourcesFreqs       = CheckingFreqs[ 'ResourcesFreqs' ]
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
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
################################################################################

  def execute( self ):

    try:

      kwargs = { 'columns' : [ 'ResourceName', 'StatusType', 'Status', 'FormerStatus', \
                              'SiteType', 'ResourceType', 'TokenOwner' ] }

      resQuery = self.rsAPI.getStuffToCheck( 'Resource', self.ResourcesFreqs, **kwargs )

      for resourceTuple in resQuery[ 'Value' ]:
        
        #THIS IS IMPORTANT !!
        #Ignore all elements with token != RS_SVC
        if resourceTuple[ 6 ] != 'RS_SVC':
          continue
        
        if ( resourceTuple[ 0 ], resourceTuple[ 1 ] ) in self.ResourceNamesInCheck:
          continue
        
        resourceL = [ 'Resource' ] + resourceTuple
        
        self.ResourceNamesInCheck.insert( 0, ( resourceTuple[ 0 ], resourceTuple[ 1 ] ) )
        self.ResourcesToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      gLogger.exception( errorStr,lException=x )
      return S_ERROR( errorStr )


################################################################################
################################################################################

  def _executeCheck( self, _arg ):
    
    # Init the APIs beforehand, and reuse them. 
    __APIs__ = [ 'ResourceStatusAPI', 'ResourceManagementAPI' ]
    clients = initAPIs( __APIs__, {} )
    
    pep = PEP( self.VOExtension, setup = self.setup, clients = clients )

    while True:

      try:

        toBeChecked  = self.ResourcesToBeChecked.get()

        pepDict = { 'granularity'  : toBeChecked[ 0 ],
                    'name'         : toBeChecked[ 1 ],
                    'statusType'   : toBeChecked[ 2 ],
                    'status'       : toBeChecked[ 3 ],
                    'formerStatus' : toBeChecked[ 4 ],
                    'siteType'     : toBeChecked[ 5 ],
                    'resourceType' : toBeChecked[ 6 ],
                    'tokenOwner'   : toBeChecked[ 7 ] }

        gLogger.info( "Checking Resource %s, with type/status: %s/%s" % \
                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )
       
        pep.enforce( **pepDict )

        # remove from InCheck list
        self.ResourceNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )

      except Exception:
        gLogger.exception( 'RSInspector._executeCheck' )
        try:
          self.ResourceNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )
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