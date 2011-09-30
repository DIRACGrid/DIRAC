################################################################################
# $HeadURL:  $
################################################################################

import Queue
from DIRAC                                                  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.Utilities.ThreadPool                        import ThreadPool

from DIRAC.ResourceStatusSystem                             import CheckingFreqs
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.PEP            import PEP
from DIRAC.ResourceStatusSystem.Utilities.CS                import getSetup, getExt
from DIRAC.ResourceStatusSystem.Utilities.Utils             import where

__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/RSInspectorAgent'

class RSInspectorAgent( AgentModule ):
  """ Class RSInspectorAgent is in charge of going through Resources
      table, and pass Resource and Status to the PEP
  """

################################################################################

  def initialize( self ):
    """ Standard constructor
    """

    try:
      
      self.VOExtension = getExt()
      self.setup       = getSetup()[ 'Value' ]
      
      self.rsClient             = ResourceStatusClient()
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

  def execute( self ):
    """
    The main RSInspectorAgent execution method.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getStuffToCheck` and
    put result in self.ResourcesToBeChecked (a Queue) and in self.ResourceNamesInCheck (a list)
    """

    try:

      kwargs = { 'columns' : [ 'ResourceName', 'StatusType', 'Status', 'FormerStatus', \
                              'SiteType', 'ResourceType', 'TokenOwner' ] }

      resQuery = self.rsClient.getStuffToCheck( 'Resource', self.ResourcesFreqs, **kwargs )

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

  def _executeCheck( self, _arg ):
    """
    Create instance of a PEP, instantiated popping a resource from lists.
    """
    
    pep = PEP( self.VOExtension, setup = self.setup )

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
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF