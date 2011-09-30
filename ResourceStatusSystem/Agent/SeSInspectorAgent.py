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

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/SeSInspectorAgent'

class SeSInspectorAgent( AgentModule ):
  """ Class SeSInspectorAgent is in charge of going through Services
      table, and pass Service and Status to the PEP
  """

################################################################################

  def initialize( self ):
    """ Standard constructor
    """

    try:
      
      self.VOExtension = getExt()
      self.setup       = getSetup()[ 'Value' ]
      
      self.rsClient            = ResourceStatusClient()
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

  def execute( self ):
    """
    The main SSInspectorAgent execution method.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getResourcesToCheck` and
    put result in self.ServicesToBeChecked (a Queue) and in self.ServiceNamesInCheck (a list)
    """

    try:

      kwargs = { 'columns' : [ 'ServiceName', 'StatusType', 'Status', 'FormerStatus', \
                              'SiteType', 'ServiceType', 'TokenOwner' ] }
      resQuery = self.rsClient.getStuffToCheck( 'Service', self.ServicesFreqs, **kwargs )

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

  def _executeCheck( self, _arg ):
    """
    Create instance of a PEP, instantiated popping a service from lists.
    """

    pep = PEP( self.VOExtension, setup = self.setup )

    while True:

      try:

        toBeChecked  = self.ServicesToBeChecked.get()

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
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF