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

AGENT_NAME = 'ResourceStatus/SSInspectorAgent'

class SSInspectorAgent( AgentModule ):
  """ Class SSInspectorAgent is in charge of going through Sites
      table, and pass Site and Status to the PEP
  """

################################################################################

  def initialize( self ):
    """ Standard constructor
    """

    try:
      
      self.VOExtension = getExt()
      self.setup       = getSetup()[ 'Value' ]
      
      self.rsClient         = ResourceStatusClient()
      self.SitesFreqs       = CheckingFreqs[ 'SitesFreqs' ]
      self.SitesToBeChecked = Queue.Queue()
      self.SiteNamesInCheck = []

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
      errorStr = "SSInspectorAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################

  def execute( self ):
    """
    The main RSInspectorAgent execution method.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getResourcesToCheck` and
    put result in self.SitesToBeChecked (a Queue) and in self.SiteNamesInCheck (a list)
    """

    try:
      
      kwargs = { 'columns' : ['SiteName', 'StatusType', 'Status', 'FormerStatus',\
                               'SiteType', 'TokenOwner'] }
      resQuery = self.rsClient.getStuffToCheck( 'Site', self.SitesFreqs, **kwargs )

      for siteTuple in resQuery[ 'Value' ]:
        
        #THIS IS IMPORTANT !!
        #Ignore all elements with token != RS_SVC
        if siteTuple[ 5 ] != 'RS_SVC':
          continue
        
        if ( siteTuple[ 0 ],siteTuple[ 1 ] ) in self.SiteNamesInCheck:
          continue
        
        resourceL = [ 'Site' ] + siteTuple

        self.SiteNamesInCheck.insert( 0, ( siteTuple[ 0 ], siteTuple[ 1 ] ) )
        self.SitesToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      gLogger.exception( errorStr, lException = x )
      return S_ERROR( errorStr )

################################################################################

  def _executeCheck( self, _arg ):
    """
    Create instance of a PEP, instantiated popping a resource from lists.
    """
    
    pep = PEP( self.VOExtension, setup = self.setup )

    while True:

      try:

        toBeChecked  = self.SitesToBeChecked.get()

        pepDict = { 'granularity'  : toBeChecked[ 0 ],
                    'name'         : toBeChecked[ 1 ],
                    'statusType'   : toBeChecked[ 2 ],
                    'status'       : toBeChecked[ 3 ],
                    'formerStatus' : toBeChecked[ 4 ],
                    'siteType'     : toBeChecked[ 5 ],
                    'tokenOwner'   : toBeChecked[ 6 ] }

        gLogger.info( "Checking Site %s, with type/status: %s/%s" % \
                      ( pepDict['name'], pepDict['statusType'], pepDict['status'] ) )
     
        pep.enforce( **pepDict )

        # remove from InCheck list
        self.SiteNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )       

      except Exception:
        gLogger.exception( 'SSInspector._executeCheck' )
        try:
          self.SiteNamesInCheck.remove( ( pepDict[ 'name' ], pepDict[ 'statusType' ] ) )
        except IndexError:
          pass

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF