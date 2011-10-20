################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"
AGENT_NAME = 'ResourceStatus/SSInspectorAgent'

import Queue

from DIRAC                                                  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.Core.Utilities.ThreadPool                        import ThreadPool

from DIRAC.ResourceStatusSystem                             import CheckingFreqs
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command.knownAPIs           import initAPIs
from DIRAC.ResourceStatusSystem.PolicySystem.PEP            import PEP
from DIRAC.ResourceStatusSystem.Utilities.CS                import getSetup, getExt
from DIRAC.ResourceStatusSystem.Utilities.Utils             import where

class SSInspectorAgent( AgentModule ):
  """ 
    The SSInspector agent ( SiteInspectorAgent ) is one of the four
    InspectorAgents of the RSS. 
    
    This Agent takes care of the Sites. In order to do so, it gathers
    the eligible ones and then evaluates their statuses with the PEP. 
  
    If you want to know more about the SSInspectorAgent, scroll down to the 
    end of the file.
  """

  def initialize( self ):
 
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
################################################################################

  def execute( self ):

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
################################################################################

  def _executeCheck( self, _arg ):
    
    # Init the APIs beforehand, and reuse them. 
    __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]
    clients = initAPIs( __APIs__, {} )
    
    pep = PEP( self.VOExtension, setup = self.setup, clients = clients )

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
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF