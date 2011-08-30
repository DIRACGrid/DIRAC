########################################################################
# $HeadURL:  $
########################################################################

import copy
import Queue
from DIRAC                                              import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.Core.Utilities.ThreadPool                    import ThreadPool
from DIRAC.Interfaces.API.DiracAdmin                    import DiracAdmin
from DIRAC.ConfigurationSystem.Client.CSAPI             import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient

from DIRAC.ResourceStatusSystem.Utilities.CS            import getSetup, getExt
from DIRAC.ResourceStatusSystem.Utilities.Utils         import where

from DIRAC.ResourceStatusSystem                         import CheckingFreqs
from DIRAC.ResourceStatusSystem.PolicySystem.PEP        import PEP
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB     import ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB


__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/SeSInspectorAgent'

class SeSInspectorAgent( AgentModule ):
  """ Class SeSInspectorAgent is in charge of going through Services
      table, and pass Service and Status to the PEP
  """

#############################################################################

  def initialize( self ):
    """ Standard constructor
    """

    try:
      self.rsDB = ResourceStatusDB()
      self.rmDB = ResourceManagementDB()

      self.ServicesToBeChecked = Queue.Queue()
      self.ServiceNamesInCheck = []

      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
      self.threadPool         = ThreadPool( self.maxNumberOfThreads,
                                            self.maxNumberOfThreads )

      if not self.threadPool:
        self.log.error( 'Can not create Thread Pool' )
        return S_ERROR( 'Can not create Thread Pool' )

      self.setup         = getSetup()['Value']
      self.VOExtension   = getExt()
      self.ServicesFreqs = CheckingFreqs[ 'ServicesFreqs' ]
      self.nc            = NotificationClient()
      self.diracAdmin    = DiracAdmin()
      self.csAPI         = CSAPI()

      for _i in xrange( self.maxNumberOfThreads ):
        self.threadPool.generateJobAndQueueIt( self._executeCheck, args = ( None, ) )

      return S_OK()

    except Exception:
      errorStr = "SeSInspectorAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )


#############################################################################

  def execute( self ):
    """
    The main SSInspectorAgent execution method.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getResourcesToCheck` and
    put result in self.ServicesToBeChecked (a Queue) and in self.ServiceNamesInCheck (a list)
    """

    try:

      res = self.rsDB.getStuffToCheck( 'Services', self.ServicesFreqs )

      for resourceTuple in res:
        if resourceTuple[ 0 ] in self.ServiceNamesInCheck:
          break
        resourceL = [ 'Service' ]
        for x in resourceTuple:
          resourceL.append( x )
        self.ServiceNamesInCheck.insert( 0, resourceL[ 1 ] )
        self.ServicesToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      gLogger.exception( errorStr, lException = x )
      return S_ERROR( errorStr )

#############################################################################

  def _executeCheck( self, toBeChecked ):
    """
    Create instance of a PEP, instantiated popping a service from lists.
    """

    while True:

      try:

        toBeChecked  = self.ServicesToBeChecked.get()

        granularity  = toBeChecked[ 0 ]
        serviceName  = toBeChecked[ 1 ]
        status       = toBeChecked[ 2 ]
        formerStatus = toBeChecked[ 3 ]
        siteType     = toBeChecked[ 4 ]
        serviceType  = toBeChecked[ 5 ]
        tokenOwner   = toBeChecked[ 6 ]

        gLogger.info( "Checking Service %s, with status %s" % ( serviceName, status ) )

        newPEP = PEP( self.VOExtension, granularity = granularity, name = serviceName, status = status,
                      formerStatus = formerStatus, siteType = siteType,
                      serviceType = serviceType, tokenOwner = tokenOwner )

        newPEP.enforce( rsDBIn = self.rsDB, rmDBIn = self.rmDB, setupIn = self.setup, ncIn = self.nc,
                        daIn = self.diracAdmin, csAPIIn = self.csAPI )

        # remove from InCheck list
        self.ServiceNamesInCheck.remove( toBeChecked[ 1 ] )

      except Exception:
        gLogger.exception( 'SeSInspector._executeCheck' )
        try:
          self.ServiceNamesInCheck.remove( serviceName )
        except IndexError:
          pass

#############################################################################
