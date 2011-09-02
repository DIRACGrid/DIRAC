########################################################################
# $HeadURL:  $
########################################################################

import Queue
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

from DIRAC.ResourceStatusSystem.Utilities.CS import getSetup, getExt
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

from DIRAC.ResourceStatusSystem.Policy.Configurations import CheckingFreqs
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB


__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/RSInspectorAgent'

class RSInspectorAgent( AgentModule ):
  """ Class RSInspectorAgent is in charge of going through Resources
      table, and pass Resource and Status to the PEP
  """

#############################################################################

  def initialize( self ):
    """ Standard constructor
    """

    try:
      self.rsDB = ResourceStatusDB()
      self.rmDB = ResourceManagementDB()

      self.ResourcesToBeChecked = Queue.Queue()
      self.ResourceNamesInCheck = []

      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
      self.threadPool         = ThreadPool( self.maxNumberOfThreads,
                                            self.maxNumberOfThreads )

      if not self.threadPool:
        self.log.error( 'Can not create Thread Pool' )
        return S_ERROR( 'Can not create Thread Pool' )

      self.setup          = getSetup()[ 'Value' ]
      self.VOExtension    = getExt()
      self.ResourcesFreqs = CheckingFreqs[ 'ResourcesFreqs' ]
      self.nc             = NotificationClient()
      self.diracAdmin     = DiracAdmin()
      self.csAPI          = CSAPI()

      for _i in xrange( self.maxNumberOfThreads ):
        self.threadPool.generateJobAndQueueIt( self._executeCheck )

      return S_OK()

    except Exception:
      errorStr = "RSInspectorAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  def execute( self ):
    """
    The main RSInspectorAgent execution method.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getStuffToCheck` and
    put result in self.ResourcesToBeChecked (a Queue) and in self.ResourceNamesInCheck (a list)
    """

    try:

      res = self.rsDB.getStuffToCheck( 'Resources', self.ResourcesFreqs )

      for resourceTuple in res:
        if resourceTuple[ 0 ] in self.ResourceNamesInCheck:
          break
        resourceL = [ 'Resource' ]
        for x in resourceTuple:
          resourceL.append( x )
        self.ResourceNamesInCheck.insert( 0, resourceL[ 1 ] )
        self.ResourcesToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      gLogger.exception( errorStr,lException=x )
      return S_ERROR( errorStr )


#############################################################################

  def _executeCheck( self ):
    """
    Create instance of a PEP, instantiated popping a resource from lists.
    """


    while True:

      try:

        toBeChecked  = self.ResourcesToBeChecked.get()

        granularity  = toBeChecked[ 0 ]
        resourceName = toBeChecked[ 1 ]
        status       = toBeChecked[ 2 ]
        formerStatus = toBeChecked[ 3 ]
        siteType     = toBeChecked[ 4 ]
        resourceType = toBeChecked[ 5 ]
        tokenOwner   = toBeChecked[ 6 ]

        # Ignore all elements with token != RS_SVC
        if tokenOwner != 'RS_SVC':
          continue

        gLogger.info( "Checking Resource %s, with status %s" % ( resourceName, status ) )

        newPEP = PEP( self.VOExtension, granularity = granularity, name = resourceName,
                      status = status, formerStatus = formerStatus, siteType = siteType,
                      resourceType = resourceType, tokenOwner = tokenOwner )

        newPEP.enforce( rsDBIn = self.rsDB, rmDBIn = self.rmDB, setupIn = self.setup,
                        ncIn = self.nc, daIn = self.diracAdmin, csAPIIn = self.csAPI )

        # remove from InCheck list
        self.ResourceNamesInCheck.remove( toBeChecked[ 1 ] )

      except Exception:
        gLogger.exception( 'RSInspector._executeCheck' )
        try:
          self.ResourceNamesInCheck.remove( resourceName )
        except IndexError:
          pass

#############################################################################
