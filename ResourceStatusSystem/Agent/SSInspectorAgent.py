########################################################################
# $HeadURL:  $
########################################################################

import copy
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

AGENT_NAME = 'ResourceStatus/SSInspectorAgent'

class SSInspectorAgent( AgentModule ):
  """ Class SSInspectorAgent is in charge of going through Sites
      table, and pass Site and Status to the PEP
  """

#############################################################################

  def initialize( self ):
    """ Standard constructor
    """
    
    try:
      self.rsDB = ResourceStatusDB()
      self.rmDB = ResourceManagementDB()
      
      self.SitesToBeChecked = Queue.Queue()
      self.SiteNamesInCheck = []
      
      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
      self.threadPool         = ThreadPool( self.maxNumberOfThreads,
                                            self.maxNumberOfThreads )

      if not self.threadPool:
        self.log.error( 'Can not create Thread Pool' )
        return S_ERROR( 'Can not create Thread Pool' )
      
      self.setup       = getSetup()['Value']
      self.VOExtension = getExt()
      self.SitesFreqs  = CheckingFreqs[ 'ServicesFreqs' ]     
      self.nc          = NotificationClient()
      self.diracAdmin  = DiracAdmin()
      self.csAPI        = CSAPI()      
      
      for i in xrange( self.maxNumberOfThreads ):
        self.threadPool.generateJobAndQueueIt( self._executeCheck, args = ( None, ) )  
        
      return S_OK()

    except Exception:
      errorStr = "SSInspectorAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  def execute( self ):
    """ 
    The main RSInspectorAgent execution method.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getResourcesToCheck` and 
    put result in self.SitesToBeChecked (a Queue) and in self.SiteNamesInCheck (a list)
    """
    
    try:

      res = self.rsDB.getStuffToCheck( 'Sites', self.SitesFreqs ) 
   
      for resourceTuple in res:
        if resourceTuple[ 0 ] in self.SiteNamesInCheck:
          break
        resourceL = [ 'Site' ]
        for x in resourceTuple:
          resourceL.append( x )
        self.SiteNamesInCheck.insert( 0, resourceL[ 1 ] )
        self.SitesToBeChecked.put( resourceL )

      return S_OK()

    except Exception, x:
      errorStr = where( self, self.execute )
      gLogger.exception( errorStr, lException = x )
      return S_ERROR( errorStr )
              
#############################################################################

  def _executeCheck( self, arg ):
    """ 
    Create instance of a PEP, instantiated popping a resource from lists.
    """
        
    while True:
      
      try:
      
        toBeChecked  = self.SitesToBeChecked.get()
      
      
        granularity  = toBeChecked[ 0 ]
        siteName     = toBeChecked[ 1 ]
        status       = toBeChecked[ 2 ]
        formerStatus = toBeChecked[ 3 ]
        siteType     = toBeChecked[ 4 ]
        tokenOwner   = toBeChecked[ 5 ]
        
        gLogger.info( "Checking Site %s, with status %s" % ( siteName, status ) )
        
        newPEP = PEP( self.VOExtension, granularity = granularity, name = siteName, status = status, 
                      formerStatus = formerStatus, siteType = siteType, tokenOwner = tokenOwner )
        
        newPEP.enforce( rsDBIn = self.rsDB, rmDBIn = self.rmDB, setupIn = self.setup, ncIn = self.nc, 
                        daIn = self.diracAdmin, csAPIIn = self.csAPI )
    
        # remove from InCheck list
        self.SiteNamesInCheck.remove( toBeChecked[ 1 ] )

      except Exception:
        gLogger.exception( 'SSInspector._executeCheck' )
        try:
          self.SiteNamesInCheck.remove( siteName )
        except IndexError:
          pass

#############################################################################    