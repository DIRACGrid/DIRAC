########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/private/DIRACPilotDirector.py,v 1.8 2009/07/07 07:28:52 rgracian Exp $
# File :   DIRACPilotDirector.py
# Author : Ricardo Graciani
########################################################################
"""
  Dirac PilotDirector class, it uses DIRAC CE backends to submit and monitor pilots.
  It includes:
   - basic configuration for Dirac PilotDirector

  A DIRAC PilotDirector make use directly to CE methods to place the pilots on the
  underlying resources.


"""
__RCSID__ = "$Id: DIRACPilotDirector.py,v 1.8 2009/07/07 07:28:52 rgracian Exp $"

import os, sys, tempfile, shutil

from DIRAC.WorkloadManagementSystem.private.PilotDirector import PilotDirector
from DIRAC.Resources.Computing.ComputingElementFactory    import ComputingElementFactory
from DIRAC.Core.Security import CS
from DIRAC.FrameworkSystem.Client.ProxyManagerClient      import gProxyManager
from DIRAC import S_OK, S_ERROR, DictCache, gConfig

ERROR_CE         = 'No CE available'
ERROR_JDL        = 'Could not create Pilot script'
ERROR_SCRIPT     = 'Could not copy Pilot script'

COMPUTING_ELEMENTS = ['InProcess']

class DIRACPilotDirector(PilotDirector):
  """
    DIRAC PilotDirector class
  """
  def __init__( self, submitPool ):
    """
     Define some defaults and call parent __init__
    """
    self.gridMiddleware    = 'DIRAC'

    self.computingElements = COMPUTING_ELEMENTS
    self.siteName          = gConfig.getValue('/LocalSite/Site','')
    if not self.siteName:
      self.log.error( 'Can not run a Director if Site Name is not defined' )
      sys.exit()

    self.__failingCECache  = DictCache()
    self.__ticketsCECache  = DictCache()

    PilotDirector.__init__( self, submitPool )

  def configure(self, csSection, submitPool ):
    """
     Here goes common configuration for DIRAC PilotDirector
    """

    PilotDirector.configure( self, csSection, submitPool )
    self.reloadConfiguration( csSection, submitPool )

    self.__failingCECache.purgeExpired()
    self.__ticketsCECache.purgeExpired()

    for ce in self.__failingCECache.getKeys():
      if ce in self.computingElements:
        try:
          self.computingElements.remove( ce )
        except:
          pass
    if self.computingElements:
      self.log.info( ' ComputingElements:', ', '.join(self.computingElements) )

    if self.siteName:
      self.log.info( ' SiteName:', self.siteName )


  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    PilotDirector.configureFromSection( self, mySection )

    self.computingElements    = gConfig.getValue( mySection+'/ComputingElements'      , self.computingElements )
    self.siteName             = gConfig.getValue( mySection+'/SiteName'               , self.siteName )


  def _submitPilots( self, workDir, taskQueueDict, pilotOptions, pilotsToSubmit,
                     ceMask, submitPrivatePilot, privateTQ, proxy ):
    """
      This method does the actual pilot submission to the DIRAC CE
      The logic is as follows:
      - If there are no available CE it return error
      - It creates a temp directory
      - Prepare a PilotScript
    """
    taskQueueID = taskQueueDict['TaskQueueID']
    ownerDN = taskQueueDict['OwnerDN']

    if not self.computingElements:
      # Since we can exclude CEs from the list, it may become empty
      return S_ERROR( ERROR_CE )

    baseDir = os.getcwd()
    workingDirectory = tempfile.mkdtemp( prefix= 'TQ_%s_' % taskQueueID, dir = workDir )
    self.log.verbose( 'Using working Directory:', workingDirectory )
    os.chdir( workingDirectory )

    # set the Site Name
    if self.siteName:
      pilotOptions.append( "-n '%s'" % self.siteName)

    try:
      pilotScript = self._writePilotScript( workingDirectory, pilotOptions )
      shutil.copy( self.pilot, os.path.join( workingDirectory, os.path.basename(self.pilot) ) )
      shutil.copy( self.install, os.path.join( workingDirectory, os.path.basename(self.install) ) )
    except:
      self.log.exception( ERROR_SCRIPT )
      try:
        os.chdir( baseDir )
        shutil.rmtree( workingDirectory )
      except:
        pass
      return S_ERROR( ERROR_SCRIPT )

    # FIXME: this is to start testing
    ceFactory = ComputingElementFactory("InProcess")
    ceName = "InProcess"
    ceInstance = ceFactory.getCE()
    if not ceInstance['OK']:
      self.log.warn(ceInstance['Message'])
      try:
        os.chdir( baseDir )
        shutil.rmtree( workingDirectory )
      except:
        pass
      return ceInstance

    computingElement = ceInstance['Value']
    submission = computingElement.submitJob(pilotScript,'',proxy.dumpAllToString()['Value'],'')
    try:
      os.chdir( baseDir )
      shutil.rmtree( workingDirectory )
    except:
      pass
    return S_OK(1)

    return submission

  def _writePilotScript( self, workingDirectory, pilotOptions ):
    """
     Prepare the script to execute the pilot
     For the moment it will do like Grid Pilots, a full DIRAC installation
    """

    pilot = os.path.basename( self.pilot )
    install = os.path.basename(self.install)
    localPilot = """#!/usr/bin/env python
#
import os, tempfile, sys, shutil
try:
  pilotWorkingDirectory = tempfile.mkdtemp( suffix = 'pilot', prefix= 'DIRAC_' )
  shutil.move( "%s",  pilotWorkingDirectory )
  shutil.move( "%s", pilotWorkingDirectory )
  os.chdir( pilotWorkingDirectory )
except Exception, x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "python %s %s"
print 'Executing:', cmd
sys.stdout.flush()
os.system( cmd )

shutil.rmtree( pilotWorkingDirectory )

""" % ( pilot, install, pilot, ' '.join( pilotOptions ) )

    pilotScript = os.path.join( workingDirectory, 'local-pilot' )
    fd = open( pilotScript, 'w' )
    fd.write( localPilot )
    fd.close()

    return pilotScript

  def _getPilotProxyFromDIRACGroup( self, ownerDN, ownerGroup, requiredTimeLeft ):
    """
    Download a limited pilot proxy with VOMS extensions depending on the group
    """
    #Assign VOMS attribute
    vomsAttr = CS.getVOMSAttributeForGroup( ownerGroup )
    if not vomsAttr:
      return S_ERROR( "No voms attribute assigned to group %s" % ownerGroup )
    return self.downloadVOMSProxy( ownerDN,
                                   ownerGroup,
                                   limited = True,
                                   requiredTimeLeft = requiredTimeLeft,
                                   requiredVOMSAttribute = vomsAttr )    
