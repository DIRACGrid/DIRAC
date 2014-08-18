########################################################################
# $HeadURL$
# File :   LCGPilotDirector.py
# Author : Ricardo Graciani
########################################################################
"""
  LCGPilotDirector class,
  It includes:
   - basic configuration for LCG
   - submit and monitor methods for LCG MiddleWare.
"""
__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.private.GridPilotDirector  import GridPilotDirector
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers                import getVO
from DIRAC.Core.Utilities import List


import os, time

# Some default values

BROKERS = ['rb123.cern.ch']

class LCGPilotDirector( GridPilotDirector ):
  def __init__( self, submitPool ):
    """
     Define some defaults and call parent __init__
    """
    self.gridMiddleware = 'LCG'

    GridPilotDirector.__init__( self, submitPool )

    self.resourceBrokers = BROKERS
    self.loggingServers = []

  def configure( self, csSection, submitPool ):
    """
     Here goes specific configuration for LCG PilotDirectors
    """
    GridPilotDirector.configure( csSection, submitPool )

    self.log.info( '' )
    self.log.info( '===============================================' )

  def _prepareJDL( self, taskQueueDict, workingDirectory, pilotOptions, pilotsToSubmit, ceMask, submitPrivatePilot, privateTQ ):
    """
      Write JDL for Pilot Submission
    """
    # RB = List.randomize( self.resourceBrokers )[0]
    LDs = []
    NSs = []
    LBs = []
    # Select Randomly one RB from the list
    RB = List.randomize( self.resourceBrokers )[0]
    LDs.append( '"%s:9002"' % RB )
    LBs.append( '"%s:9000"' % RB )

    for LB in self.loggingServers:
      NSs.append( '"%s:7772"' % LB )

    LD = ', '.join( LDs )
    NS = ', '.join( NSs )
    LB = ', '.join( LBs )

    vo = getVO()
    if privateTQ or vo not in ['lhcb']:
      extraReq = "True"
    else:
      if submitPrivatePilot:
        extraReq = "! AllowsGenericPilot"
      else:
        extraReq = "AllowsGenericPilot"

    rbJDL = """
AllowsGenericPilot = Member( "VO-lhcb-pilot" , other.GlueHostApplicationSoftwareRunTimeEnvironment );
Requirements = pilotRequirements && other.GlueCEStateStatus == "Production" && %s;
RetryCount = 0;
ErrorStorage = "%s/pilotError";
OutputStorage = "%s/pilotOutput";
# ListenerPort = 44000;
ListenerStorage = "%s/Storage";
VirtualOrganisation = "lhcb";
LoggingTimeout = 30;
LoggingSyncTimeout = 30;
LoggingDestination = { %s };
# Default NS logger level is set to 0 (null)
# max value is 6 (very ugly)
NSLoggerLevel = 0;
DefaultLogInfoLevel = 0;
DefaultStatusLevel = 0;
NSAddresses = { %s };
LBAddresses = { %s };
MyProxyServer = "no-myproxy.cern.ch";
""" % ( extraReq, workingDirectory, workingDirectory, workingDirectory, LD, NS, LB )

    pilotJDL, pilotRequirements = self._JobJDL( taskQueueDict, pilotOptions, ceMask )

    jdl = os.path.join( workingDirectory, '%s.jdl' % taskQueueDict['TaskQueueID'] )
    jdl = self._writeJDL( jdl, [pilotJDL, rbJDL] )

    return {'JDL':jdl, 'Requirements':pilotRequirements + " && " + extraReq, 'Pilots': pilotsToSubmit, 'RB':RB }

  def _listMatch( self, proxy, jdl, taskQueueID, rb ):
    """
     Check the number of available queues for the pilots to prevent submission
     if there are no matching resources.
    """
    cmd = ['edg-job-list-match', '-c', '%s' % jdl , '--config-vo', '%s' % jdl, '%s' % jdl]
    return self.parseListMatchStdout( proxy, cmd, taskQueueID, rb )

  def _submitPilot( self, proxy, pilotsToSubmit, jdl, taskQueueID, rb ):
    """
     Submit pilot and get back the reference
    """
    result = []
    for i in range( pilotsToSubmit ):
      cmd = [ 'edg-job-submit', '-c', '%s' % jdl, '--config-vo', '%s' % jdl, '%s' % jdl ]
      ret = self.parseJobSubmitStdout( proxy, cmd, taskQueueID, rb )
      if ret:
        result.append( ret )

    return result

