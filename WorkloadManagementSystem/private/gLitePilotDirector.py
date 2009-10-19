########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/private/gLitePilotDirector.py,v 1.4 2009/10/19 10:06:57 rgracian Exp $
# File :   gLitePilotDirector.py
# Author : Ricardo Graciani
########################################################################
"""
  gLitePilotDirector class,
  It includes:
   - basic configuration for gLite
   - submit and monitor methods for gLite MiddleWare.
"""
__RCSID__ = "$Id: gLitePilotDirector.py,v 1.4 2009/10/19 10:06:57 rgracian Exp $"

from DIRAC.WorkloadManagementSystem.private.GridPilotDirector  import GridPilotDirector
from DIRAC import S_OK, S_ERROR, gConfig, List

import os, time, re

# Some default values

BROKERS  = ['wms206.cern.ch']
LOGGING_SERVER   = 'lb101.cern.ch'


class gLitePilotDirector(GridPilotDirector):
  def __init__(self, submitPool):
    """
     Define some defaults and call parent __init__
    """
    self.gridMiddleware     = 'gLite'

    self.resourceBrokers    = BROKERS
    # FIXME: We might be able to remove this
    self.loggingServers     = [ LOGGING_SERVER ]

    GridPilotDirector.__init__( self, submitPool )

  def configure(self, csSection, submitPool):
    """
     Here goes especific configuration for gLite PilotDirectors
    """
    GridPilotDirector.configure( self, csSection, submitPool )

    self.reloadConfiguration( csSection, submitPool )

    if self.loggingServers:
      self.log.info( ' LoggingServers:', ', '.join(self.loggingServers) )
    self.log.info( '' )
    self.log.info( '===============================================' )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    GridPilotDirector.configureFromSection( self, mySection )


    self.loggingServers       = gConfig.getValue( mySection+'/LoggingServers'       , self.loggingServers )


  def _prepareJDL(self, taskQueueDict, workingDirectory, pilotOptions, pilotsToSubmit, ceMask, submitPrivatePilot, privateTQ ):
    """
      Write JDL for Pilot Submission
    """
    RBs = []
    # Select Randomly one RB from the list
    RB = List.randomize( self.resourceBrokers )[0]
    RBs.append( '"https://%s:7443/glite_wms_wmproxy_server"' % RB )

    LBs = []
    for LB in self.loggingServers:
      LBs.append('"https://%s:9000"' % LB)
    LBs = List.randomize( LBs )

    nPilots = 1

    if privateTQ:
      extraReq = "True"
    else:
      if submitPrivatePilot:
        extraReq = "! AllowsGenericPilot"
      else:
        extraReq = "AllowsGenericPilot"

    wmsClientJDL = """

RetryCount = 0;
ShallowRetryCount = 0;
MyProxyServer = "no-myproxy.cern.ch";

AllowsGenericPilot = Member( "VO-lhcb-pilot" , other.GlueHostApplicationSoftwareRunTimeEnvironment );
Requirements = pilotRequirements && %s;
WmsClient = [
Requirements = other.GlueCEStateStatus == "Production";
ErrorStorage = "%s/pilotError";
OutputStorage = "%s/pilotOutput";
# ListenerPort = 44000;
ListenerStorage = "%s/Storage";
# VirtualOrganisation = "lhcb";
RetryCount = 0;
ShallowRetryCount = 0;
WMProxyEndPoints = { %s };
LBEndPoints = { %s };
MyProxyServer = "no-myproxy.cern.ch";
EnableServiceDiscovery = false;
];
""" % ( extraReq, workingDirectory, workingDirectory, workingDirectory, ', '.join(RBs), ', '.join(LBs) )

    if pilotsToSubmit > 1:
      wmsClientJDL += """
JobType = "Parametric";
Parameters= %s;
ParameterStep =1;
ParameterStart = 0;
""" % pilotsToSubmit
      nPilots = pilotsToSubmit


    (pilotJDL , pilotRequirements) = self._JobJDL( taskQueueDict, pilotOptions, ceMask )

    jdl = os.path.join( workingDirectory, '%s.jdl' % taskQueueDict['TaskQueueID'] )
    jdl = self._writeJDL( jdl, [pilotJDL, wmsClientJDL] )

    return {'JDL':jdl, 'Requirements':pilotRequirements + " && " + extraReq, 'Pilots':nPilots, 'RB':RB }

  def _listMatch(self, proxy, jdl, taskQueueID, rb):
    """
     Check the number of available queues for the pilots to prevent submission
     if there are no matching resources.
    """
    cmd = [ 'glite-wms-job-list-match', '-a', '-c', '%s' % jdl, '%s' % jdl ]
    return self.parseListMatchStdout( proxy, cmd, taskQueueID, rb )

  def _submitPilot(self, proxy, pilotsToSubmit, jdl, taskQueueID, rb):
    """
     Submit pilot and get back the reference
    """
    result = []
    cmd = [ 'glite-wms-job-submit', '-a', '-c', '%s' % jdl, '%s' % jdl ]
    ret = self.parseJobSubmitStdout( proxy, cmd, taskQueueID, rb )
    if ret:
      result.append(ret)

    return result

  def _getChildrenReferences(self, proxy, parentReference, taskQueueID ):
    """
     Get reference for all Children
    """
    cmd = [ 'glite-wms-job-status', parentReference ]

    start = time.time()
    self.log.verbose( 'Executing Job Status for TaskQueue', taskQueueID )

    ret = self._gridCommand( proxy, cmd )

    if not ret['OK']:
      self.log.error( 'Failed to execute Job Status', ret['Message'] )
      return False
    if ret['Value'][0] != 0:
      self.log.error( 'Error executing Job Status:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return False
    self.log.info( 'Job Status Execution Time: %.2f' % (time.time()-start) )

    stdout = ret['Value'][1]
    stderr = ret['Value'][2]

    references = []

    failed = 1
    for line in List.fromChar(stdout,'\n'):
      m = re.search("Status info for the Job : (https:\S+)",line)
      if (m):
        glite_id = m.group(1)
        if glite_id not in references and glite_id != parentReference:
          references.append( glite_id )
        failed = 0
    if failed:
      self.log.error( 'Job Status returns no Child Reference:', str(ret['Value'][0]) + '\n'.join( ret['Value'][1:3] ) )
      return [parentReference]

    return references


