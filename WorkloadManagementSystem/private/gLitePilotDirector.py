########################################################################
# $HeadURL$
# File :    gLitePilotDirector.py
# Author :  Ricardo Graciani
########################################################################
"""
  gLitePilotDirector module
"""
__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.private.GridPilotDirector  import GridPilotDirector
from DIRAC import gConfig
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Core.Utilities import List

import os, time, re

from hashlib import md5

# Some default values

BROKERS = ['wms206.cern.ch']
LOGGING_SERVER = 'lb101.cern.ch'
MYPROXYSERVER = ' '


class gLitePilotDirector( GridPilotDirector ):
  """
    gLitePilotDirector class,
    It includes:
     - basic configuration for gLite
     - submit and monitor methods for gLite MiddleWare.
  """
  def __init__( self, submitPool ):
    """
     Define some defaults and call parent __init__
    """
    self.gridMiddleware = 'gLite'

    self.resourceBrokers = BROKERS
    self.myProxyServer = MYPROXYSERVER
    # FIXME: We might be able to remove this
    self.loggingServers = [ LOGGING_SERVER ]

    GridPilotDirector.__init__( self, submitPool )

  def configure( self, csSection, submitPool ):
    """
     Here goes specific configuration for gLite PilotDirectors
    """
    GridPilotDirector.configure( self, csSection, submitPool )

    self.reloadConfiguration( csSection, submitPool )

    if self.loggingServers:
      self.log.info( ' LoggingServers:', ', '.join( self.loggingServers ) )
    self.log.info( '' )
    self.log.info( '===============================================' )

  def configureFromSection( self, mySection ):
    """
      reload from CS
    """
    GridPilotDirector.configureFromSection( self, mySection )


    self.loggingServers = gConfig.getValue( mySection + '/LoggingServers'       , self.loggingServers )
    # This allows to set it to '' for LHCB and prevent CREAM CEs to reuse old proxies
    # For this to work with parametric jobs it requires the UI default configuration files to properly defined a
    #  consistent default.
    # For other VOs it allows to set a proper MyProxyServer for automatic renewal
    #  of pilot credentials for private pilots
    self.myProxyServer = gConfig.getValue( mySection + '/MyProxyServer'         , self.myProxyServer )


  def _prepareJDL( self, taskQueueDict, workingDirectory, pilotOptions,
                   pilotsToSubmit, ceMask, submitPrivatePilot, privateTQ ):
    """
      Write JDL for Pilot Submission
    """
    rbList = []
    # Select Randomly one RB from the list
    rb = List.randomize( self.resourceBrokers )[0]
    rbList.append( '"https://%s:7443/glite_wms_wmproxy_server"' % rb )

    lbList = []
    for lb in self.loggingServers:
      lbList.append( '"https://%s:9000"' % lb )
    lbList = List.randomize( lbList )

    nPilots = 1
    vo = gConfig.getValue( '/DIRAC/VirtualOrganization', '' )
    if privateTQ or vo not in ['lhcb']:
      extraReq = "True"
    else:
      if submitPrivatePilot:
        extraReq = "! AllowsGenericPilot"
      else:
        extraReq = "AllowsGenericPilot"

    myProxyServer = self.myProxyServer.strip()
    if not myProxyServer:
      #Random string to avoid caching
      myProxyServer = "%s.cern.ch" % md5( str( time.time() ) ).hexdigest()[:10]

    wmsClientJDL = """
RetryCount = 0;
ShallowRetryCount = 0;
AllowsGenericPilot = Member( "VO-lhcb-pilot" , other.GlueHostApplicationSoftwareRunTimeEnvironment );
Requirements = pilotRequirements && %s;
MyProxyServer = "%s";
WmsClient = [
  ErrorStorage = "%s/pilotError";
  OutputStorage = "%s/pilotOutput";
# ListenerPort = 44000;
  ListenerStorage = "%s/Storage";
  RetryCount = 0;
  ShallowRetryCount = 0;
  WMProxyEndPoints = { %s };
  LBEndPoints = { %s };
  EnableServiceDiscovery = false;
  MyProxyServer = "%s";
  JdlDefaultAttributes =  [
    requirements  =  ( other.GlueCEStateStatus == "Production" || other.GlueCEStateStatus == "Special" );
    AllowZippedISB  =  true;
    SignificantAttributes  =  {"Requirements", "Rank", "FuzzyRank"};
    PerusalFileEnable  =  false;
  ];
];
""" % ( extraReq, myProxyServer,
        workingDirectory, workingDirectory,
        workingDirectory, ', '.join( rbList ),
        ', '.join( lbList ), myProxyServer )

    if pilotsToSubmit > 1:
      wmsClientJDL += """
JobType = "Parametric";
Parameters= %s;
ParameterStep =1;
ParameterStart = 0;
""" % pilotsToSubmit
      nPilots = pilotsToSubmit


    ( pilotJDL , pilotRequirements ) = self._JobJDL( taskQueueDict, pilotOptions, ceMask )

    jdl = os.path.join( workingDirectory, '%s.jdl' % taskQueueDict['TaskQueueID'] )
    jdl = self._writeJDL( jdl, [pilotJDL, wmsClientJDL] )

    return {'JDL':jdl, 'Requirements':pilotRequirements + " && " + extraReq, 'Pilots':nPilots, 'RB':rb }

  def _listMatch( self, proxy, jdl, taskQueueID, rb ):
    """
     Check the number of available queues for the pilots to prevent submission
     if there are no matching resources.
    """
    cmd = [ 'glite-wms-job-list-match', '-a', '-c', '%s' % jdl, '%s' % jdl ]
    return self.parseListMatchStdout( proxy, cmd, taskQueueID, rb )

  def _submitPilot( self, proxy, pilotsToSubmit, jdl, taskQueueID, rb ):
    """
     Submit pilot and get back the reference
    """
    result = []
    cmd = [ 'glite-wms-job-submit', '-a', '-c', '%s' % jdl, '%s' % jdl ]
    ret = self.parseJobSubmitStdout( proxy, cmd, taskQueueID, rb )
    if ret:
      result.append( ret )

    return result

  def _getChildrenReferences( self, proxy, parentReference, taskQueueID ):
    """
     Get reference for all Children
    """
    cmd = [ 'glite-wms-job-status', parentReference ]

    start = time.time()
    self.log.verbose( 'Executing Job Status for TaskQueue', taskQueueID )

    ret = executeGridCommand( proxy, cmd, self.gridEnv )

    if not ret['OK']:
      self.log.error( 'Failed to execute Job Status', ret['Message'] )
      return []
    if ret['Value'][0] != 0:
      self.log.error( 'Error executing Job Status:', str( ret['Value'][0] ) + '\n'.join( ret['Value'][1:3] ) )
      return []
    self.log.info( 'Job Status Execution Time: %.2f' % ( time.time() - start ) )

    stdout = ret['Value'][1]
    # stderr = ret['Value'][2]

    references = []

    failed = 1
    for line in List.fromChar( stdout, '\n' ):
      match = re.search( "Status info for the Job : (https:\S+)", line )
      if ( match ):
        glite_id = match.group( 1 )
        if glite_id not in references and glite_id != parentReference:
          references.append( glite_id )
        failed = 0
    if failed:
      error = str( ret['Value'][0] ) + '\n'.join( ret['Value'][1:3] )
      self.log.error( 'Job Status returns no Child Reference:', error )
      return [parentReference]

    return references

