""" :mod: RestartReqExeAgent
    =====================

    .. module: RestartReqExeAgent
    :synopsis: restart the RequestExecutingAgent in case it gets stuck

"""
__RCSID__ = "$Id$"

# # imports
import datetime
import os
import signal

# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Subprocess import systemCall

AGENT_NAME = "RequestManagement/RestartReqExeAgent"

#Define units
MINUTES = 60
SECONDS = 1

########################################################################
class RestartReqExeAgent( AgentModule ):
  """
  .. class:: RestartReqExeAgent

  """

  def initialize( self ):
    """ initialization """

    self.maxLogAge = self.am_getOption( "MaxLogAge", 60*MINUTES )
    self.agentNames = self.am_getOption( "AgentNames", ['RequestExecutingAgent'] )
    self.enabled = self.am_getOption( "Enabled" )
    return S_OK()

  def execute( self ):
    """ execution in one cycle """
    ok = True
    for agentName in self.agentNames:
      res = self._checkAgent( agentName )
      if not res['OK']:
        self.log.error( "Failure when checking agent", "%s, %s" %( agentName, res['Message'] ) )
        ok = False

    if not ok:
      return S_ERROR( "Error during this cycle, check log" )
    return S_OK()

  def _checkAgent( self, agentName ):
    """Check if the given agent is still running
    we are assuming this is an agent in the RequestManagementSystem
    """
    diracLocation = os.environ.get( "DIRAC", "/opt/dirac/pro" )
    currentLogLocation = os.path.join( diracLocation, 'runit', 'RequestManagement', agentName, 'log', 'current' )
    self.log.verbose( "Current Log File location: %s " % currentLogLocation )

    ## get the age of the current log file
    lastAccessTime = 0
    try:
      lastAccessTime = os.path.getmtime( currentLogLocation )
      lastAccessTime = datetime.datetime.fromtimestamp( lastAccessTime )
    except OSError as e:
      self.log.error( "Failed to access current log file", str(e) )
      return S_ERROR( "Failed to access current log file" )

    now = datetime.datetime.now()
    age = now - lastAccessTime

    self.log.info( "Current log file for %s is %d minutes old" % ( agentName, ( age.seconds / MINUTES ) ) )

    if age.seconds > self.maxLogAge:
      self.log.info( "Current log file is too old!" )
      res = self.__getPID( agentName )
      if not res['OK']:
        return res
      pid = res['Value']

      self.log.info( "Found PID for %s: %d" % ( agentName, pid ) )
      ## kill the agent
      if self.enabled:
        os.kill( pid, signal.SIGTERM )
        self.log.info( "Killed the %s Agent" % agentName )
      else:
        self.log.info( "Would have killed the %s Agent" % agentName )


    return S_OK()


  def __getPID( self, agentName ):
    """return PID for agentName"""

    ## Whitespaces around third argument are mandatory to only match the given agentName
    pidRes = systemCall( 10, [ 'pgrep', '-f', ' RequestManagement/%s ' % agentName ] )
    if not pidRes['OK']:
      return pidRes
    pid = pidRes['Value'][1].strip()

    ## this is checking there is only one PID returned
    try:
      pid = int( pid )
    except ValueError as e:
      self.log.error( "Could not create int from PID: ", str(e) )
      return S_ERROR( "Could not create int from PID" )

    return S_OK( pid )
