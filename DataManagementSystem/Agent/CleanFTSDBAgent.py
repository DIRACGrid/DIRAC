########################################################################
# $HeadURL $
# File: CleanFTSDBAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/17 07:46:34
########################################################################
""" :mod: CleanFTSDBAgent
    =====================

    .. module: CleanFTSDBAgent
    :synopsis: cleaning FTSDB from old records
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    cleaning FTSDB from old records
"""
__RCSID__ = "$Id: $"
# #
# @file CleanFTSDBAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/17 07:46:45
# @brief Definition of CleanFTSDBAgent class.

# # imports
import datetime
# # from DIRAC
from DIRAC import S_OK, gMonitor
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.Client.FTSClient import FTSClient
from DIRAC.DataManagementSystem.Client.FTSJob import FTSJob

# # agent's name
AGENT_NAME = 'DataManagement/MonitorFTSAgent'

########################################################################
class CleanFTSDBAgent( AgentModule ):
  """
  .. class:: CleanFTSDBAgent

  This agent is performing two actions:

  * deletion of obsoleted and finished FTSJobs (DEL_GRACE_DAYS)
  * kicking FTSJobs assigned for too long (KICK_ASSIGNED_HOURS)
  """
  # # FTSClient
  __ftsClient = None

  # # DELETION GRACE PERIOD IN DAYS
  DEL_GRACE_DAYS = 14
  # # DEL_LIMIT
  DEL_LIMIT = 100
  # # KICK_ASSIGNED_PERIOD IN HOURS
  KICK_ASSIGNED_HOURS = 1
  # # KICK LIMIT
  KICK_LIMIT = 100

  def ftsClient( self ):
    """ FTSClient getter """
    if not self.__ftsClient:
      self.__ftsClient = FTSClient()
    return self.__ftsClient

  def initialize( self ):
    """ agent initialization  """
    self.DEL_GRACE_DAYS = self.am_getOption( "DeleteGraceDays", self.DEL_GRACE_DAYS )
    self.log.info( "Delete grace period = %s days" % self.DEL_GRACE_DAYS )
    self.DEL_LIMIT = self.am_getOption( "DeleteLimitPerCycle", self.DEL_LIMIT )
    self.log.info( "Delete FTSJob limit = %s" % self.DEL_LIMIT )
    self.KICK_ASSIGNED_HOURS = self.am_getOption( "KickAssignedHours", self.KICK_ASSIGNED_HOURS )
    self.log.info( "Kick assigned period = %s hours" % self.KICK_ASSIGNED_HOURS )
    self.KICK_LIMIT = self.am_getOption( "KickLimitPerCycle", self.KICK_LIMIT )
    self.log.info( "Kick FTSJobs limit = %s" % self.KICK_LIMIT )

    gMonitor.registerActivity( "KickedFTSJobs", "Assigned FTSJobs kicked",
                              "CleanFTSDBAgent", "FTSJobs/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "DeletedFTSJobs", "Deleted FTSJobs",
                              "CleanFTSDBAgent", "FTSJobs/min", gMonitor.OP_SUM )


    return S_OK()

  def execute( self ):
    """ one cycle execution """

    now = datetime.datetime.now()
    kickTime = now - datetime.timedelta( hours = self.KICK_ASSIGNED_HOURS )
    rmTime = now - datetime.timedelta( days = self.DEL_GRACE_DAYS )

    kicked = 0
    deleted = 0

    # # select Assigned FTSJobs
    assignedFTSJobList = self.ftsClient().getFTSJobList( ["Assigned"], self.KICK_LIMIT )
    if not assignedFTSJobList["OK"]:
      self.log.error( "execute: %s" % assignedFTSJobList["Message"] )
      return assignedFTSJobList
    assignedFTSJobList = assignedFTSJobList["Value"]

    for ftsJob in assignedFTSJobList:
      if ftsJob.LastUpdate > kickTime:
        self.log.debug( "FTSJob %s is Assigned for too long and has to be kicked" % ftsJob.FTSGUID )
        kicked += 1
        ftsJob.Status = "Submitted"
      put = self.ftsClient().putFTSJob( ftsJob )
      if not put["OK"]:
        self.log.error( "execute: unable to put back FTSJob %s: %s" % ( ftsJob.FTSGUID, put["Message"] ) )
        return put

    finishedFTSJobList = self.ftsClient().getFTSJobList( list( FTSJob.FINALSTATES ), self.DEL_LIMIT )
    if not finishedFTSJobList["OK"]:
      self.log.error( "execute: %s" % finishedFTSJobList["Message"] )
      return finishedFTSJobList
    finishedFTSJobList = finishedFTSJobList["Value"]

    for ftsJob in finishedFTSJobList:
      if ftsJob.LastUpdate > rmTime:
        self.log.debug( "FTSJob %s is too old and has to be deleted" % ftsJob.FTSGUID )
        delJob = self.ftsClient().deleteFTSJob( ftsJob.FTSJobID )
        if not delJob["OK"]:
          self.log.error( "execute: %s" % delJob["Message"] )
          return delJob
      else:
        putJob = self.ftsClient().putFTSJob( ftsJob )
        if not putJob["OK"]:
          self.log.error( "execute: %s" % putJob["Message"] )
          return putJob

    self.log.info( "Assigned FTSJobs kicked %s Finished FTSJobs deleted %s" % ( kicked, deleted ) )
    gMonitor.addMark( "KickedFTSJobs", kicked )
    gMonitor.addMark( "DeletedFTSJobs", deleted )
    return S_OK()

