########################################################################
# $HeadURL $
# File: CleanReqDBAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/17 08:31:26
########################################################################
""" :mod: CleanReqDBAgent
    =======================

    .. module: CleanReqDBAgent
    :synopsis: cleaning RequestDB from obsolete records and kicking assigned requests
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    cleaning RequestDB from obsolete records and kicking assigned requests
"""
__RCSID__ = "$Id: $"
# #
# @file CleanReqDBAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/17 08:32:08
# @brief Definition of CleanReqDBAgent class.

# # imports
import datetime
# # from DIRAC
from DIRAC import S_OK, gMonitor
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Request import Request

AGENT_NAME = "RequestManagement/CleanReqDBAgent"

########################################################################
class CleanReqDBAgent( AgentModule ):
  """
  .. class:: CleanReqDBAgent

  """
  # # DEL GRACE PERIOD in DAYS
  DEL_GRACE_DAYS = 180
  # # DEL LIMIT
  DEL_LIMIT = 100
  # # KICK PERIOD in HOURS
  KICK_GRACE_HOURS = 1
  # # KICK LIMIT
  KICK_LIMIT = 100

  # # request client
  __requestClient = None

  def requestClient( self ):
    """ request client getter """
    if not self.__requestClient:
      self.__requestClient = ReqClient()
    return self.__requestClient

  def initialize( self ):
    """ initialization """
    self.DEL_GRACE_DAYS = self.am_getOption( "DeleteGraceDays", self.DEL_GRACE_DAYS )
    self.log.info( "Delete grace period = %s days" % self.DEL_GRACE_DAYS )
    self.DEL_LIMIT = self.am_getOption( "DeleleLimit", self.DEL_LIMIT )
    self.log.info( "Delete limit = %s request/cycle" % self.DEL_LIMIT )

    self.KICK_GRACE_HOURS = self.am_getOption( "KickGraceHours", self.KICK_GRACE_HOURS )
    self.log.info( "Kick assigned requests period = %s hours" % self.KICK_GRACE_HOURS )
    self.KICK_LIMIT = self.am_getOption( "KickLimit", self.KICK_LIMIT )
    self.log.info( "Kick limit = %s request/cycle" % self.KICK_LIMIT )

    gMonitor.registerActivity( "DeletedRequests", "Deleted finished requests",
                               "CleanReqDBAgent", "Requests/min", gMonitor.OP_SUM )

    gMonitor.registerActivity( "KickedRequests", "Assigned requests kicked",
                               "CleanReqDBAgent", "Requests/min", gMonitor.OP_SUM )
    return S_OK()

  def execute( self ):
    """ execution in one cycle """

    now = datetime.datetime.now()
    kickTime = now - datetime.timedelta( hours = self.KICK_GRACE_HOURS )
    rmTime = now - datetime.timedelta( days = self.DEL_GRACE_DAYS )

    kicked = 0
    deleted = 0

    # # TODO: add selection here

    gMonitor.addMark( "KickedRequests", kicked )
    gMonitor.addMark( "DeletedRequests", deleted )
    self.log.info( "execute: kicked assigned requests = %s" % kicked )
    self.log.info( "execute: deleted finished requests = %s" % deleted )
    return S_OK()
