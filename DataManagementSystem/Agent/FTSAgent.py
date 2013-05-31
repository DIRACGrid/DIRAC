########################################################################
# $HeadURL $
# File: FTSAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/31 10:00:13
########################################################################
""" :mod: FTSAgent 
    ==============
 
    .. module: FTSAgent
    :synopsis: agent propagating scheduled RMS request in FTS 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    agent propagating scheduled RMS request in FTS 
"""
__RCSID__ = "$Id: $"
##
# @file FTSAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/31 10:00:51
# @brief Definition of FTSAgent class.

## imports
from DIRAC.Core.Base.AgentModule import AgentModule


# # agent base name 
AGENT_NAME = "DataManagement/FTSAgent"

########################################################################
class FTSAgent( AgentModule ):
  """
  .. class:: FTSAgent
  
  """
  # # min threads
  MIN_THREADS = 1
  # # max threads
  MAX_THREADS = 10
  # # 
  


  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    pass


  
