########################################################################
# File: FTSSite.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/15 12:33:08
########################################################################
""" :mod: FTSSite
    =============

    .. module: FTSSite
    :synopsis: class representing FTS site
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing FTS site

    we need this one to know which site is a part of FTS infrastructure
"""
# #
# @file FTSSite.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/15 12:33:21
# @brief Definition of FTSSite class.

# # imports
import urlparse

########################################################################
class FTSSite( object ):
  """
  .. class:: FTSSite

  FTS infrastructure

  props:
    Name = LCG.FOO.bar
    FTSServer = FQDN for server
    MaxActiveJobs = 50
  """

  def __init__( self, name = "", ftsServer = "", maxActiveJobs = 50 ):
    """c'tor

    :param self: self reference
    :param str name: site name
    :param str ftsServer: FTS server URL
    :param int maxActiveJobs: max active jobs transferring to this site
    """

    self.Name = name
    if ftsServer:
      self.FTSServer = ftsServer
    self.MaxActiveJobs = maxActiveJobs

  def set_FTSServer( self, value ):
    """ Setter """
    if type( value ) != str:
      raise TypeError( "FTSServer has to be a string!" )
    if not urlparse.urlparse( value ).scheme:
      raise ValueError( "Wrongly formatted URI!" )
    self._FTSServer = value
  def get_FTSServer( self ):
    """ Getter """
    return self._FTSServer
  FTSServer = property( get_FTSServer, set_FTSServer )

  def set_MaxActiveJobs( self, value ):
    """ Setter """
    self._MaxActiveJobs = int( value ) if value else 50
  def get_MaxActiveJobs( self ):
    """ Getter """
    return self._MaxActiveJobs
  MaxActiveJobs = property( get_MaxActiveJobs, set_MaxActiveJobs )
