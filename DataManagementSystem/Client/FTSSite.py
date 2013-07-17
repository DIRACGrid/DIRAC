########################################################################
# $HeadURL $
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
# for properties
# pylint: disable=E0211,W0612,W0142,E1101,E0102,C0103
__RCSID__ = "$Id $"
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
  MAX_ACTIVE_JOBS = 50

  def __init__( self, name = None, ftsServer = None, maxActiveJobs = None ):
    """c'tor

    :param self: self reference
    :param str name: site name
    :param str ftsServer: FTS server URL
    :param int maxActiveJobs: max active jobs transferring to this site
    """
    self.__name = ""
    self.__ftsServer = ""
    self.__maxActiveJobs = self.MAX_ACTIVE_JOBS
    if name:
      self.Name = name
    if ftsServer:
      self.FTSServer = ftsServer
    if maxActiveJobs:
      self.MaxActiveJobs = maxActiveJobs

  @property
  def Name( self ):
    """ Name getter """
    return self.__name

  @Name.setter
  def Name( self, value ):
    """ Name setter """
    self.__name = value

  @property
  def FTSServer( self ):
    """ FTS server uri getter """
    return self.__ftsServer

  @FTSServer.setter
  def FTSServer( self, value ):
    """ server uri setter """
    if type( value ) != str:
      raise TypeError( "FTSServer has to be a string!" )
    if not urlparse.urlparse( value ).scheme:
      raise ValueError( "Wrongly formatted URI!" )
    self.__ftsServer = value

  @property
  def MaxActiveJobs( self ):
    """ max active jobs """
    return self.__maxActiveJobs

  @MaxActiveJobs.setter
  def MaxActiveJobs( self, value ):
    """ max active jobs setter """
    self.__maxActiveJobs = int( value ) if value else 50

