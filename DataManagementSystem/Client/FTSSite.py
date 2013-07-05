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
# # from DIRAC
from DIRAC import S_OK

########################################################################
class FTSSite( object ):
  """
  .. class:: FTSSite

  site with FTS infrastructure
  
  props:
  Name = LCG.FOO.bar
  FTSServer = FQDN for server
  MaxActiveJobs = 50
  """
  MAX_ACTIVE_JOBS = 50

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    self.__data__["MaxActiveJobs"] = self.MAX_ACTIVE_JOBS
    fromDict = fromDict if fromDict else {}
    for attrName, attrValue in fromDict.items():
      if attrName not in self.__data__:
        raise AttributeError( "unknown FTSSite attribute %s" % str( attrName ) )
      setattr( self, attrName, attrValue )

  @property
  def Name( self ):
    """ Name getter """
    return self.__data__["Name"]

  @Name.setter
  def Name( self, value ):
    """ Name setter """
    self.__data__["Name"] = value

  @property
  def FTSServer( self ):
    """ FTS server uri getter """
    return self.__data__["FTSServer"]

  @FTSServer.setter
  def FTSServer( self, value ):
    """ server uri setter """
    if type( value ) != str:
      raise TypeError( "FTSServer has to be a string!" )
    if not urlparse.urlparse( value ).scheme:
      raise ValueError( "Wrongly formatted URI!" )
    self.__data__["FTSServer"] = value

  @property
  def MaxActiveJobs( self ):
    """ max active jobs """
    return self.__data__["MaxActiveJobs"]

  @MaxActiveJobs.setter
  def MaxActiveJobs( self, value ):
    """ max active jobs setter """
    self.__data__["MaxActiveJobs"] = int( value ) if value else 50

