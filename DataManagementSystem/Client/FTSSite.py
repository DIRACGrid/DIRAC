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
# pylint: disable=E0211,W0612,W0142,E1101,E0102
__RCSID__ = "$Id $"
# #
# @file FTSSite.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/15 12:33:21
# @brief Definition of FTSSite class.

# # imports
import urlparse
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.Record import Record

########################################################################
class FTSSite( Record ):
  """
  .. class:: FTSSite

  site with FTS infrastructure
  """
  MAX_ACTIVE_JOBS = 50

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    Record.__init__( self )
    self.__data__["MaxActiveJobs"] = self.MAX_ACTIVE_JOBS
    fromDict = fromDict if fromDict else {}
    for attrName, attrValue in fromDict.items():
      if attrName not in self.__data__:
        raise AttributeError( "unknown FTSSite attribute %s" % str( attrName ) )
      setattr( self, attrName, attrValue )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "FTSSiteID": "INTEGER NOT NULL AUTO_INCREMENT",
               "Name": "VARCHAR(255) NOT NULL",
               "FTSServer":  "VARCHAR(255)",
               "MaxActiveJobs": "INTEGER NOT NULL DEFAULT 50" },
             "PrimaryKey": [ "FTSSiteID" ] }

  @property
  def FTSSiteID( self ):
    """ FTSSiteID getter """
    return self.__data__["FTSSiteID"]

  @FTSSiteID.setter
  def FTSSiteID( self, value ):
    """ FTSSiteID setter """
    self.__data__["FTSSiteID"] = value

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

  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) == str else str( value ) )
                for column, value in self.__data__.items()
                if value and column != "FTSSiteID" ]
    query = []
    if self.FTSSiteID:
      query.append( "UPDATE `FTSSite` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSSiteID`=%d;\n" % self.FTSSiteID )
    else:
      query.append( "INSERT INTO `FTSSite` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return S_OK( "".join( query ) )

  def toJSON( self ):
    """ dump FTSFile to JSON format """
    return S_OK( dict( zip( self.__data__.keys(),
                      [ str( val ) if val else "" for val in self.__data__.values() ] ) ) )
