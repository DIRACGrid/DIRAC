########################################################################
# $HeadURL $
# File: FTSHistoryView.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/19 12:26:30
########################################################################

""" :mod: FTSHistoryView
    ====================

    .. module: FTSHistoryView
    :synopsis: last hour history of FTS transfers as read from FTSHistoryView
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    last hour history of FTS transfers as read from FTSHistoryView
"""
# for properties
# pylint: disable=E0211,W0612,W0142,E1101,E0102
__RCSID__ = "$Id $"

# #
# @file FTSHistoryView.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/19 12:26:55
# @brief Definition of FTSHistoryView class.

# # imports
import urlparse
# # from DIRAC
from DIRAC import S_OK

########################################################################
class FTSHistoryView( object ):
  """
  .. class:: FTSHistoryView

  helper class for FTSManagerHandler to keep 1h history of FTS transfers
  """
  INTERVAL = 3600

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    """
    fromDict = fromDict if fromDict else {}
    self.__data__ = dict.fromkeys( self.viewDesc()["Fields"].keys(), None )
    self.__data__["Files"] = 0
    self.__data__["Size"] = 0
    self.__data__["FailedFiles"] = 0
    self.__data__["FailedSize"] = 0
    self.__data__["FTSJobs"] = 0
    self.__data__["Completeness"] = 0.0
    for key, value in fromDict.items():
      if key not in self.__data__:
        raise AttributeError( "Unknown FTSHistoryView attribute '%s'" % key )
      if value:
        setattr( self, key, value )

  @staticmethod
  def viewDesc():
    """ view description """
    return { "Fields": {  "SourceSE": "`FTSJob`.`SourceSE`",
                          "TargetSE": "`FTSJob`.`TargetSE`",
                          "FTSJobs": "COUNT(DISTINCT `FTSJob`.`FTSJobID`)",
                          "FTSServer": "`FTSJob`.`FTSServer`",
                          "Status": "`FTSJob`.`Status`",
                          "Files": "SUM(`FTSJob`.`Files`)",
                          "Size": "SUM(`FTSJob`.`Size`)",
                          "FailedFiles": "SUM(`FTSJob`.`FailedFiles`)",
                          "FailedSize": "SUM(`FTSJob`.`FailedSize`)",
                          "Completeness": "SUM(`FTSJob`.`Completeness`)/COUNT(DISTINCT `FTSJob`.`FTSJobID`)" },
             "SelectFrom" : "`FTSJob`",
             "Clauses": [ "`FTSJob`.`LastUpdate` > ( UTC_TIMESTAMP() - INTERVAL %s SECOND )" % FTSHistoryView.INTERVAL ],
             "GroupBy": [ "`SourceSE`", "`TargetSE`", "`Status`" ] }

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      print name, value, error

  @property
  def SourceSE( self ):
    """ source se getter """
    return self.__data__["SourceSE"]

  @SourceSE.setter
  def SourceSE( self, sourceSE ):
    """ source se setter """
    self.__data__["SourceSE"] = sourceSE

  @property
  def TargetSE( self ):
    """ target se getter """
    return self.__data__["TargetSE"]

  @TargetSE.setter
  def TargetSE( self, targetSE ):
    """ target se setter """
    self.__data__["TargetSE"] = targetSE

  @property
  def FTSJobs( self ):
    """ FTSJob count getter """
    return self.__data__["FTSJobs"]

  @FTSJobs.setter
  def FTSJobs( self, ftsJobs ):
    """ FTSJob count setter """
    self.__data__["FTSJobs"] = long( ftsJobs ) if ftsJobs else 0

  @property
  def FTSServer( self ):
    """ FTS server getter """
    return self.__data__["FTSServer"]

  @FTSServer.setter
  def FTSServer( self, ftsServer ):
    """ FTS server setter """
    if type( ftsServer ) != str:
      raise TypeError( "FTSServer has to be a string!" )
    if not urlparse.urlparse( ftsServer ).scheme:
      raise ValueError( "Wrongly formatted URI!" )
    self.__data__["FTSServer"] = ftsServer

  @property
  def Status( self ):
    """ status getter """
    return self.__data__["Status"]

  @Status.setter
  def Status( self, status ):
    """ status setter """
    self.__data__["Status"] = status

  @property
  def Files( self ):
    """ files getter """
    return self.__data__["Files"]

  @Files.setter
  def Files( self, files ):
    """ files setter """
    self.__data__["Files"] = long( files ) if files else 0

  @property
  def Size( self ):
    """ size getter """
    return self.__data__["Size"]

  @Size.setter
  def Size( self, size ):
    """ size setter """
    self.__data__["Size"] = long( size ) if size else 0

  @property
  def FailedFiles( self ):
    """ failed files getter """
    return self.__data__["FailedFiles"]

  @FailedFiles.setter
  def FailedFiles( self, failedFiles ):
    """ failed files setter """
    self.__data__["FailedFiles"] = long( failedFiles ) if failedFiles else 0

  @property
  def FailedSize( self ):
    """ failed files size getter """
    return self.__data__["FailedSize"]

  @FailedSize.setter
  def FailedSize( self, failedSize ):
    """ failed files size setter """
    self.__data__["FailedSize"] = long( failedSize ) if failedSize else 0

  @property
  def Completeness( self ):
    """ completeness getter """
    return self.__data__["Completeness"]

  @Completeness.setter
  def Completeness( self, value ):
    """ completeness setter  """
    self.__data__["Completeness"] = float( value ) if value else 0.0

  def toJSON( self ):
    """ serialize to JSON format """
    return S_OK( self.__data__ )

