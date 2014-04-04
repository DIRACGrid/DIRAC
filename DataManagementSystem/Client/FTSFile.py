########################################################################
# File: FTSFile.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/04/08 09:28:29
########################################################################
""" :mod: FTSFile
    =============

    .. module: FTSFile
    :synopsis: class representing a single file in the FTS job
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    class representing a single file in the FTS job
"""
# for properties
__RCSID__ = "$Id $"
# #
# @file FTSFile.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/04/08 09:28:45
# @brief Definition of FTSFile class.

# # imports
import os
import re
import datetime
# # from DIRAC
from DIRAC import S_OK
from DIRAC.RequestManagementSystem.private.Record import Record

########################################################################
class FTSFile( Record ):
  """
  .. class:: FTSFile

  class representing a single file in the FTS job
  """
  # # all FTS states
  ALL_STATES = ( "Submitted", "Ready", "Active", "Failed", "Finished", "Staging", "Canceled" )
  # # final FTS states
  FINAL_STATES = ( "Canceled" "Failed", "Finished" )
  # # successful states
  SUCCESS_STATES = ( "Finished" )
  # # failed states
  FAILED_STATES = ( "Canceled", "Failed" )

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: data dict
    """
    _parent = None
    Record.__init__( self )
    self._parent = None
    self.__data__["Status"] = 'Waiting'
    self.__data__["Attempt"] = 0
    self.__data__["ChecksumType"] = 'ADLER32'
    now = datetime.datetime.utcnow().replace( microsecond = 0 )
    self.__data__["CreationTime"] = now
    self.__data__["LastUpdate"] = now
    fromDict = fromDict if fromDict else {}
    for attrName, attrValue in fromDict.items():
      if attrName not in self.__data__:
        raise AttributeError( "unknown FTSFile attribute %s" % str( attrName ) )
      setattr( self, attrName, attrValue )

  @staticmethod
  def tableDesc():
    """ get table description """
    return { "Fields" :
             { "FTSFileID": "INTEGER NOT NULL AUTO_INCREMENT",
               "FileID": "INTEGER NOT NULL",
               "OperationID": "INTEGER NOT NULL",
               "RequestID": "INTEGER NOT NULL",
               "LFN": "VARCHAR(255) NOT NULL",
               "Attempt": "INTEGER NOT NULL DEFAULT 0",
               "Checksum": "VARCHAR(64)",
               "ChecksumType": "ENUM('ADLER32', 'MD5', 'SHA1', 'NONE') DEFAULT 'ADLER32'",
               "CreationTime" : "DATETIME",
               "LastUpdate" : "DATETIME",
               "Size": "INTEGER NOT NULL",
               "FTSGUID":  "VARCHAR(64)",
               "SourceSE": "VARCHAR(128) NOT NULL",
               "SourceSURL": "VARCHAR(255) NOT NULL",
               "TargetSE": "VARCHAR(128) NOT NULL",
               "TargetSURL": "VARCHAR(255) NOT NULL",
               "Status": "VARCHAR(128) DEFAULT 'Waiting'",
               "Error": "VARCHAR(255)"  },
             "PrimaryKey": [ "FTSFileID" ],
             "Indexes": { "FTSGUID": [ "FTSGUID" ], "FTSFileID": [ "FTSFileID"],
                          "FileID": ["FileID", "OperationID"], "LFN": ["LFN"],
                          "SourceSETargetSE": [ "SourceSE", "TargetSE" ]  } }

  @property
  def FTSFileID( self ):
    """ FTSFileID getter """
    return self.__data__["FTSFileID"]

  @FTSFileID.setter
  def FTSFileID( self, value ):
    """ FTSFileID setter """
    self.__data__["FTSFileID"] = long( value ) if value else 0

  @property
  def FTSGUID( self ):
    """ FTSGUID getter """
    if self._parent:
      self.__data__["FTSGUID"] = self._parent.FTSGUID
    return self.__data__["FTSGUID"]

  @FTSGUID.setter
  def FTSGUID( self, value ):
    """ FTSGUID setter """
    if self._parent:
      self.__data__["FTSGUID"] = self._parent.FTSGUID
    else:
      self.__data__["FTSGUID"] = value

  @property
  def RequestID( self ):
    """ RequestID getter """
    return self.__data__["RequestID"]

  @RequestID.setter
  def RequestID( self, value ):
    """ RequestID setter """
    value = long( value ) if value else 0
    self.__data__["RequestID"] = value

  @property
  def OperationID( self ):
    """ OperationID getter """
    return self.__data__["OperationID"]

  @OperationID.setter
  def OperationID( self, value ):
    """ OperationID setter """
    value = long( value ) if value else 0
    self.__data__["OperationID"] = value

  @property
  def FileID( self ):
    """ FileID getter """
    return self.__data__["FileID"]

  @FileID.setter
  def FileID( self, value ):
    """ FileID setter """
    value = long( value ) if value else None
    self.__data__["FileID"] = value

  @property
  def LFN( self ):
    """ LFN prop """
    return self.__data__["LFN"]

  @LFN.setter
  def LFN( self, value ):
    """ lfn setter """
    if type( value ) != str:
      raise TypeError( "LFN has to be a string!" )
    if not os.path.isabs( value ):
      raise ValueError( "LFN should be an absolute path!" )
    self.__data__["LFN"] = value

  @property
  def CreationTime( self ):
    """ creation time getter """
    return self.__data__["CreationTime"]

  @CreationTime.setter
  def CreationTime( self, value = None ):
    """ creation time setter """
    if type( value ) not in ( datetime.datetime, str ) :
      raise TypeError( "CreationTime should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["CreationTime"] = value

  @property
  def LastUpdate( self ):
    """ last update getter """
    return self.__data__["LastUpdate"]

  @LastUpdate.setter
  def LastUpdate( self, value = None ):
    """ last update setter """
    if type( value ) not in  ( datetime.datetime, str ):
      raise TypeError( "LastUpdate should be a datetime.datetime!" )
    if type( value ) == str:
      value = datetime.datetime.strptime( value.split( "." )[0], '%Y-%m-%d %H:%M:%S' )
    self.__data__["LastUpdate"] = value

  @property
  def Attempt( self ):
    """ attempt getter """
    return self.__data__["Attempt"]

  @Attempt.setter
  def Attempt( self, value ):
    """ attempt setter """
    value = int( value )
    if value < 0:
      raise ValueError( "Attempt should be a positive integer!" )
    self.__data__["Attempt"] = int( value )

  @property
  def Size( self ):
    """ file size """
    return self.__data__["Size"]

  @Size.setter
  def Size( self, value ):
    """ file size setter """
    value = long( value )
    if value < 0:
      raise ValueError( "Size should be a positive integer!" )
    self.__data__["Size"] = value

  @property
  def Checksum( self ):
    """ checksum prop """
    return self.__data__["Checksum"]

  @Checksum.setter
  def Checksum( self, value ):
    """ checksum setter """
    self.__data__["Checksum"] = str( value )

  @property
  def ChecksumType( self ):
    """ checksum type prop """
    return self.__data__["ChecksumType"]

  @ChecksumType.setter
  def ChecksumType( self, value ):
    """ checksum type setter """
    if str( value ).upper() not in ( "ADLER32", "MD5", "SHA1", "NONE" ):
      raise ValueError( "unknown checksum type: %s" % value )
    self.__data__["ChecksumType"] = str( value ).upper() if value else None

  @property
  def SourceSE( self ):
    """ source SE prop """
    return self.__data__["SourceSE"] if self.__data__["SourceSE"] else ""

  @SourceSE.setter
  def SourceSE( self, value ):
    """source SE setter """
    self.__data__["SourceSE"] = value[:255] if value else ""

  @property
  def SourceSURL( self ):
    """ source SURL getter """
    return self.__data__["SourceSURL"] if self.__data__["SourceSURL"] else ""

  @SourceSURL.setter
  def SourceSURL( self, value ):
    """ source SURL getter """
    self.__data__["SourceSURL"] = value[:255] if value else ""

  @property
  def TargetSE( self ):
    """ target SE prop """
    return self.__data__["TargetSE"] if self.__data__["TargetSE"] else ""

  @TargetSE.setter
  def TargetSE( self, value ):
    """ target SE setter """
    self.__data__["TargetSE"] = value[:255] if value else ""

  @property
  def TargetSURL( self ):
    """ target SURL getter """
    return self.__data__["TargetSURL"] if self.__data__["TargetSURL"] else ""

  @TargetSURL.setter
  def TargetSURL( self, value ):
    """ target SURL getter """
    self.__data__["TargetSURL"] = value[:255] if value else ""

  @property
  def Error( self ):
    """ error prop """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, value ):
    """ error setter """
    self.__data__["Error"] = str( value )[:255] if value else ""

  @property
  def Status( self ):
    """ status prop """
    if not self.__data__["Status"]:
      self.__data__["Status"] = "Waiting"
    return self.__data__["Status"]

  @Status.setter
  def Status( self, value ):
    """ status setter """
    reStatus = re.compile( "Waiting.*|Submitted|Ready|Staging|Canceled|Active|Failed|Finished" )
    if not reStatus.match( value ):
      raise ValueError( "Unknown FTSFile Status: %s" % str( value ) )
    self.__data__["Status"] = value

  def toJSON( self ):
    """ dump FTSFile to JSON format """
    return S_OK( dict( zip( self.__data__.keys(),
                      [ val if val != None else "" for val in self.__data__.values() ] ) ) )
  def toSQL( self ):
    """ prepare SQL INSERT or UPDATE statement """
    colVals = [ ( "`%s`" % column, "'%s'" % value if type( value ) in ( str, datetime.datetime ) else str( value ) )
                for column, value in self.__data__.items()
                if value and column not in ( "FTSFileID", "LastUpdate" )  ]
    colVals.append( ( "LastUpdate", "UTC_TIMESTAMP()" ) )
    query = []
    if self.FTSFileID:
      query.append( "UPDATE `FTSFile` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FTSFileID`=%d;\n" % self.FTSFileID )
    else:
      query.append( "INSERT INTO `FTSFile` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;" % values )
    return S_OK( "".join( query ) )
