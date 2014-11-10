########################################################################
# $HeadURL $
# File: File.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/03 15:02:53
########################################################################
"""
:mod: File

.. module: File
  :synopsis: RMS operation file

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

operation file
"""
# for properties
# pylint: disable=E0211,W0612,W0142,E1101,E0102,C0103

__RCSID__ = "$Id $"

# #
# @file File.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/03 15:03:03
# @brief Definition of File class.

# # imports
import datetime
import copy
import os
import json
from types import StringTypes
# import urlparse
# # from DIRAC
from DIRAC import S_OK, S_ERROR
# from DIRAC.RequestManagementSystem.private.RMSBase import RMSBase
from DIRAC.Core.Utilities.File import checkGuid
from DIRAC.RequestManagementSystem.private.JSONUtils import RMSEncoder


from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Enum, BLOB, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property


########################################################################
# class File( RMSBase ):
class File( object ):

  """
  .. class:: File

   A bag object holding Operation file attributes.

  :param Operation _parent: reference to parent Operation
  :param dict __data__: attrs dict
  """

#   __tablename__ = 'File'
#   FileID = Column( Integer, primary_key = True )
#   OperationID = Column( Integer,
#                         ForeignKey( 'Operation.OperationID', ondelete = 'CASCADE' ),
#                         nullable = False )
#
#   _Status = Column( 'Status', Enum( 'Waiting', 'Done', 'Failed', 'Scheduled' ), server_default = 'Waiting' )
#   _LFN = Column( 'LFN', String( 255 ), index = True )
#   PFN = Column( String( 255 ) )
#   _ChecksumType = Column( 'ChecksumType', Enum( 'ADLER32', 'MD5', 'SHA1', '' ), server_default = '' )
#   Checksum = Column( String( 255 ) )
#   _GUID = Column( 'GUID', String( 36 ) )
#   Size = Column(BigInteger)
#   Attempt = Column(Integer)
#   Error = Column( String( 255 ) )


  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: property dict
    """
    self._parent = None
#     self.FileID = 0
#     self._OperationID = 0
    self._Status = 'Waiting'
    self._LFN = None
    self.PFN = None
    self._ChecksumType = None
    self.Checksum = None
    self._GUID = None
    self.Attempt = 0
    self.Size = 0
    self.Error = None
    self._duration = 0
#     self.FileID = -1
#     self.OperationID = -2

    fromDict = fromDict if isinstance( fromDict, dict ) else json.loads( fromDict ) if isinstance( fromDict, StringTypes ) else {}

    for attrName, attrValue in fromDict.items():
#       if attrName not in self.__data__:
#         raise AttributeError( "unknown File attribute %s" % str( attrName ) )
      if type( attrValue ) in StringTypes:
        attrValue = attrValue.encode()
      if attrValue:
        setattr( self, attrName, attrValue )

#   @staticmethod
#   def tableDesc():
#     """ get table desc """
#     return { "Fields" :
#              { "FileID" : "INTEGER NOT NULL AUTO_INCREMENT",
#                "OperationID" : "INTEGER NOT NULL",
#                "Status" : "ENUM('Waiting', 'Done', 'Failed', 'Scheduled') DEFAULT 'Waiting'",
#                "LFN" : "VARCHAR(255)",
#                "PFN" : "VARCHAR(255)",
#                "ChecksumType" : "ENUM('ADLER32', 'MD5', 'SHA1', '') DEFAULT ''",
#                "Checksum" : "VARCHAR(255)",
#                "GUID" : "VARCHAR(36)",
#                "Size" : "BIGINT",
#                "Attempt": "INTEGER",
#                "Error" : "VARCHAR(255)" },
#              "PrimaryKey" : "FileID",
#              'ForeignKeys': {'OperationID': 'Operation.OperationID' },
#              "Indexes" : { "LFN" : [ "LFN" ] } }

  # # properties

#   @hybrid_property
#   def OperationID( self ):
#     """ operation ID (RO) """
#     self._OperationID = self._parent.OperationID if self._parent else 0
#     return self._OperationID
#
#   @OperationID.setter
#   def OperationID( self, value ):
#     """ operation ID (RO) """
#     self._OperationID = self._parent.OperationID if self._parent else 0


  @hybrid_property
  def LFN( self ):
    """ LFN prop """
    return self._LFN

  @LFN.setter
  def LFN( self, value ):
    """ lfn setter """
    if type( value ) not in StringTypes:
      raise TypeError( "LFN has to be a string!" )
    if not os.path.isabs( value ):
      raise ValueError( "LFN should be an absolute path!" )
    self._LFN = value


  @hybrid_property
  def GUID( self ):
    """ GUID prop """
    return self._GUID

  @GUID.setter
  def GUID( self, value ):
    """ GUID setter """
    if value:
      if type( value ) not in StringTypes:
        raise TypeError( "GUID should be a string!" )
      if not checkGuid( value ):
        raise ValueError( "'%s' is not a valid GUID!" % str( value ) )
    self._GUID = value

  @hybrid_property
  def ChecksumType( self ):
    """ checksum type prop """
    return self._ChecksumType

  @ChecksumType.setter
  def ChecksumType( self, value ):
    """ checksum type setter """
    if not value:
      self._ChecksumType = ""
    elif value and str( value ).strip().upper() not in ( "ADLER32", "MD5", "SHA1" ):
      if str( value ).strip().upper() == 'AD':
        self._ChecksumType = 'ADLER32'
      else:
        raise ValueError( "unknown checksum type: %s" % value )
    else:
      self._ChecksumType = str( value ).strip().upper()


  @hybrid_property
  def Status( self ):
    """ status prop """
    if not self._Status:
      self._Status = 'Waiting'
    return self._Status

  @Status.setter
  def Status( self, value ):
    """ status setter """
    if value not in ( "Waiting", "Failed", "Done", "Scheduled" ):
      raise ValueError( "Unknown Status: %s!" % str( value ) )
    if value == 'Done':
      self.Error = ''
    self._Status = value
    if self._parent:
      self._parent._notify()

  def __str__( self ):
    """ str operator """
    return self.toJSON()['Value']

  def toSQL( self ):
    """ get SQL INSERT or UPDATE statement """
    if not self._parent:
      raise AttributeError( "File does not belong to any Operation" )
    colVals = [ ( "`%s`" % column, "'%s'" % getattr( self, column )
                  if type( getattr( self, column ) ) == str
                    else str( getattr( self, column ) ) if getattr( self, column ) != None else "NULL" )
                for column in self.__data__
                if ( column == 'Error' or getattr( self, column ) ) and column != "FileID" ]
    query = []
    if self.FileID:
      query.append( "UPDATE `File` SET " )
      query.append( ", ".join( [ "%s=%s" % item for item in colVals  ] ) )
      query.append( " WHERE `FileID`=%d;\n" % self.FileID )
    else:
      query.append( "INSERT INTO `File` " )
      columns = "(%s)" % ",".join( [ column for column, value in colVals ] )
      values = "(%s)" % ",".join( [ value for column, value in colVals ] )
      query.append( columns )
      query.append( " VALUES %s;\n" % values )
    return S_OK( "".join( query ) )

#   def toJSON( self ):
#     """ get json """
#     digest = dict( [( key, str( val ) ) for key, val in self.__data__.items()] )
#     return S_OK( digest )
#
  def toJSON( self ):
    try:
      jsonStr = json.dumps( self, cls = RMSEncoder )
      return S_OK( jsonStr )
    except Exception, e:
      return S_ERROR( str( e ) )

  def _getJSONData( self ):
    """ Returns the data that have to be serialized by JSON """
    attrNames = [ "Status", "LFN",
                 "PFN", "ChecksumType", "Checksum", "GUID",
                 "Size", "Error"]

    jsonData = {}

    if hasattr( self, 'OperationID' ):
      jsonData['OperationID'] = getattr( self, 'OperationID' )

    if hasattr( self, 'FileID' ):
      jsonData['FileID'] = getattr( self, 'FileID' )

    for attrName in attrNames :
      jsonData[attrName] = getattr( self, attrName )
      value = getattr( self, attrName )

      if isinstance( value, datetime.datetime ):
        # We convert date time to a string
        jsonData[attrName] = value.strftime( self._datetimeFormat )
      else:
        jsonData[attrName] = value

    return jsonData

