########################################################################
# $HeadURL $
# File: File.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/03 15:02:53
########################################################################
""" :mod: File
    ==========

    .. module: File
    :synopsis: RMS operation file
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    sub-request file
"""
# for properties
# pylint: disable=E0211,W0612,W0142,E1101,E0102

__RCSID__ = "$Id $"

# #
# @file File.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/03 15:03:03
# @brief Definition of File class.

# # imports
import os
import urlparse
import xml.etree.ElementTree as ElementTree
from xml.parsers.expat import ExpatError
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.Record import Record
from DIRAC.Core.Utilities.File import checkGuid

########################################################################
class File( Record ):
  """
  .. class:: File

   A bag object holding Operation file attributes.

  :param Operation _parent: reference to parent Operation
  :param dict __data__: attrs dict
  """

  def __init__( self, fromDict = None ):
    """c'tor

    :param self: self reference
    :param dict fromDict: property dict
    """
    Record.__init__( self )
    self._parent = None
    self.__data__["Status"] = "Waiting"
    self.__data__["OperationID"] = 0
    self.__data__["FileID"] = 0
    self.__data__["Checksum"] = ""
    self.__data__["ChecksumType"] = ""
    self.__data__["Attempt"] = 0
    fromDict = fromDict if fromDict else {}
    for attrName, attrValue in fromDict.items():
      if attrName not in self.__data__:
        raise AttributeError( "unknown File attribute %s" % str( attrName ) )
      setattr( self, attrName, attrValue )

  @staticmethod
  def tableDesc():
    """ get table desc """
    return { "Fields" :
             { "FileID" : "INTEGER NOT NULL AUTO_INCREMENT",
               "OperationID" : "INTEGER NOT NULL",
               "Status" : "ENUM('Waiting', 'Done', 'Failed', 'Scheduled') DEFAULT 'Waiting'",
               "LFN" : "VARCHAR(255)",
               "PFN" : "VARCHAR(255)",
               "ChecksumType" : "ENUM('ADLER32', 'MD5', 'SHA1', '') DEFAULT ''",
               "Checksum" : "VARCHAR(255)",
               "GUID" : "VARCHAR(36)",
               "Size" : "INTEGER",
               "Attempt": "INTEGER",
               "Error" : "VARCHAR(255)" },
             "PrimaryKey" : "FileID",
             "Indexes" : { "LFN" : [ "LFN" ] } }

  def __eq__( self, other ):
    """ == operator, comparing only LFN or PFN """
    return ( self.LFN == other.LFN ) or ( self.PFN == other.PFN )

  # # properties

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
  def OperationID( self ):
    """ operation ID (RO) """
    self.__data__["OperationID"] = self._parent.OperationID if self._parent else 0
    return self.__data__["OperationID"]

  @OperationID.setter
  def OperationID( self, value ):
    """ operation ID (RO) """
    self.__data__["OperationID"] = self._parent.OperationID if self._parent else 0

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
    """ file size getter """
    return self.__data__["Size"]

  @Size.setter
  def Size( self, value ):
    """ file size setter """
    value = long( value )
    if value < 0:
      raise ValueError( "Size should be a positive integer!" )
    self.__data__["Size"] = value

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
  def PFN( self ):
    """ PFN prop """
    return self.__data__["PFN"]

  @PFN.setter
  def PFN( self, value ):
    """ PFN setter """
    if type( value ) != str:
      raise TypeError( "PFN has to be a string!" )
    isURL = urlparse.urlparse( value ).scheme
    isABS = os.path.isabs( value )
    if not isURL and not isABS:
      raise ValueError( "Wrongly formatted PFN!" )
    self.__data__["PFN"] = value

  @property
  def GUID( self ):
    """ GUID prop """
    return self.__data__["GUID"]

  @GUID.setter
  def GUID( self, value ):
    """ GUID setter """
    if value:
      if type( value ) not in ( str, unicode ):
        raise TypeError( "GUID should be a string!" )
      if not checkGuid( value ):
        raise ValueError( "'%s' is not a valid GUID!" % str( value ) )
    self.__data__["GUID"] = value

  @property
  def ChecksumType( self ):
    """ checksum type prop """
    return self.__data__["ChecksumType"]

  @ChecksumType.setter
  def ChecksumType( self, value ):
    """ checksum type setter """
    if not value:
      self.__data__["ChecksumType"] = ""
    elif value and str( value ).strip().upper() not in ( "ADLER32", "MD5", "SHA1" ):
      raise ValueError( "unknown checksum type: %s" % value )
    else:
      self.__data__["ChecksumType"] = str( value ).strip().upper()

  @property
  def Checksum( self ):
    """ checksum prop """
    return self.__data__["Checksum"]

  @Checksum.setter
  def Checksum( self, value ):
    """ checksum setter """
    self.__data__["Checksum"] = str( value ) if value else ""

  @property
  def Error( self ):
    """ error prop """
    return self.__data__["Error"]

  @Error.setter
  def Error( self, value ):
    """ error setter """
    if type( value ) != str:
      raise TypeError( "Error has to be a string!" )
    self.__data__["Error"] = value.replace( "'", "\'" )[255:]
    print self.__data__["Error"]

  @property
  def Status( self ):
    """ status prop """
    if not self.__data__["Status"]:
      self.__data__["Status"] = "Waiting"
    return self.__data__["Status"]

  @Status.setter
  def Status( self, value ):
    """ status setter """
    if value not in ( "Waiting", "Failed", "Done", "Scheduled" ):
      raise ValueError( "Unknown Status: %s!" % str( value ) )
    self.__data__["Status"] = value
    if self._parent: self._parent._notify()

  # # (de)serialization
  def toXML( self, dumpToStr = False ):
    """ serialize File to XML """
    dumpToStr = bool( dumpToStr )
    attrs = dict( [ ( k, str( getattr( self, k ) ) if getattr( self, k ) else "" ) for k in self.__data__ ] )
    element = ElementTree.Element( "file", attrs )
    return S_OK( { False: element,
                    True: ElementTree.tostring( element ) }[dumpToStr] )

  @classmethod
  def fromXML( cls, element ):
    """ build File form ElementTree.Element :element: """
    if type( element ) == str:
      try:
        element = ElementTree.fromstring( element )
      except ExpatError, error:
        return S_ERROR( str( error ) )
    if element.tag != "file":
      return S_ERROR( "wrong tag, expected 'file', got %s" % element.tag )
    fromDict = dict( [ ( key, value ) for key, value in element.attrib.items() if value ] )
    return S_OK( File( fromDict ) )

  def __str__( self ):
    """ str operator """
    return ElementTree.tostring( self.toXML() )

  def toSQL( self ):
    """ get SQL INSERT or UPDATE statement """
    if not self._parent:
      raise AttributeError( "File does not belong to any Operation" )

    colVals = [ ( "`%s`" % column, "'%s'" % getattr( self, column )
                  if type( getattr( self, column ) ) == str else str( getattr( self, column ) ) )
                for column in self.__data__
                if getattr( self, column ) and column != "FileID" ]
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

  def toJSON( self ):
    """ get json """
    digest = dict( zip( self.__data__.keys(),
                        [ str( val ) if val else "" for val in self.__data__.values() ] ) )
    digest["OperationID"] = str( self.OperationID )
    return S_OK( digest )
