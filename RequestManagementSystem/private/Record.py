########################################################################
# $HeadURL $
# File: Record.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/02 08:42:19
########################################################################

""" :mod: Record
    ============

    .. module: Record
    :synopsis: db record
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    base class for orm-like db record
"""

__RCSID__ = "$Id $"

# #
# @file Record.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/02 08:42:24
# @brief Definition of Record class.

# # imports
from DIRAC import gLogger

########################################################################
class Record( object ):
  """
  .. class:: Record

  a single record in the db

  all columns should be exported as properties in the inherited classes
  """

  def __init__( self ):
    """c'tor

    :param self: self reference
    """
    self.__dirty = []
    self.__data__ = dict.fromkeys( self.tableDesc()["Fields"].keys(), None )

  def __setattr__( self, name, value ):
    """ bweare of tpyos!!! """
    if not name.startswith( "_" ) and name not in dir( self ):
      raise AttributeError( "'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
    try:
      object.__setattr__( self, name, value )
    except AttributeError, error:
      gLogger.exception( error )

  @staticmethod
  def tableDesc():
    """ should return dict with table description, i.e.::

    { "Fields" : { "RecID" : "INTEGER NOT NULL AUTO_INCREMENT",
                   "Data": "FLOAT" },
      "PrimaryKey" : [ "RecID" ] },
      "Indexes" : { "RecID" : [ "RecID" ] } }
    """
    raise NotImplementedError( "Must provide table description!" )

  @staticmethod
  def _uniqueList( value, sep = "," ):
    """ make unique list from :value: """
    if type( value ) not in ( str, unicode, list ):
      raise TypeError( "wrong type for value" )
    if type( value ) in ( str, unicode ):
      value = value.split( sep )
    return list ( set ( [ str( item ).strip() for item in value if str( item ).strip() ] ) )

  @staticmethod
  def _escapeStr( aStr, lenght = 255 ):
    """ remove ' and cut  """
    return str( aStr ).replace( "'", "" )[:lenght] if aStr else ""

