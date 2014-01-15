########################################################################
# $HeadURL $
# File: TypedList.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/07/19 08:21:16
########################################################################

""" :mod: TypedList 
    =======================
 
    .. module: TypedList
    :synopsis: typed list
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    typed list
"""

__RCSID__ = "$Id $"

##
# @file TypedList.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/19 08:21:22
# @brief Definition of TypedList class.

from collections import deque
from copy import deepcopy

class Unsortable( list ):
  def sort( self, *args, **kwargs ):
    return 

class TDeque( deque ):

  def __init__( self, iterable, allowedTypes = None ):
    types = allowedTypes if isinstance( allowedTypes, tuple ) else ( allowedTypes, )
    for item in types:
      if not isinstance( item, type ):
        raise TypeError("%s is not a type" % repr(item) )
    deque.__init__( self, iterable )
    
class TypedList( list ):
  """ 
  .. class:: TypedList

  A list-like class holding only objects of specified type(s).
  """   
  def __init__( self, iterable=None, allowedTypes=None ):
    """ c'tor

    :param self: self reference
    :param mixed iterable: initial values
    :param tuple allowedTypes: alowed types tuple
    """
    iterable = list() if not iterable else iterable
    ## make sure it is iterable
    iter(iterable) 
  
    types = allowedTypes if isinstance( allowedTypes, tuple ) else ( allowedTypes, )
    for item in types:
      if not isinstance( item, type ):
        raise TypeError("%s is not a type" % repr(item) )
       
    self._allowedTypes = allowedTypes
    map( self._typeCheck, iterable )
    list.__init__( self, iterable )

  def __deepcopy__( self, memo ):
    """ deepcopy """
    return type(self)( self, allowedTypes = self._allowedTypes )


  def allowedTypes( self ):
    """ allowed types getter """
    return self._allowedTypes

  def _typeCheck( self, val ):
    """ check type of :val:

    :param self: self reference
    :param mixed val: obj to check
    """
    if not self._allowedTypes:
      return 
    if not isinstance( val, self._allowedTypes ):
      raise TypeError("Wrong type %s, this list can hold only instances of %s" % ( type(val), 
                                                                                   str(self._allowedTypes) ) )
  def __iadd__( self, other ):
    """ += operator

    :param self: self reference
    :param mixed other: iterable to add
    :raises: TypeError 
    """
    map( self._typeCheck, other )
    list.__iadd__( self, other )
    return self 
    
  def __add__( self, other ):
    """ plus lvalue operator

    :param self: self reference
    :param mixed other: rvalue iterable
    :return: TypedList
    :raises: TypeError
    """
    iterable = [ item for item in self ] + [ item for item in other ]
    return TypedList( iterable, self._allowedTypes )

  def __radd__( self, other ):
    """ plus rvalue operator

    :param self: self reference
    :param mixed other: lvalue iterable
    :raises: TypeError
    :return: TypedList
    """
    iterable = [ item for item in other ] + [ item for item in self ]
    if isinstance( other, TypedList ):
      return self.__class__( iterable , other.allowedTypes() )
    return TypedList( iterable, self._allowedTypes )

  def __setitem__( self, key, value ):
    """ setitem operator 

    :param self: self reference
    :param int or slice key: index
    :param mixed value: a value to set
    :raises: TypeError
    """
    itervalue = ( value, )
    if isinstance( key, slice ):
      iter( value )
      itervalue = value
    map( self._typeCheck, itervalue )
    list.__setitem__( self, key, value )
   
  def __setslice__( self, i, j, iterable ):
    """ setslice slot, only for python 2.6

    :param self: self reference
    :param int i: start index
    :param int j: end index
    :param mixed iterable: iterable
    """
    iter(iterable)
    map( self._typeCheck, iterable )
    list.__setslice__( self, i, j, iterable )
   
  def append( self, val ):
    """ append :val: to list

    :param self: self reference
    :param mixed val: value
    """
    self._typeCheck( val )
    list.append( self, val )
   
  def extend( self, iterable ):
    """ extend list with :iterable: 
    
    :param self: self referenace
    :param mixed iterable: an interable
    """
    iter(iterable)
    map( self._typeCheck, iterable )
    list.extend( self, iterable )
   
  def insert( self, i, val ):
    """ insert :val: at index :i:
    
    :param self: self reference
    :param int i: index
    :param mixed val: value to set
    """
    self._typeCheck( val )
    list.insert( self, i, val )

class BooleanList( TypedList ):
  """
  .. class:: BooleanList

  A list holding only True or False items.
  """
  def __init__( self, iterable = None ):
    """ c'tor

    :param self: self reference
    :param mixed iterable: initial values
    """
    TypedList.__init__( self, iterable, allowedTypes = bool )
  
class IntList( TypedList ):
  """
  .. class:: IntList

  A list holding only int type items.
  """
  def __init__( self, iterable = None ):
    """ c'tor

    :param self: self reference
    :param mixed iterable: initial values
    """
    TypedList.__init__( self, iterable, allowedTypes = int )

class LongList( TypedList ):
  """
  .. class:: LongList

  A list holding only long type items.
  """
  def __init__( self, iterable = None ):
    """ c'tor

    :param self: self reference
    :param mixed iterable: initial values
    """
    TypedList.__init__( self, iterable, allowedTypes = long )

class FloatList( TypedList ):
  """
  .. class:: FloatList

  A list holding only float type items.
  """
  def __init__( self, iterable = None ):
    """ c'tor

    :param self: self reference
    :param mixed iterable: initial values
    """
    TypedList.__init__( self, iterable, allowedTypes = float )

class NumericList( TypedList ):
  """
  .. class:: NumericList

  A list holding only int, long  or float type items.
  """
  def __init__( self, iterable = None ):
    """ c'tor

    :param self: self reference
    :param mixed iterable: initial values
    """

    TypedList.__init__( self, iterable, allowedTypes = ( int, long, float ) )

class StrList( TypedList ):
  """
  .. class:: StrList

  A list holding only str type items.
  """
  def __init__( self, iterable = None ):
    """ c'tor

    :param self: self reference
    :param mixed iterable: initial values
    """
    TypedList.__init__( self, iterable, allowedTypes = str )

class StringsList( TypedList ):
  """
  .. class:: StringsList

  A list holding only str or unicode type items.
  """
  def __init__( self, iterable = None ):
    """ c'tor

    :param self: self reference
    :param mixed iterable: initial values
    """
    TypedList.__init__( self, iterable, allowedTypes = ( str, unicode ) )
