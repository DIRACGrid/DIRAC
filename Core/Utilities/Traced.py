########################################################################
# $HeadURL $
# File:  Traced.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/08/08 13:29:18
########################################################################
""" :mod: Traced
    ============
 
    .. module: Traced
    :synopsis: watched mutable metaclass
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    watched mutable metaclass tracing all updated indexes or keys
"""

__RCSID__ = "$Id $"

##
# @file Traced.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/08/08 13:29:27
# @brief Definition of Traced  metaclass.

########################################################################
class Traced( type ):
  """
  .. class:: Traced

  metaclass telling if some attrs was updated

  overwrites __setattr__ and __setitem__
  adds updated member function and __updated__ attribute 
  """
  def __new__( cls, cls_name, bases, classdict ):
    """ prepare new class instance """

    def updated( self, element=None, reset=False ):
      """ updates and returns __updated__ list 
      
      :param self: self reference
      :param mixed element: key name or list index
      :param bool reset: flag to zero __updated__ list
      
      :return: __updated__ list when called without arguments 
      """
      if not self.__update__ or reset:
        self.__update__ = list()
      if element != None and element not in self.__update__:
        self.__update__.append( element )
      return self.__update__

    def trace_setattr( self, name, value ):
      """ __setattr__ tracing value update """
      #if not name.startswith("_") and name not in dir(self):
      #  raise AttributeError("'%s' has no attribute '%s'" % ( self.__class__.__name__, name ) )
      if name != "__update__":
        if not hasattr( self, name ) or getattr( self, name ) != value:
          self.updated( name )
      bases[0].__setattr__( self, name, value )
    
    def trace_update( self, seq ):      
      """ for dict only """
      for key, value in seq.items():
        if key not in self or bases[0].__getitem__( self, key ) != value:
          self.updated( key )
      bases[0].update( seq )
   
    def trace_append( self, item ):
      """ append for list """
      self.updated( len(self) )
      self += [ item ] 

    def trace_setitem( self, ind, item ):
      """ __setitem__ tracing value update """
      if bases[0] == dict and ( ind not in self or bases[0].__getitem__( self, ind ) != item ):
        self.updated( ind )
      elif bases[0] == list and bases[0].__getitem__( self, ind ) != item:
        self.updated( ind )
      bases[0].__setitem__( self, ind, item )
   
    classdict["__setattr__"] = trace_setattr
    classdict["__setitem__"] = trace_setitem
    if bases[0] == dict:
      classdict["update"] = trace_update
    if bases[0] == list:
      classdict["append"] = trace_append

    classdict["updated"] = updated 
    classdict["__update__"] = None

    return type.__new__( cls, cls_name, bases, classdict )
  
class TracedDict(dict):
  """ traced dict """
  __metaclass__ = Traced
    
class TracedList(list):
  """ traced list """
  __metaclass__ = Traced
