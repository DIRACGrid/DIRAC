########################################################################
# $HeadURL $
# File: FTSCurePlugin.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/06/30 09:50:42
########################################################################

""" :mod: FTSCurePlugin 
    =======================
 
    .. module: FTSCurePlugin
    :synopsis: base class for FTSCleaningAgent plugins
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    base class for FTSCleaningAgent plugins
"""

__RCSID__ = "$Id $"

##
# @file FTSCurePlugin.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/06/30 09:50:54
# @brief Definition of FTSCurePlugin class.

## imports 
from types import MethodType

def injectFunction( function, toInstance, toClass ):
  """The simplest function injection to the instance toInstance of a class toClass. 

  :param function: function object
  :param toInstance: instance object
  :param toClass: class object

  To temporary inject a function to class you need to:
  
  - create an instance 
  - define a global function with ''self'' parameter as a firts argument
  - call injectFunction, i.e.::

    theInstance = MyClass()
    def aFunction( self, spam, egg ):
      self.methodOfAMyClass()
      msg = "We need more " + ", ".join( ["spam" for i in range(spam)] "
      if egg:
        msg += "and egg!!!"
      return msg        
    injectFunction( aFunction, theInstance, MyClass )

  and from now on you can execute aFunction as a member method of a class MyClass::

    msg = theInstance.aFunction( spam = 10, egg = True )
  """
  setattr( toInstance, 
           function.__name__,
           MethodType( function, toInstance, toClass ) )

########################################################################
class FTSCurePlugin(object):
  """
  .. class:: FTSCurePlugin
  
  base class for cure plugin

  """

  @classmethod
  def injectFunction( cls, function, toInstance, toClass ):
    injectFunction( function, toInstance, toClass )

  def execute( self ):
    """ this function will be executed in threads 
    you have to override it in your child class
    this function should return S_OK/S_ERROR
    """
    raise NotImplementedError("execute() must be implemented in inherited class.")

