########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ModuleFactory.py,v 1.1 2007/12/13 20:47:31 paterson Exp $
# File :   ModuleFactory.py
# Author : Stuart Paterson
########################################################################

"""  The Module Factory instantiates a given Module based on a given input
     string and set of arguments to be passed.  This allows for VO specific
     module utilities to be used in various contexts.
"""

from DIRAC                                               import S_OK, S_ERROR, gLogger

__RCSID__ = "$Id: ModuleFactory.py,v 1.1 2007/12/13 20:47:31 paterson Exp $"

import re,sys,types,string

class ModuleFactory:

  #############################################################################
  def __init__(self):
    """ Standard constructor
    """
    self.log = gLogger

  #############################################################################
  def getModule(self,importString,argumentsDict):
    """This method returns the Module instance given the import string and
       arguments dictionary.
    """
    try:
      moduleName = string.split(importString,'.')[-1]
      modulePath = importString.replace('.%s' %(moduleName),'')
      importModule = __import__('%s.%s' % modulePath,moduleName,globals(),locals(),[moduleName])
    except Exception, x:
      msg = 'ModuleFactory could not import %s.%s' %(modulePath,moduleName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    try:
      moduleStr = 'importModule.%s(argumentsDict)' %(moduleName)
      moduleInstance = eval(moduleStr)
    except Exception, x:
      msg = 'ModuleFactory could not instantiate %s()' %(moduleName)
      self.log.warn(x)
      self.log.warn(msg)
      return S_ERROR(msg)

    return S_OK(moduleInstance)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#