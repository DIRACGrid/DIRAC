########################################################################
# File :    CSGlobals.py
# Author :  Ricardo Graciani
########################################################################
"""
Some Helper functions to retrieve common location from the CS
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import imp
import six
from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton


@six.add_metaclass(DIRACSingleton)
class Extensions(object):
  def __init__(self):
    self.__modules = {}
    self.__orderedExtNames = []
    self.__csExt = []

  def __load(self):
    if self.__orderedExtNames:
      return
    for extName in self.getCSExtensions() + ['']:
      try:
        if not extName.endswith("DIRAC"):
          extName = '%sDIRAC' % extName
        res = imp.find_module(extName)
        if res[0]:
          res[0].close()
        self.__orderedExtNames.append(extName)
        self.__modules[extName] = res
      except ImportError:
        pass

  def getCSExtensions(self):
    if not self.__csExt:
      if six.PY3:
        from DIRAC.Core.Utilities.DIRACScript import _extensionsByPriority
        exts = _extensionsByPriority()
      else:
        from DIRAC.ConfigurationSystem.Client.Config import gConfig
        exts = gConfig.getValue('/DIRAC/Extensions', [])

      self.__csExt = []
      for iP in range(len(exts)):
        ext = exts[iP]
        if ext.endswith("DIRAC"):
          ext = ext[:-5]
        # If the extension is now "" (i.e. vanilla DIRAC), don't include it
        if ext:
          self.__csExt.append(ext)
    return self.__csExt

  def getInstalledExtensions(self):
    self.__load()
    return list(self.__orderedExtNames)

  def getExtensionPath(self, extName):
    self.__load()
    return self.__modules[extName][1]

  def getExtensionData(self, extName):
    self.__load()
    return self.__modules[extName]


def getSetup():
  from DIRAC import gConfig
  return gConfig.getValue("/DIRAC/Setup", "")


def getVO(defaultVO=''):
  """
    Return VO from configuration
  """
  from DIRAC import gConfig
  return gConfig.getValue('/DIRAC/VirtualOrganization', defaultVO)


def getCSExtensions():
  """
    Return list of extensions registered in the CS
    They do not include DIRAC
  """
  return Extensions().getCSExtensions()


def getInstalledExtensions():
  """
    Return list of extensions registered in the CS and available in local installation
  """
  return Extensions().getInstalledExtensions()


def skipCACheck():
  from DIRAC import gConfig
  return gConfig.getValue("/DIRAC/Security/SkipCAChecks", "false").lower() in ("y", "yes", "true")


def useServerCertificate():
  from DIRAC import gConfig
  return gConfig.getValue("/DIRAC/Security/UseServerCertificate", "false").lower() in ("y", "yes", "true")
