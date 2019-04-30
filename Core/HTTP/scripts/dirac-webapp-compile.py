#!/usr/bin/env python
"""
This is used to compile WebAppDIRAC and its extension, if exists
"""
import sys
import os
from DIRAC.Core.Base import Script

newCompiler = False  # we have to keep the backward compatibility...

from DIRAC import gLogger, S_ERROR, S_OK
from DIRAC.FrameworkSystem.Client.WebAppCompiler import WebAppCompiler

try:
  from DIRAC.Core.HTTP.Lib.Compiler import Compiler
except ImportError as e:
  newCompiler = True

__RCSID__ = "$Id$"


class Params:

  def __init__(self):
    self.destination = ''
    self.name = False
    self.webappversion = ''  # this is useful for installing a web extension
    self.extjspath = None

  def isOK(self):
    if not self.name:
      return S_ERROR("No name defined")
    return S_OK()

  def setDestination(self, opVal):
    self.destination = os.path.realpath(opVal)
    return S_OK()

  def setName(self, opVal):
    self.name = opVal
    return S_OK()

  def setWebappversion(self, opVal):
    self.webappversion = opVal
    return S_OK()

  def setExtJsPath(self, opVal):
    self.extjspath = opVal
    return S_OK()


if __name__ == "__main__":

  if newCompiler:
    cliParams = Params()

    Script.disableCS()
    Script.addDefaultOptionValue("/DIRAC/Setup", "Dummy")
    Script.registerSwitch("n:", "name=", "Tarball name", cliParams.setName)
    Script.registerSwitch("D:", "destination=", "Destination where to build the tar files", cliParams.setDestination)
    Script.registerSwitch("P:", "extjspath=", "directory of the extjs library", cliParams.setExtJsPath)
    Script.registerSwitch("w:", "webappversion=", "In case you have a Web extension", cliParams.setWebappversion)
    Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                      '\nUsage:',
                                      '  %s <option> ...\n' % Script.scriptName,
                                      '  A source, name and version are required to build the tarball',
                                      '  For instance:',
                                      '     %s -n WebAppDIRAC ' % Script.scriptName]))
    Script.parseCommandLine(ignoreErrors=False)

    result = cliParams.isOK()
    if not result['OK']:
      gLogger.error(result['Message'])
      Script.showHelp()
      sys.exit(1)

    compiler = WebAppCompiler(cliParams)
    result = compiler.run()
    if not result['OK']:
      gLogger.fatal(result['Message'])
      sys.exit(1)
  else:
    result = Compiler().run()
    if not result['OK']:
      gLogger.fatal(result['Message'])
      sys.exit(1)
  sys.exit(0)
