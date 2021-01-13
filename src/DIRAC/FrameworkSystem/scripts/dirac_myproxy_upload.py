#!/usr/bin/env python
########################################################################
# File :    dirac-proxy-init.py
# Author :  Adrian Casajus
########################################################################

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import sys
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class Params:
  proxyLoc = False
  dnAsUsername = False

  def setProxyLocation(self, arg):
    self.proxyLoc = arg
    return DIRAC.S_OK()

  def setDNAsUsername(self, arg):
    self.dnAsUsername = True
    return DIRAC.S_OK()

  def showVersion(self, arg):
    print("Version:")
    print(" ", __RCSID__)
    sys.exit(0)
    return DIRAC.S_OK()


@DIRACScript()
def main():
  params = Params()

  Script.registerSwitch("f:", "file=", "File to use as proxy", params.setProxyLocation)
  Script.registerSwitch("D", "DN", "Use DN as myproxy username", params.setDNAsUsername)
  Script.registerSwitch("i", "version", "Print version", params.showVersion)

  Script.addDefaultOptionValue("LogLevel", "always")
  Script.parseCommandLine()

  from DIRAC.Core.Security.MyProxy import MyProxy
  from DIRAC.Core.Security import Locations

  if not params.proxyLoc:
    params.proxyLoc = Locations.getProxyLocation()

  if not params.proxyLoc:
    print("Can't find any valid proxy")
    sys.exit(1)
  print("Uploading proxy file %s" % params.proxyLoc)

  mp = MyProxy()
  retVal = mp.uploadProxy(params.proxyLoc, params.dnAsUsername)
  if not retVal['OK']:
    print("Can't upload proxy:")
    print(" ", retVal['Message'])
    sys.exit(1)
  print("Proxy uploaded")
  sys.exit(0)


if __name__ == "__main__":
  main()
