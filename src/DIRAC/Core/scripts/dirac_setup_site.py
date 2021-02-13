#!/usr/bin/env python
########################################################################
# File :    dirac-setup-site
# Author :  Ricardo Graciani
########################################################################
"""
Initial installation and configuration of a new DIRAC server (DBs, Services, Agents, Web Portal,...)

Usage:
  dirac-setup-site [option] ... [cfgfile]

Arguments:
  cfgfile: DIRAC Cfg with description of the configuration (optional)
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


class Params(object):

  def __init__(self):
    self.exitOnError = False

  def setExitOnError(self, value):
    self.exitOnError = True
    return S_OK()


@DIRACScript()
def main():
  cliParams = Params()

  Script.disableCS()
  Script.registerSwitch(
      "e",
      "exitOnError",
      "flag to exit on error of any component installation",
      cliParams.setExitOnError)

  Script.addDefaultOptionValue('/DIRAC/Security/UseServerCertificate', 'yes')
  Script.addDefaultOptionValue('LogLevel', 'INFO')
  Script.parseCommandLine()
  args = Script.getExtraCLICFGFiles()

  if len(args) > 1:
    Script.showHelp(exitCode=1)

  cfg = None
  if len(args):
    cfg = args[0]
  from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

  gComponentInstaller.exitOnError = cliParams.exitOnError

  result = gComponentInstaller.setupSite(Script.localCfg, cfg)
  if not result['OK']:
    print("ERROR:", result['Message'])
    exit(-1)

  result = gComponentInstaller.getStartupComponentStatus([])
  if not result['OK']:
    print('ERROR:', result['Message'])
    exit(-1)

  print("\nStatus of installed components:\n")
  result = gComponentInstaller.printStartupStatus(result['Value'])
  if not result['OK']:
    print('ERROR:', result['Message'])
    exit(-1)


if __name__ == "__main__":
  main()
