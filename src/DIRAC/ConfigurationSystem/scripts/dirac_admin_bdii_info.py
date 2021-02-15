#! /usr/bin/env python
########################################################################
# File :    dirac-admin-bdii-info
# Author :  Aresh Vedaee
########################################################################
"""
Check info on BDII for a given CE or site

Usage:
  dirac-admin-bdii-info [options] ... <info> <Site|CE>

Arguments:
  Site:     Name of the Site (i.e. CERN-PROD)
  CE:       Name of the CE (i.e. cccreamceli05.in2p3.fr)
  info:     Accepted values (ce|ce-state|ce-cluster|ce-vo|site)
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup


def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  Script.registerSwitch("H:", "host=", "BDII host")
  Script.registerSwitch("V:", "vo=", "vo")


def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''

  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  if not len(args) == 2:
    Script.showHelp()

  params = {}
  params['ce'] = None
  params['site'] = None
  params['host'] = None
  params['vo'] = None
  params['info'] = args[0]
  ret = getProxyInfo(disableVOMS=True)

  if ret['OK'] and 'group' in ret['Value']:
    params['vo'] = getVOForGroup(ret['Value']['group'])
  else:
    Script.gLogger.error('Could not determine VO')
    Script.showHelp()

  if params['info'] in ['ce', 'ce-state', 'ce-cluster', 'ce-vo']:
    params['ce'] = args[1]
  elif params['info'] in ['site']:
    params['site'] = args[1]
  else:
    Script.gLogger.error('Wrong argument value')
    Script.showHelp()

  for unprocSw in Script.getUnprocessedSwitches():
    if unprocSw[0] in ("H", "host"):
      params['host'] = unprocSw[1]
    if unprocSw[0] in ("V", "vo"):
      params['vo'] = unprocSw[1]

  return params


def getInfo(params):
  '''
    Retrieve information from BDII
  '''

  from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
  diracAdmin = DiracAdmin()

  if params['info'] == 'ce':
    result = diracAdmin.getBDIICE(params['ce'], host=params['host'])
  if params['info'] == 'ce-state':
    result = diracAdmin.getBDIICEState(params['ce'], useVO=params['vo'], host=params['host'])
  if params['info'] == 'ce-cluster':
    result = diracAdmin.getBDIICluster(params['ce'], host=params['host'])
  if params['info'] == 'ce-vo':
    result = diracAdmin.getBDIICEVOView(params['ce'], useVO=params['vo'], host=params['host'])
  if params['info'] == 'site':
    result = diracAdmin.getBDIISite(params['site'], host=params['host'])

  if not result['OK']:
    print(result['Message'])
    DIRAC.exit(2)

  return result


def showInfo(result, info):
  '''
    Display information
  '''

  elements = result['Value']

  for element in elements:
    if info == 'ce' or info == 'all':
      print("CE: %s \n{" % element.get('GlueSubClusterName', 'Unknown'))

    if info == 'ce-state' or info == 'all':
      print("CE: %s \n{" % element.get('GlueCEUniqueID', 'Unknown'))

    if info == 'ce-cluster' or info == 'all':
      print("Cluster: %s \n{" % element.get('GlueClusterName', 'Unknown'))

    if info == 'ce-vo' or info == 'all':
      print("CEVOView: %s \n{" % element.get('GlueChunkKey', 'Unknown'))

    if info == 'site' or info == 'all':
      print("Site: %s \n{" % element.get('GlueSiteName', 'Unknown'))

    for item in element.items():
      print("  %s: %s" % item)
    print("}")


@DIRACScript()
def main():
  # Script initialization
  registerSwitches()
  # registerUsageMessage()
  params = parseSwitches()
  result = getInfo(params)
  showInfo(result, params['info'])

  DIRAC.exit(0)


if __name__ == "__main__":
  main()
