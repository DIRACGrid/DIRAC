#!/usr/bin/env python
"""
Script to apply update to all or some dirac servers and restart them
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from io import open

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s version' % Script.scriptName,
                                  ' ',
                                  'Arguments:',
                                  '  version:           version of DIRAC you want to update to']
                                 )
                       )
Script.registerSwitch("", "hosts=", "Comma separated list of hosts or file containing row wise list of hosts"
                                    " targeted for update (leave empty for all)")
Script.parseCommandLine(ignoreErrors=False)

args = Script.getPositionalArgs()
if len(args) < 1 or len(args) > 2:
  Script.showHelp()

version = args[0]

hosts = Script.getUnprocessedSwitches()

if hosts:
  hosts = hosts[0][1]

try:
  with open(hosts, 'r') as f:
    hosts = f.read().splitlines()
except Exception:
  pass

if not isinstance(hosts, list):
  hosts = hosts.split(',')

from concurrent.futures import ThreadPoolExecutor, as_completed

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient


def getListOfHosts():
  """
  Return the list of all hosts that constitute a DIRAC instance
  """
  client = ComponentMonitoringClient()
  result = client.getHosts({}, False, False)
  if result['OK']:
    hosts = [host['HostName'] for host in result['Value']]
    return S_OK(hosts)
  return S_ERROR('Cannot get list of hosts: %s' % result['Message'])


def parseHostname(hostName):
  """
  Separate the hostname from the port

  :param hostName: hostname you want to parse
  """
  hostList = hostName.split(':')
  host = hostList[0]
  if len(hostList) == 2:
    port = hostList[1]
  else:
    port = None
  return host, port


def updateHost(hostName, version):
  """
  Apply update to specific host

  :param hostName: name of the host you want to update
  :param version: version vArBpC you want to update to
  """
  host, port = parseHostname(hostName)

  client = SystemAdministratorClient(host, port)
  result = client.ping()
  if not result['OK']:
    gLogger.error("Cannot connect to %s" % host)
    return result

  gLogger.notice("Initiating software update of %s, this can take a while, please be patient ..." % host)
  result = client.updateSoftware(version, '', '', timeout=600)
  if not result['OK']:
    return result
  return S_OK()


def updateHosts(version, hosts=None):
  """
  Apply update to all hosts

  :param version: version vArBpC you want to update to
  :param hosts: list of hosts to be updated
  """
  if not hosts:
    result = getListOfHosts()
    if not result['OK']:
      return result
    hosts = result['Value']

  updateSuccess = []
  updateFail = []

  executor = ThreadPoolExecutor(max_workers=len(hosts))
  futureUpdate = {executor.submit(updateHost, host, version): host for host in hosts}
  for future in as_completed(futureUpdate):
    host = futureUpdate[future]
    result = future.result()
    if result['OK']:
      updateSuccess.append(host)
    else:
      updateFail.append(host)

  if not updateFail:
    gLogger.notice("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    gLogger.notice("!!! Successfully updated all hosts !!!")
    gLogger.notice("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    return S_OK([updateSuccess, updateFail])
  elif not updateSuccess:
    gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    gLogger.notice("XXXXX Failed to update all hosts XXXXX")
    gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    return S_ERROR('Failed to update all hosts')
  else:
    gLogger.notice("X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!")
    gLogger.notice("X!X!X Partially updated hosts X!X!X!")
    gLogger.notice("Succeeded to update:")
    for host in updateSuccess:
      gLogger.notice(" + %s" % host)
    gLogger.notice("Failed to update:")
    for host in updateFail:
      gLogger.notice(" - %s" % host)
    gLogger.notice("X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!X!")
    return S_OK([updateSuccess, updateFail])


def restartHost(hostName):
  """
  Restart all systems and components of a host

  :param hostName: name of the host you want to restart
  """
  host, port = parseHostname(hostName)

  gLogger.notice("Pinging %s ..." % host)

  client = SystemAdministratorClient(host, port)
  result = client.ping()
  if not result['OK']:
    gLogger.error("Could not connect to %s: %s" % (host, result['Message']))
    return result
  gLogger.notice("Host %s is active" % host)

  gLogger.notice("Initiating restart of all systems and components")
  # This restart call will always return S_ERROR because of SystemAdministrator restart
  # Connection will be lost to the host
  result = client.restartComponent('*', '*')
  if result['Message'] == "Peer closed connection":
    gLogger.notice("Restarted all systems on %s : connection to SystemAdministrator lost" % host)
    return S_OK(result['Message'])
  gLogger.error("Received unxpected message: %s" % result['Message'])
  return result


def updateInstance(version, hosts):
  """
  Update each server of an instance and restart them

  :param version: version vArBpC you want to update to
  :param hosts: list of hosts to be updated
  """
  result = updateHosts(version, hosts)
  if not result['OK']:
    return result

  updateSuccess = result['Value'][0]
  updateFail = result['Value'][1]
  restartSuccess = []
  restartFail = []
  for host in updateSuccess:
    result = restartHost(host)
    if result['OK']:
      restartSuccess.append(host)
    else:
      restartFail.append(host)

  if not restartFail and not updateFail:
    return S_OK("Successfully updated and restarted all hosts")

  gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
  gLogger.notice("XXXXX There were problems in the update process XXXXX")
  gLogger.notice("Succeeded to update:")
  for host in updateSuccess:
    gLogger.notice(" + %s" % host)
  gLogger.notice("Succeeded to restart:")
  for host in restartSuccess:
    gLogger.notice(" + %s" % host)
  gLogger.notice("Failed to update:")
  for host in updateFail:
    gLogger.notice(" - %s" % host)
  gLogger.notice("Failed to restart:")
  for host in restartFail:
    gLogger.notice(" - %s" % host)
  gLogger.notice("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
  return S_ERROR("Update failed!")


if __name__ == "__main__":
  result = updateInstance(version, hosts)
  if not result['OK']:
    gLogger.fatal(result['Message'])
    DIRAC.exit(1)
  gLogger.notice(result['Value'])
  DIRAC.exit(0)
