#!/usr/bin/env python
"""
Command line tool to remove local and remote proxies

Example:
  $ dirac-proxy-destroy -a
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os

import DIRAC
from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Security import Locations, ProxyInfo
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class ProxyDestroy(DIRACScript):
  """
  handles input options for dirac-proxy-destroy
  """

  def initParameters(self):
    """
    creates a Params class with default values
    """
    self.vos = []
    self.delete_all = False
    self.switches = [
        ("a", "all", "Delete the local and all uploaded proxies (the nuclear option)", self.setDeleteAll),
        ("v:", "vo=", "Delete uploaded proxy for vo name given", self.addVO)
    ]

  def addVO(self, voname):
    """
    adds a VO to be deleted from remote proxies
    """
    self.vos.append(voname)
    return S_OK()

  def setDeleteAll(self, _):
    """
    deletes local and remote proxies
    """
    self.delete_all = True
    return S_OK()

  def needsValidProxy(self):
    """
    returns true if any remote operations are required
    """
    return self.vos or self.delete_all

  def getProxyGroups(self):
    """
    Returns a set of all remote proxy groups stored on the dirac server for the user invoking the command.
    """
    proxies = gProxyManager.getUserProxiesInfo()
    if not proxies['OK']:
      raise RuntimeError('Could not retrieve uploaded proxy info.')

    user_groups = set()
    for dn in proxies['Value']:
      dn_groups = set(proxies['Value'][dn].keys())
      user_groups.update(dn_groups)

    return user_groups

  def mapVoToGroups(self, voname):
    """
    Returns all groups available for a given VO as a set.
    """

    vo_dict = Registry.getGroupsForVO(voname)
    if not vo_dict['OK']:
      raise RuntimeError('Could not retrieve groups for vo %s.' % voname)

    return set(vo_dict['Value'])

  def deleteRemoteProxy(self, userdn, vogroup):
    """
    Deletes proxy for a vogroup for the user envoking this function.
    Returns a list of all deleted proxies (if any).
    """
    rpcClient = RPCClient("Framework/ProxyManager")
    retVal = rpcClient.deleteProxyBundle([(userdn, vogroup)])

    if retVal['OK']:
      gLogger.notice('Deleted proxy for %s.' % vogroup)
    else:
      gLogger.error('Failed to delete proxy for %s.' % vogroup)

  def deleteLocalProxy(self, proxyLoc):
    """
    Deletes the local proxy.
    Returns false if no local proxy found.
    """
    try:
      os.unlink(proxyLoc)
    except IOError:
      gLogger.error('IOError: Failed to delete local proxy.')
      return
    except OSError:
      gLogger.error('OSError: Failed to delete local proxy.')
      return
    gLogger.notice('Local proxy deleted.')

  def run(self):
    """
    main program entry point
    """
    self.registerSwitches(self.switches)

    self.parseCommandLine(ignoreErrors=True)

    if self.delete_all and self.vos:
      gLogger.error("-a and -v options are mutually exclusive. Please pick one or the other.")
      return 1

    proxyLoc = Locations.getDefaultProxyLocation()

    if not os.path.exists(proxyLoc):
      gLogger.error("No local proxy found in %s, exiting." % proxyLoc)
      return 1

    result = ProxyInfo.getProxyInfo(proxyLoc, True)
    if not result['OK']:
      raise RuntimeError('Failed to get local proxy info.')

    if result['Value']['secondsLeft'] < 60 and self.needsValidProxy():
      raise RuntimeError('Lifetime of local proxy too short, please renew proxy.')

    userDN = result['Value']['identity']

    if self.delete_all:
      # delete remote proxies
      remote_groups = self.getProxyGroups()
      if not remote_groups:
        gLogger.notice('No remote proxies found.')
      for vo_group in remote_groups:
        self.deleteRemoteProxy(userDN, vo_group)
      # delete local proxy
      self.deleteLocalProxy(proxyLoc)
    elif self.vos:
      vo_groups = set()
      for voname in self.vos:
        vo_groups.update(self.mapVoToGroups(voname))
      # filter set of all groups to only contain groups for which there is a user proxy
      user_groups = self.getProxyGroups()
      vo_groups.intersection_update(user_groups)
      if not vo_groups:
        gLogger.notice('You have no proxies registered for any of the specified VOs.')
      for group in vo_groups:
        self.deleteRemoteProxy(userDN, group)
    else:
      self.deleteLocalProxy(proxyLoc)

    return 0


@ProxyDestroy()
def main(self):
  try:
    DIRAC.exit(self.run())
  except RuntimeError as rtError:
    gLogger.error('Operation failed: %s' % str(rtError))
  DIRAC.exit(1)


if __name__ == "__main__":
  main()  # pylint: disable=no-value-for-parameter
