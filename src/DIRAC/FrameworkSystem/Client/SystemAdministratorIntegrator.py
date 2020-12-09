""" The SystemAdministratorIntegrator is a class integrating access to all the
    SystemAdministrator services configured in the system
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
import DIRAC.ConfigurationSystem.Client.Helpers.Registry as Registry

SYSADMIN_PORT = 9162


class SystemAdministratorIntegrator(object):

  def __init__(self, **kwargs):
    """ Constructor
    """
    if 'hosts' in kwargs:
      self.__hosts = kwargs['hosts']
      del kwargs['hosts']
    else:
      result = Registry.getHosts()
      if result['OK']:
        self.__hosts = result['Value']
      else:
        self.__hosts = []
      # Excluded hosts
      if 'exclude' in kwargs:
        self.__hosts = list(set(self.__hosts) - set(kwargs['exclude']))

    # Ping the hosts to remove those that don't have a SystemAdministrator service
    sysAdminHosts = []
    self.silentHosts = []
    self.__resultDict = {}
    self.__kwargs = {}
    pool = ThreadPool(len(self.__hosts))
    for host in self.__hosts:
      pool.generateJobAndQueueIt(self.__executeClient,
                                 args=[host, "ping"],
                                 kwargs={},
                                 oCallback=self.__processResult)

    pool.processAllResults()
    for host, result in self.__resultDict.items():
      if result['OK']:
        sysAdminHosts.append(host)
      else:
        self.silentHosts.append(host)
    del pool

    self.__hosts = sysAdminHosts

    self.__kwargs = dict(kwargs)
    self.__pool = ThreadPool(len(self.__hosts))
    self.__resultDict = {}

  def getSilentHosts(self):
    """ Get a list of non-responding hosts

    :return: list of hosts
    """
    return self.silentHosts

  def getRespondingHosts(self):
    """ Get a list of responding hosts

    :return: list of hosts
    """
    return self.__hosts

  def __getattr__(self, name):
    self.call = name
    return self.execute

  def __executeClient(self, host, method, *parms, **kwargs):
    """ Execute RPC method on a given host
    """
    hostName = Registry.getHostOption(host, 'Host', host)
    client = SystemAdministratorClient(hostName, **self.__kwargs)
    result = getattr(client, method)(*parms, **kwargs)
    result['Host'] = host
    return result

  def __processResult(self, id_, result):
    """ Collect results in the final structure
    """
    host = result['Host']
    del result['Host']
    self.__resultDict[host] = result

  def execute(self, *args, **kwargs):
    """ Main execution method
    """
    self.__resultDict = {}
    for host in self.__hosts:
      self.__pool.generateJobAndQueueIt(self.__executeClient,
                                        args=[host, self.call] + list(args),
                                        kwargs=kwargs,
                                        oCallback=self.__processResult)

    self.__pool.processAllResults()
    return S_OK(self.__resultDict)
