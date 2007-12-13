########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/Attic/ProxyRenewalAgent.py,v 1.1 2007/12/13 15:01:09 atsareg Exp $
########################################################################

"""  Proxy Renewal agent is the key element of the Proxy Repository
     which maintains the user proxies alive
"""

from DIRAC.Core.Base.Agent import Agent
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB import ProxyRepositoryDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

AGENT_NAME = 'WorkloadManagement/ProxyRenewal'

class ProxyRenewal(Agent):

  def __init__(self):
    """ Standard constructor
    """

    Agent.__init__(self,AGENT_NAME)

  def initialize(self):

    result = Agent.initialize(self)
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    self.proxyDB = ProxyRepositoryDB()

    self.minValidity = gConfig.getValue(self.section+'/MinValidity',12)

    return S_OK()

  def execute(self):
    """ The main agent execution method
    """

    result = self.proxyDB(validity=self.minValidity)
    if not result["OK"]:
      self.log.error("Failed to acces Proxy Repository Database",result['Message'])
      return S_ERROR("Failed to acces Proxy Repository Database")

    ticket_dn_list = result['Value']
    self.log.verbose("Proxies stored in repository with validity less than %s minutes:" % self.validity)
    for dn in ticket_dn_list:
      self.log.verbose(dn)

    result = self.jobDB.getDistinctJobAttributes("OwnerDN")
    if not result["OK"]:
      return S_ERROR("Can not get existing job owner DNs")

    job_dn_list = result['Value']
    for dn in ticket_dn_list:
      if dn in job_dn_list:
        self.log.verbose("Renewing proxy for "+dn)
        result = self.proxyDB.getProxy(dn)
        if not result["OK"]:
          self.log.warn('Can not get ticket for '+dn)
          self.log.warn(result['Message'])
          continue
        ticket = result['Value']
        result = getProxyTimeLeft(ticket)
        if result["OK"]:
          time_left = int(result["Value"])
          if time_left <= 0:

            # Removing of expired proxies
            self.log.info("Proxy has expired and will be deleted for %s" % dn)
            result = self.proxyDB.removeProxy(dn)
            if result["OK"]:
              self.log.info('Proxy removed for '+dn)
            else:
              self.log.error('Failed to remove proxy for '+dn)
          else:
            result = renewVOMSProxy(ticket,
                                    self.validity_period,
                                    server_key=self.server_key,
                                    server_cert=self.server_cert
                                    )
            if result["OK"]:
              new_proxy = result['Value']
              resTime = getProxyTimeLeft(new_proxy)
              if resTime['OK']:
                tleft = resTime['Value']
                result = self.proxyDB.storeProxy(new_proxy,dn,tleft)
                if result["OK"]:
                  self.log.verbose('Proxy extended for '+dn)
                else:
                  self.log.warn('Failed to store the renewed proxy')
              else:
                self.log.warn('Failed to determine the new proxy time left')
            else:
              self.log.warn(result["Message"])
              self.log.warn('Failed to get proxy delegation for '+dn)
        else:
          self.log.verbose("Removing proxy for "+dn)
          result = self.db.removeProxy(dn)
          if result["OK"]:
            self.log.verbose('Proxy removed for '+dn)
          else:
            self.log.warn(result["Message"])
            self.log.warn('Failed to remove proxy for '+dn)
