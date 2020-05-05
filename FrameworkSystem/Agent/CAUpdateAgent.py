""" CAUpdateAgent is meant to be used in a multi-server installations
    where one server has some machinery of keeping up to date the CA's data
    and other servers are just synchronized with the master one without "official" CA installations locally.

    It's like installing CAs in the pilot in dirac-install but for the servers.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient


class CAUpdateAgent(AgentModule):
  """ just routinely calls BundleDeliveryClient.syncCAs()/syncCRLs()
  """

  def initialize(self):
    self.am_setOption("PollingTime", 3600 * 6)
    return S_OK()

  def execute(self):
    """ The main agent execution method
    """
    bdc = BundleDeliveryClient()
    result = bdc.syncCAs()
    if not result['OK']:
      self.log.error("Error while updating CAs", result['Message'])
    elif result['Value']:
      self.log.info("CAs got updated")
    else:
      self.log.info("CAs are already synchronized")
    result = bdc.syncCRLs()
    if not result['OK']:
      self.log.error("Error while updating CRLs", result['Message'])
    elif result['Value']:
      self.log.info("CRLs got updated")
    else:
      self.log.info("CRLs are already synchronized")

    return S_OK()
