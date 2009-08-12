########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Agent/CAUpdateAgent.py,v 1.1 2009/08/12 15:03:05 acasajus Exp $
########################################################################

"""  Proxy Renewal agent is the key element of the Proxy Repository
     which maintains the user proxies alive
"""

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.BundleDeliveryClient import BundleDeliveryClient

class CAUpdateAgent(AgentModule):

  def initialize(self):
    self.am_setOption( "PollingTime", 3600*6 )
    return S_OK()

  def execute(self):
    """ The main agent execution method
    """
    bdc = BundleDeliveryClient()
    result = bdc.syncCAs()
    if not result[ 'OK' ]:
      self.log.error( "Error while updating CAs", result[ 'Message' ] )
    elif result[ 'Value' ]:
      self.log.info( "CAs got updated" )
    else:
      self.log.info( "CAs are already synchronized" )
    result = bdc.syncCRLs()
    if not result[ 'OK' ]:
      self.log.error( "Error while updating CRLs", result[ 'Message' ] )
    elif result[ 'Value' ]:
      self.log.info( "CRLs got updated" )
    else:
      self.log.info( "CRLs are already synchronized" )

    return S_OK()
