""" Base class for DIRAC Client """
########################################################################
# $Id: Client.py 18427 2009-11-20 10:28:53Z acsmith $
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/Base/Client.py $
########################################################################
__RCSID__ = "$Id: Client.py 19179 2009-12-04 09:54:19Z acsmith $"

from DIRAC.Core.DISET.RPCClient                     import RPCClient
import types

class Client:
  """ Simple class to redirect unknown actions directly to the server. 
      - The self.serverURL member should be set by the inheriting class
  """
  def __getattr__(self, name):
    self.call = name
    return self.executeRPC

  def executeRPC(self, *parms, **kws):
    toExecute = self.call
    # Check whether 'rpc' keyword is specified
    oRPC = False
    if kws.has_key('rpc'):
      oRPC = kws['rpc']
      del kws['rpc']
    # Check whether the 'timeout' keyword is specified
    timeout=120
    if kws.has_key('timeout'):
      oRPC = kws['timeout']
      del kws['timeout']
    # Check whether the 'url' keyword is specified
    url = ''
    if kws.has_key('url'):
      url = kws['url']
      del kws['url']
    # Create the RPCClient
    rpcClient = self.__getRPC(oRPC, url, timeout)
    # Execute the method
    return eval("rpcClient.%s(*parms,**kws)" % toExecute)

  def __getRPC(self,oRPC=False,url='',timeout=120):
    if not oRPC:
      if not url:
        url = self.serverURL
      oRPC = RPCClient(self.serverURL,timeout=timeout)
    return oRPC