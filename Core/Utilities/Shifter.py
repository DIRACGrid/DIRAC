"""
Handling the download of the shifter Proxy
"""

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import findDefaultGroupForUser,\
    getDNForUsernameInGroup, getVOMSAttributeForGroup


def getShifterProxy(shifterType, fileName=False):
  """ This method returns a shifter's proxy

      :param str shifterType: ProductionManager / DataManager...
      :param str fileName: file name

      :return: S_OK(dict)/S_ERROR()
  """
  if fileName:
    mkDir(os.path.dirname(fileName))
  opsHelper = Operations()
  userName = opsHelper.getValue(cfgPath('Shifter', shifterType, 'User'), '')
  if not userName:
    return S_ERROR("No shifter User defined for %s" % shifterType)
  result = findDefaultGroupForUser(userName)
  if not result['OK']:
    return result
  defaultGroup = result['Value']
  userGroup = opsHelper.getValue(cfgPath('Shifter', shifterType, 'Group'), defaultGroup)
  result = getDNForUsernameInGroup(userName, userGroup)
  if not result['OK']:
    return result
  userDN = result['Value']
  if not userDN:
    return S_ERROR('No user DN found for shifter %s@%s' % (userName, userGroup))
  vomsAttr = getVOMSAttributeForGroup(userGroup)
  if vomsAttr:
    gLogger.info("Getting VOMS [%s] proxy for shifter %s@%s (%s)" % (vomsAttr, userName,
                                                                     userGroup, userDN))
    result = gProxyManager.downloadVOMSProxyToFile(userName, userGroup,
                                                   filePath=fileName,
                                                   requiredTimeLeft=86400,
                                                   cacheTime=86400)
  else:
    gLogger.info("Getting proxy for shifter %s@%s (%s)" % (userName, userGroup, userDN))
    result = gProxyManager.downloadProxyToFile(userName, userGroup,
                                               filePath=fileName,
                                               requiredTimeLeft=86400,
                                               cacheTime=86400)
  if not result['OK']:
    return result
  chain = result['chain']
  fileName = result['Value']
  return S_OK({'DN': userDN,
               'username': userName,
               'group': userGroup,
               'chain': chain,
               'proxyFile': fileName})


def setupShifterProxyInEnv(shifterType, fileName=False):
  """ Return the shifter's proxy and set it up as the default
      proxy via changing the environment.
      This method returns a shifter's proxy

      :param str shifterType: ProductionManager / DataManager...
      :param str fileName: file name

      :return: S_OK(dict)/S_ERROR()
  """
  result = getShifterProxy(shifterType, fileName)
  if not result['OK']:
    return result
  proxyDict = result['Value']
  os.environ['X509_USER_PROXY'] = proxyDict['proxyFile']
  return result
