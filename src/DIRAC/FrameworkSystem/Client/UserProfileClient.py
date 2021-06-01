from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import re
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities import DEncode, Time
from DIRAC.Core.Utilities.Decorators import deprecated


class UserProfileClient(object):

  def __init__(self, profile, rpcClientFunctor=False):
    if rpcClientFunctor:
      self.rpcClientFunctor = rpcClientFunctor
    else:
      self.rpcClientFunctor = RPCClient
    self.profile = profile

  def __getRPCClient(self):
    return self.rpcClientFunctor("Framework/UserProfileManager")

  def __generateTypeDest(self, dataObj):
    if isinstance(dataObj, bool):
      return "b"
    if dataObj is None:
      return "o"
    if isinstance(dataObj, six.integer_types + (float,)):
      return "n"
    if isinstance(dataObj, six.string_types):
      return "s"
    # Not even trying here...
    if isinstance(dataObj, Time._allTypes):
      return "t"
    if isinstance(dataObj, (list, tuple)):
      return "l%se" % "".join([self.__generateTypeDest(so) for so in dataObj])
    if isinstance(dataObj, dict):
      return "d%se" % "".join(["%s%s" % (self.__generateTypeDest(k),
                                         self.__generateTypeDest(dataObj[k])) for k in dataObj])
    return ""

  @deprecated("Unused, will be removed in v7r3")
  def checkTypeRe(self, dataObj, typeRE):
    if typeRE[0] != "^":
      typeRE = "^%s" % typeRE
    if typeRE[-1] != "$":
      typeRE = "%s$" % typeRE
    typeDesc = self.__generateTypeDest(dataObj)
    if not re.match(typeRE, typeDesc):
      return S_ERROR("Stored data does not match typeRE: %s vs %s" % (typeDesc, typeRE))
    return S_OK()

  def storeVar(self, varName, data, perms={}):
    try:
      stub = DEncode.encode(data)
    except Exception as e:
      return S_ERROR("Cannot encode data:%s" % str(e))
    return self.__getRPCClient().storeProfileVar(self.profile, varName, stub, perms)

  def __decodeVar(self, data, dataTypeRE):
    try:
      dataObj, lenData = DEncode.decode(data.encode())
    except Exception as e:
      return S_ERROR("Cannot decode data: %s" % str(e))
    if dataTypeRE:
      result = self.checkTypeRe(dataObj, dataTypeRE)
      if not result['OK']:
        return result
    return S_OK(dataObj)

  def retrieveVar(self, varName, dataTypeRE=False):
    rpcClient = self.__getRPCClient()
    result = rpcClient.retrieveProfileVar(self.profile, varName)
    if not result['OK']:
      return result
    return self.__decodeVar(result['Value'], dataTypeRE)

  def retrieveVarFromUser(self, ownerName, ownerGroup, varName, dataTypeRE=False):
    rpcClient = self.__getRPCClient()
    result = rpcClient.retrieveProfileVarFromUser(ownerName, ownerGroup, self.profile, varName)
    if not result['OK']:
      return result
    return self.__decodeVar(result['Value'], dataTypeRE)

  def retrieveAllVars(self):
    rpcClient = self.__getRPCClient()
    result = rpcClient.retrieveProfileAllVars(self.profile)
    if not result['OK']:
      return result
    try:
      encodedData = result['Value']
      dataObj = {
          key: DEncode.decode(value.encode())[0]
          for key, value in encodedData.items()
      }
    except Exception as e:
      return S_ERROR("Cannot decode data: %s" % str(e))
    return S_OK(dataObj)

  def listAvailableVars(self, filterDict={}):
    rpcClient = self.__getRPCClient()
    return rpcClient.listAvailableProfileVars(self.profile, filterDict)

  def getUserProfiles(self):
    rpcClient = self.__getRPCClient()
    return rpcClient.getUserProfiles()

  def setVarPermissions(self, varName, perms):
    rpcClient = self.__getRPCClient()
    return rpcClient.setProfileVarPermissions(self.profile, varName, perms)

  def getVarPermissions(self, varName):
    rpcClient = self.__getRPCClient()
    return rpcClient.getProfileVarPermissions(self.profile, varName)

  def deleteVar(self, varName):
    return self.__getRPCClient().deleteProfileVar(self.profile, varName)

  def deleteProfiles(self, userList):
    return self.__getRPCClient().deleteProfiles(userList)

  @deprecated("Unused, will be removed in v7r3")
  def storeHashTag(self, tagName):
    return self.__getRPCClient().storeHashTag(tagName)

  @deprecated("Unused, will be removed in v7r3")
  def retrieveHashTag(self, hashTag):
    return self.__getRPCClient().retrieveHashTag(hashTag)

  @deprecated("Unused, will be removed in v7r3")
  def retrieveAllHashTags(self):
    return self.__getRPCClient().retrieveAllHashTags()

  def getUserProfileNames(self, permission=dict()):
    """
    it returns the available profile names by not taking account the permission: ReadAccess and PublishAccess
    """
    return self.__getRPCClient().getUserProfileNames(permission)

  def listStatesForWeb(self, permission={}):
    return self.__getRPCClient().listStatesForWeb(permission)
