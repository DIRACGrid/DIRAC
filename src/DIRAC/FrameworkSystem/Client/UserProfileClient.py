from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities import DEncode


class UserProfileClient:
    def __init__(self, profile, rpcClientFunctor=Client):
        self.rpcClientFunctor = rpcClientFunctor
        self.profile = profile

    def __getRPCClient(self):
        return self.rpcClientFunctor(url="Framework/UserProfileManager")

    def storeVar(self, varName, data, perms={}):
        try:
            stub = DEncode.encode(data).decode()
        except Exception as e:
            return S_ERROR("Cannot encode data:%s" % str(e))
        return self.__getRPCClient().storeProfileVar(self.profile, varName, stub, perms)

    def __decodeVar(self, data):
        try:
            dataObj, lenData = DEncode.decode(data.encode())
        except Exception as e:
            return S_ERROR("Cannot decode data: %s" % str(e))
        return S_OK(dataObj)

    def retrieveVar(self, varName):
        rpcClient = self.__getRPCClient()
        result = rpcClient.retrieveProfileVar(self.profile, varName)
        if not result["OK"]:
            return result
        return self.__decodeVar(result["Value"])

    def retrieveVarFromUser(self, ownerName, ownerGroup, varName):
        rpcClient = self.__getRPCClient()
        result = rpcClient.retrieveProfileVarFromUser(ownerName, ownerGroup, self.profile, varName)
        if not result["OK"]:
            return result
        return self.__decodeVar(result["Value"])

    def retrieveAllVars(self):
        rpcClient = self.__getRPCClient()
        result = rpcClient.retrieveProfileAllVars(self.profile)
        if not result["OK"]:
            return result
        try:
            encodedData = result["Value"]
            dataObj = {key: DEncode.decode(value.encode())[0] for key, value in encodedData.items()}
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

    def getUserProfileNames(self, permission=dict()):
        """
        it returns the available profile names by not taking account the permission: ReadAccess and PublishAccess
        """
        return self.__getRPCClient().getUserProfileNames(permission)

    def listStatesForWeb(self, permission={}):
        return self.__getRPCClient().listStatesForWeb(permission)
