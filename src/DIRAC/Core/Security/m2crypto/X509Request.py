""" X509Request is a class for managing X509 requests with their Pkeys.
It's main use is for proxy delegation.
"""
import M2Crypto
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.m2crypto import DEFAULT_PROXY_STRENGTH
from DIRAC.Core.Utilities import DErrno

# from DIRAC.Core.Security.m2crypto.X509Chain import X509Chain  # pylint: disable=import-error

# pylint: disable=broad-except


class X509Request:
    """
    Class representing X509 Certificate Request. it is used for delegation.
    Please see :ref:`about_proxies` for detailed explanations on delegation,
    and :py:class:`DIRAC.Core.Security.m2crypto.X509Chain` for code examples.

    """

    def __init__(self, reqObj=None, pkeyObj=None):
        """C'tor

        :param reqObj: M2Crypto.X509.Request object. Never used. Shall be removed
        :param pkeyObj: M2Crypto.EVP.PKey() object. Never used. Shall be removed
        """
        self.__valid = False
        self.__reqObj = reqObj
        self.__pkeyObj = pkeyObj
        if reqObj and pkeyObj:  # isn't it a bit too liberal?
            self.__valid = True

    def generateProxyRequest(self, bitStrength=DEFAULT_PROXY_STRENGTH, limited=False):
        """
        Initialize the Request object as well as the PKey.

        :param bitStrength: (default 2048) length of the key generated
        :param limited: (default False) If True, request is done for a limited proxy
        """
        # self.__pkeyObj is both the public and private key
        self.__pkeyObj = M2Crypto.EVP.PKey()
        self.__pkeyObj.assign_rsa(
            M2Crypto.RSA.gen_key(bitStrength, 65537, callback=M2Crypto.util.quiet_genparam_callback)
        )
        self.__reqObj = M2Crypto.X509.Request()
        self.__reqObj.set_pubkey(self.__pkeyObj)

        if limited:
            self.__reqObj.get_subject().add_entry_by_txt(
                field="CN", type=M2Crypto.ASN1.MBSTRING_ASC, entry="limited proxy", len=-1, loc=-1, set=0
            )
        else:
            self.__reqObj.get_subject().add_entry_by_txt(
                field="CN", type=M2Crypto.ASN1.MBSTRING_ASC, entry="proxy", len=-1, loc=-1, set=0
            )
        self.__reqObj.sign(self.__pkeyObj, "sha256")
        self.__valid = True

    def dumpRequest(self):
        """
        Get the request as a string

        :returns: S_OK(pem encoded request)
        """
        if not self.__valid:
            return S_ERROR(DErrno.ENOCERT)
        try:
            reqStr = self.__reqObj.as_pem().decode("ascii")
        except Exception as e:
            return S_ERROR(DErrno.EX509, "Can't serialize request: %s" % e)
        return S_OK(reqStr)

    # def getRequestObject(self):
    #   """
    #   Get internal X509Request object
    #   Not used
    #   """
    #   return S_OK(self.__reqObj)

    def getPKey(self):
        """
        Get PKey Internal

        :returns: M2Crypto.EVP.PKEY object
        """
        return self.__pkeyObj

    def dumpPKey(self):
        """
        Get the private as a string

        :returns: S_OK(PEM encoded PKey)
        """
        if not self.__valid:
            return S_ERROR(DErrno.ENOCERT)
        try:
            pkeyStr = self.__pkeyObj.as_pem(cipher=None, callback=M2Crypto.util.no_passphrase_callback).decode("ascii")
        except Exception as e:
            return S_ERROR(DErrno.EX509, "Can't serialize pkey: %s" % e)
        return S_OK(pkeyStr)

    def dumpAll(self):
        """
        Dump the Request and the PKey into a string

        :returns: S_OK(PEM encoded req + key), S_ERROR
        """

        if not self.__valid:
            return S_ERROR(DErrno.ENOCERT)

        req = self.dumpRequest()
        pkey = self.dumpPKey()
        if not req["OK"]:
            return S_ERROR(DErrno.EX509, "Can't serialize request: %s" % req["Message"])
        if not pkey["OK"]:
            return S_ERROR(DErrno.EX509, "Can't serialize pkey: %s" % pkey["Message"])
        return S_OK("{}{}".format(req["Value"], pkey["Value"]))

    def loadAllFromString(self, pemData):
        """load the Request and key argument from a PEM encoded string.

        :param pemData: PEN encoded string containing Request and PKey

        :returns: S_OK()
        """
        if not isinstance(pemData, bytes):
            pemData = pemData.encode("ascii")
        try:
            self.__reqObj = M2Crypto.X509.load_request_string(pemData)
        except Exception as e:
            return S_ERROR(DErrno.ENOCERT, str(e))
        try:
            self.__pkeyObj = M2Crypto.EVP.load_key_string(pemData)
        except Exception as e:
            return S_ERROR(DErrno.ENOPKEY, str(e))
        self.__valid = True
        return S_OK()

    # def generateChainFromResponse(self, pemData):
    #   """
    #   Generate a X509 Chain from the pkey and the pem data passed as the argument
    #   Return : S_OK( X509Chain ) / S_ERROR
    #   """
    #   if not self.__valid:
    #     return S_ERROR(DErrno.ENOCERT)
    #   chain = X509Chain()
    #   ret = chain.loadChainFromString(pemData)
    #   if not ret['OK']:
    #     return ret
    #   ret = chain.setPKey(self.__pkeyObj)
    #   if not ret['OK']:
    #     return ret
    #   return chain

    def getSubjectDN(self):
        """
        Get subject DN of the request as a string

        :return: S_OK( string )/S_ERROR
        """
        if not self.__valid:
            return S_ERROR(DErrno.ENOCERT)
        return S_OK(str(self.__reqObj.get_subject()))

    def checkChain(self, chain):
        """
        Check that the public keys of the chain and the request match.

        :param chain: :py:class:`X509Chain` object
        """

        if not self.__valid:
            return S_ERROR(DErrno.ENOCERT)
        retVal = chain.getCertInChain()
        if not retVal["OK"]:
            return retVal
        lastCert = retVal["Value"]
        chainPubKey = lastCert.getPublicKey()
        if not chainPubKey["OK"]:
            return chainPubKey

        # as_der will dump public key info, while as_pem
        # dumps private key.
        chainPubKey = chainPubKey["Value"].as_der()
        reqPubKey = self.__reqObj.get_pubkey().as_der()
        if not chainPubKey == reqPubKey:
            return S_ERROR(DErrno.EX509, "Public keys do not match")

        return S_OK(True)

    def getStrength(self):
        """
        Get the length of the key of the request in bit

        :returns: S_OK( size )/S_ERROR
        """

        try:
            return S_OK(self.__pkeyObj.size() * 8)
        except Exception as e:
            return S_ERROR("Cannot get request strength: %s" % e)
