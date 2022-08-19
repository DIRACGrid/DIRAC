""" X509Chain is a class for managing X509 chains with their Pkeys

Link to the RFC 3820: https://tools.ietf.org/html/rfc3820
In particular, limited proxy: https://tools.ietf.org/html/rfc3820#section-3.8

There are also details available about Per-User Sub-Proxies (PUSP)
here: https://wiki.egi.eu/wiki/Usage_of_the_per_user_sub_proxy_in_EGI

"""
import copy
import os
import stat
import tempfile
import hashlib

import re

import M2Crypto


from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.Decorators import executeOnlyIf, deprecated
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.m2crypto import PROXY_OID, LIMITED_PROXY_OID, DIRAC_GROUP_OID, DEFAULT_PROXY_STRENGTH
from DIRAC.Core.Security.m2crypto.X509Certificate import X509Certificate


# Decorator to check that _certList is not empty
needCertList = executeOnlyIf("_certList", S_ERROR(DErrno.ENOCHAIN))
# Decorator to check that the PKey has been loaded
needPKey = executeOnlyIf("_keyObj", S_ERROR(DErrno.ENOPKEY))


class X509Chain:
    """
    An X509Chain is basically a list of X509Certificate object, as well as a PKey object,
    which is associated to the X509Certificate the lowest in the chain.

    This is what you will want to use for user certificate (because they will turn into proxy....), and for
    proxy.

    A priori, once we get rid of pyGSI, we could even meld the X509Certificate into this one, and use the X509Chain
    for host certificates. After all, a certificate is nothing but a chain of length 1...

    There are normally 4 ways you would instanciate an X509Chain object:

    * You are loading a proxy from a file
    * Loading the chain from a file
    * You are getting information about your peer during an SSL connection
    * You are delegating

    Typical usages of X509Chain are illustrated below

    Loading a proxy from a file (this will load the chain and the key, assuming the key is in the same file)::

      proxy = X509Chain()
      res = proxy.loadProxyFromFile(myFile)
      if not res['OK']:
        return res


    Generating a proxy from a Certificate::

      cert = X509Chain()
      # Load user cert
      retVal = cert.loadChainFromFile('/home/chaen/.globus/userkey.pem')
      if not retVal['OK']:
        return retVal
      # Load the key from a different place, with a password
      retVal = cert.loadKeyFromFile('/home/chaen/.globus/userkey.pem', password='MySecretKey')
      if not retVal['OK']:
        return res

      # Generate a limited proxy, valid one hour
      retVal = cert.generateProxyToFile('/tmp/proxy.pem',
                                     3600, # only 1 h
                                     diracGroup = 'lhcb_user',
                                     strength= 2048,
                                     limited=True)


    Getting information from a peer in an SSL Connection::

      # conn is an M2Crypto.SSL.Connection instance
      chain = X509Chain.generateX509ChainFromSSLConnection(conn)
      creds = chain.getCredentials()


    Delegating a proxy to a service::

      # The server side generates a request
      # Equivalent to ProxyManager.requestDelegationUpload

      x509Req = X509Request()
      x509Req.generateProxyRequest()

      # This reqStr object is sent to the client
      reqStr = x509Req.dumpRequest()['Value']

      # This object contains both the public and private key
      pkeyReq = x509Req.getPKey()

      #######################################################

      # The client side signs the request, with its proxy
      # Assume the proxy chain was already loaded one way or the otjer

      # The proxy will contain a "bullshit private key"
      res = proxyChain.generateChainFromRequestString(reqStr, lifetime=lifetime)

      # This is sent back to the server
      delegatedProxyString = res['Value']

      ######################################################
      # Equivalent to ProxyManager.completeDelegationUpload

      # Create the new chain
      # the pkey was generated together with the Request
      delegatedProxy = X509Chain(keyObj=pkeyReq)
      delegatedProxy.loadChainFromString(delegatedProxyString)

      # make sure the public key match between Request and the new Chain
      # (Stupid, of course it will ! But it is done in the ProxyManager...)
      res = x509Req.checkChain(delegatedProxy)

    """

    def __init__(self, certList=False, keyObj=False):
        """
        C'tor

        :param certList: list of X509Certificate to constitute the chain
        :param keyObj: ~M2Crypto.EVP.PKey object. The public or public/private key associated to
                       the last certificate of the chain

        """

        # __isProxy is True if this chain represents a proxy
        self.__isProxy = False
        # Whether the proxy is limited or not
        self.__isLimitedProxy = False

        # This is the position of the first proxy in the chain
        self.__firstProxyStep = 0

        # Cache for sha1 hash of the object
        # This is just used as a unique identifier for
        # indexing in the ProxyCache
        self.__hash = False

        # List of X509Certificate constituing the chain
        # The certificate in position N has been generated from the (N+1)
        self._certList = []

        # Place holder for the EVP.PKey object
        self._keyObj = None

        if certList:
            # copy the content of the list, without copying the objects themselves
            self._certList = copy.copy(certList)
            # Immediately check if it is a proxy
            self.__checkProxyness()

        if keyObj:
            self._keyObj = keyObj

    @classmethod
    @deprecated("Use loadChainFromFile instead", onlyOnce=True)
    def instanceFromFile(cls, chainLocation):
        """Class method to generate a X509Chain from a file

        :param chainLocation: path to the file

        :returns: S_OK(X509Chain)
        """
        chain = cls()
        result = chain.loadChainFromFile(chainLocation)
        if not result["OK"]:
            return result

        return S_OK(chain)

    @staticmethod
    def generateX509ChainFromSSLConnection(sslConnection):
        """Returns an instance of X509Chain from the SSL connection

        :param sslConnection: ~M2Crypto.SSl.Connection instance

        :returns: a X509Chain instance
        """
        certList = []

        certStack = sslConnection.get_peer_cert_chain()
        for cert in certStack:
            certList.append(X509Certificate(x509Obj=cert))

        # Servers don't receive the whole chain, the last cert comes alone
        # if not self.infoDict['clientMode']:
        certList.insert(0, X509Certificate(x509Obj=sslConnection.get_peer_cert()))
        peerChain = X509Chain(certList=certList)

        return peerChain

    def loadChainFromFile(self, chainLocation):
        """
        Load a x509 chain from a pem file

        :param chainLocation: path to the file

        :returns: S_OK/S_ERROR
        """
        try:
            with open(chainLocation) as fd:
                pemData = fd.read()
        except OSError as e:
            return S_ERROR(DErrno.EOF, "{}: {}".format(chainLocation, repr(e).replace(",)", ")")))
        return self.loadChainFromString(pemData)

    def loadChainFromString(self, data):
        """
        Load a x509 cert from a string containing the pem data

        :param data: data representing the chain of certificate in the

        Return : S_OK / S_ERROR
        """
        try:
            self._certList = self.__certListFromPemString(data)
        except Exception as e:
            return S_ERROR(DErrno.ECERTREAD, "%s" % repr(e).replace(",)", ")"))

        if not self._certList:
            return S_ERROR(DErrno.EX509)

        # Update internals
        self.__checkProxyness()
        return S_OK()

    @staticmethod
    def __certListFromPemString(certString):
        """
        Create certificates list from string. String should contain certificates, just like plain text proxy file.
        """
        # To get list of X509 certificates (not X509 Certificate Chain) from string it has to be parsed like that
        # (constructors are not able to deal with big string)
        certList = []
        pattern = r"(-----BEGIN CERTIFICATE-----((.|\n)*?)-----END CERTIFICATE-----)"
        for cert in re.findall(pattern, certString):
            certList.append(X509Certificate(certString=cert[0]))
        return certList

    # Not used in m2crypto version
    # def setChain(self, certList):
    #   """
    #   Set the chain
    #   Return : S_OK / S_ERROR
    #   """
    #   self._certList = certList
    #   self.__loadedChain = True
    #   return S_OK()

    def loadKeyFromFile(self, chainLocation, password=False):
        """
        Load a PKey from a pem file

        :param chainLocation: path to the file
        :param password: password to decode the file.

        :returns: S_OK / S_ERROR
        """
        try:
            with open(chainLocation) as fd:
                pemData = fd.read()
        except Exception as e:
            return S_ERROR(DErrno.EOF, "{}: {}".format(chainLocation, repr(e).replace(",)", ")")))
        return self.loadKeyFromString(pemData, password)

    def loadKeyFromString(self, pemData, password=False):
        """
        Load a PKey from a string containing the pem data

        :param pemData: pem data of the key, potentially encoded with the password
        :param password: password to decode the file.

        :returns: S_OK / S_ERROR
        """
        self._keyObj = None
        if not isinstance(pemData, bytes):
            pemData = pemData.encode("ascii")
        if password:
            password = password.encode()
        try:
            self._keyObj = M2Crypto.EVP.load_key_string(pemData, lambda x: password)
        except Exception as e:
            return S_ERROR(DErrno.ECERTREAD, "%s (Probably bad pass phrase?)" % repr(e).replace(",)", ")"))

        return S_OK()

    def setPKey(self, pkeyObj):
        """
        Set the chain
        Return : S_OK / S_ERROR
        """
        self._keyObj = pkeyObj
        return S_OK()

    def loadProxyFromFile(self, chainLocation):
        """
        Load a Proxy from a pem file, that is both the Cert chain and the PKey

        :param chainLocation: path to the proxy file

        :returns: S_OK  / S_ERROR
        """
        try:
            with open(chainLocation) as fd:
                pemData = fd.read()
        except Exception as e:
            return S_ERROR(DErrno.EOF, "{}: {}".format(chainLocation, repr(e).replace(",)", ")")))
        return self.loadProxyFromString(pemData)

    def loadProxyFromString(self, pemData):
        """
        Load a Proxy from a pem buffer, that is both the Cert chain and the PKey

        :param pemData: PEM encoded cert chain and pkey

        :returns: S_OK / S_ERROR
        """
        retVal = self.loadChainFromString(pemData)
        if not retVal["OK"]:
            return retVal

        return self.loadKeyFromString(pemData)

    @staticmethod
    def __getProxyExtensionList(diracGroup=False, rfcLimited=False):
        """
        Get an extension stack containing the necessary extension for a proxy.
        Basically the keyUsage, the proxyCertInfo, and eventually the diracGroup

        :param diracGroup: name of the dirac group for the proxy
        :param rfcLimited: boolean to generate for a limited proxy

        :returns: M2Crypto.X509.X509_Extension_Stack object.
        """

        extStack = M2Crypto.X509.X509_Extension_Stack()

        # Standard certificate extensions
        kUext = M2Crypto.X509.new_extension(
            "keyUsage", "digitalSignature, keyEncipherment, dataEncipherment", critical=1
        )
        extStack.push(kUext)

        # Mandatory extension to be a proxy
        policyOID = LIMITED_PROXY_OID if rfcLimited else PROXY_OID
        ext = M2Crypto.X509.new_extension("proxyCertInfo", "critical, language:%s" % (policyOID), critical=1)
        extStack.push(ext)

        # Add a dirac group
        if diracGroup and isinstance(diracGroup, str):
            # the str cast is needed because M2Crypto does not play it cool with unicode here it seems
            # Also one needs to specify the ASN1 type. That's what it is...
            dGext = M2Crypto.X509.new_extension(DIRAC_GROUP_OID, str("ASN1:IA5:%s" % diracGroup))
            extStack.push(dGext)

        return extStack

    @needCertList
    def getCertInChain(self, certPos=0):
        """
        Get then a certificate in the chain

        :warning: Contrary to the pygsi version, this is not a copy!

        :param certPos: position of the certificate in the chain. Default: 0

        :returns: S_OK(X509Certificate)/S_ERROR
        """
        return S_OK(self._certList[certPos])

    @needCertList
    def getIssuerCert(self):
        """
        Returns the issuer certificate of the last one if it is a proxy, otherwise
        the last one in the chain

        :returns: S_OK(X509Certificate)/S_ERROR
        """
        if self.__isProxy:
            return S_OK(self._certList[self.__firstProxyStep + 1])
        return S_OK(self._certList[-1])

    @deprecated("Only here for compatibility reason", onlyOnce=True)
    @needPKey
    def getPKeyObj(self):
        """
        Get the pkey obj

        :returns: ~M2Crypto.EVP.PKey object
        """
        return S_OK(self._keyObj)

    @deprecated("Only here for compatibility reason")
    @needCertList
    def getCertList(self):
        """
        Get the cert list
        """
        return S_OK(self._certList)

    @needCertList
    def getNumCertsInChain(self):
        """
        length of the certificate chain

        :returns: length of the certificate chain


        """
        return S_OK(len(self._certList))

    # pylint: disable=unused-argument
    @needCertList
    @needPKey
    def generateProxyToString(
        self, lifetime, diracGroup=False, strength=DEFAULT_PROXY_STRENGTH, limited=False, proxyKey=False
    ):
        """
        Generate a proxy and get it as a string.

        Check here: https://github.com/eventbrite/m2crypto/blob/master/demo/x509/ca.py#L45

        Args:
            lifetime (int): expected lifetime in seconds of proxy
            diracGroup (str): diracGroup to add to the certificate
            strength (int): length in bits of the pair if proxyKey not given (default 2048)
            limited (bool): Create a limited proxy (default False)
            proxyKey: M2Crypto.EVP.PKey instance with private and public key. If not given, generate one
            rfc: placeholder for backward compatibility and ignored

        :returns: S_OK(PEM encoded string), S_ERROR. The PEM string contains all the certificates in the chain
                  and the private key associated to the last X509Certificate just generated.
        """

        issuerCert = self._certList[0]

        if not proxyKey:
            # Generating key is a two step process: create key object and then assign RSA key.
            # This contains both the private and public key
            proxyKey = M2Crypto.EVP.PKey()
            proxyKey.assign_rsa(M2Crypto.RSA.gen_key(strength, 65537, callback=M2Crypto.util.quiet_genparam_callback))

        # Generate a new X509Certificate object
        proxyExtensions = self.__getProxyExtensionList(diracGroup, limited)
        res = X509Certificate.generateProxyCertFromIssuer(issuerCert, proxyExtensions, proxyKey, lifetime=lifetime)
        if not res["OK"]:
            return res
        proxyCert = res["Value"]

        # Sign it with one owns key
        proxyCert.sign(self._keyObj, "sha256")

        # Generate the proxy string
        proxyString = "{}{}".format(
            proxyCert.asPem(),
            proxyKey.as_pem(cipher=None, callback=M2Crypto.util.no_passphrase_callback).decode("ascii"),
        )
        for i in range(len(self._certList)):
            crt = self._certList[i]
            proxyString += crt.asPem()
        return S_OK(proxyString)

    # pylint: disable=unused-argument
    def generateProxyToFile(self, filePath, lifetime, diracGroup=False, strength=DEFAULT_PROXY_STRENGTH, limited=False):
        """
        Generate a proxy and put it into a file

        Args:
            filePath: file to write
            lifetime: expected lifetime in seconds of proxy
            diracGroup: diracGroup to add to the certificate
            strength: length in bits of the pair
            limited: Create a limited proxy
            rfc: placeholder and ignored
        """
        retVal = self.generateProxyToString(lifetime, diracGroup, strength, limited)
        if not retVal["OK"]:
            return retVal
        try:
            with open(filePath, "w") as fd:
                fd.write(retVal["Value"])
        except Exception as e:
            return S_ERROR(DErrno.EWF, "{} :{}".format(filePath, repr(e).replace(",)", ")")))
        try:
            os.chmod(filePath, stat.S_IRUSR | stat.S_IWUSR)
        except Exception as e:
            return S_ERROR(DErrno.ESPF, "{} :{}".format(filePath, repr(e).replace(",)", ")")))
        return S_OK()

    @needCertList
    def isProxy(self):
        """
         Check whether this chain is a proxy

        :returns: S_OK(boolean)
        """
        return S_OK(self.__isProxy)

    @needCertList
    def isLimitedProxy(self):
        """
        Check whether this chain is a limited proxy

        :returns: S_OK(boolean)
        """
        return S_OK(self.__isProxy and self.__isLimitedProxy)

    @needCertList
    def isValidProxy(self, ignoreDefault=False):
        """
        Check whether this chain is a valid proxy, that is:
          * a proxy
          * still valid
          * with a valid group

        :param ignoreDefault: (what a stupid name) if True, do not lookup the CS

        :returns: S_OK(True) if the proxy is valid, S_ERROR otherwise

        """
        if not self.__isProxy:
            return S_ERROR(DErrno.ENOCHAIN, "Chain is not a proxy")

        if self.hasExpired()["Value"]:
            return S_ERROR(DErrno.ENOCHAIN)

        if ignoreDefault:
            groupRes = self.getDIRACGroup(ignoreDefault=True)
            if not groupRes["OK"]:
                return groupRes
            if not groupRes["Value"]:
                return S_ERROR(DErrno.ENOGROUP)

        return S_OK(True)

    def isVOMS(self):
        """
        Check whether this proxy contains VOMS extensions.
        It is enough for one of the certificate of the chain to have VOMS extension

        :returns: S_OK(boolean)
        """

        if not self.__isProxy:
            return S_OK(False)

        for cert in self._certList:
            if cert.hasVOMSExtensions()["Value"]:
                return S_OK(True)
        return S_OK(False)

    def isRFC(self):
        """Check whether this is an RFC proxy. It can only be true, providing it is a proxy

        :returns: S_OK(boolean)
        """

        return self.isProxy()

    def getVOMSData(self):
        """
        Returns the voms data.

        :returns: See :py:func:`~DIRAC.Core.Security.m2crypto.X509Certificate.getVOMSData`
                  If no VOMS data is available, return DErrno.EVOMS
        :warning: In case the chain is not a proxy, this method will return False.
                  Yes, it's stupid, but it is for compatibility...

        """
        if not self.__isProxy:
            return S_OK(False)

        for cert in self._certList:
            res = cert.getVOMSData()
            if res["OK"]:
                return res
        return S_ERROR(DErrno.EVOMS)

    def __checkProxyness(self):
        """This method is called upon initialization of a chain and fill in some internal attributes.

        Pure madness...

        To me, this method just seems to work by pure luck..
        """

        self.__hash = False
        self.__firstProxyStep = len(self._certList) - 2  # -1 is user cert by default, -2 is first proxy step
        self.__isProxy = True
        self.__isLimitedProxy = False
        prevDNMatch = 2
        # If less than 2 steps in the chain is no proxy
        if len(self._certList) < 2:
            self.__isProxy = False
            return

        # Here we make sure that each certificate in the chain was
        # signed by the previous one
        for step in range(len(self._certList) - 1):

            # this is a cryptographic check with the keys
            issuerMatch = self.__checkIssuer(step, step + 1)
            if not issuerMatch:
                self.__isProxy = False
                return

            # Do we need to check the proxy DN?
            if prevDNMatch:
                dnMatch = self.__checkProxyDN(step, step + 1)
                if dnMatch == 0:
                    # If we are not in the first step we've found the entity cert
                    if step > 0:
                        self.__firstProxyStep = step - 1
                    # If we are in the first step this is not a proxy
                    else:
                        self.__isProxy = False
                        return
                # Limited proxy DN match
                elif dnMatch == 2:
                    self.__isLimitedProxy = True
                    if prevDNMatch != 2:
                        self.__isProxy = False
                        self.__isLimitedProxy = False
                        return
                prevDNMatch = dnMatch

    def __checkProxyDN(self, certStep, issuerStep):
        """
        Checks that the subject of the proxy is properly derived from the issuer subject.

        Args:
            certStep: position of the certificate to check in self.__certList
            issuerStep: position of the issuer certificate to check in self.__certList

        :returns: an int based on the match:
                  0 = no match
                  1 = proxy match
                  2 = limited proxy match
        """

        issuerSubject = self._certList[issuerStep].getSubjectNameObject()
        if not issuerSubject["OK"]:
            return 0
        issuerSubject = issuerSubject["Value"]

        proxySubject = self._certList[certStep].getSubjectNameObject()
        if not proxySubject["OK"]:
            return 0
        proxySubject = proxySubject["Value"]

        lastEntry = str(proxySubject).split("/")[-1].split("=")
        limited = False
        if lastEntry[0] != "CN":
            return 0

        # For non-RFC proxy, the proxy always had these two strings in the CN
        if lastEntry[1] not in ("proxy", "limited proxy"):
            # for RFC proxy, one has to check the extension.
            ext = self._certList[certStep].getExtension("proxyCertInfo")
            if not ext["OK"]:
                return 0

            ext = ext["Value"]

            # Check the RFC
            contraint = [
                line.split(":")[1].strip()
                for line in ext.get_value().split("\n")
                if line.split(":")[0] == "Policy Language"
            ]
            if not contraint:
                return 0
            if contraint[0] == LIMITED_PROXY_OID:
                limited = True
        else:
            if lastEntry[1] == "limited proxy":
                limited = True
        if not str(issuerSubject) == str(proxySubject)[: str(proxySubject).rfind("/")]:
            return 0
        return 1 if not limited else 2

    def __checkIssuer(self, certStep, issuerStep):
        """
        Check that the issuer has signed the certificate with his private key

        :param certStep: position of the certificate in self.__certList
        :param issuerStep: position of the issuer certificate

        :returns: S_OK(boolean)

        """
        issuerCert = self._certList[issuerStep]
        cert = self._certList[certStep]
        pubKey = issuerCert.getPublicKey()["Value"]

        return cert.verify(pubKey)["Value"]

    @needCertList
    def getDIRACGroup(self, ignoreDefault=False):
        """
        Retrieve the dirac group of the chain

        :param ignoreDefault: (default False) if True, do not lookup the CS for a group if it is not in the proxy

        :returns: S_OK(dirac group)/S_ERROR
        """
        if not self.__isProxy:
            return S_ERROR(DErrno.EX509, "Chain does not contain a valid proxy")

        # If it is a PUSP, we do a lookup based on the certificate
        # (you can't do a PUSP out of a proxy)
        if self.isPUSP()["Value"]:
            return self._certList[self.__firstProxyStep - 2].getDIRACGroup(ignoreDefault=ignoreDefault)

        # The code below will find the first match of the DIRAC group
        for cert in reversed(self._certList):
            # We specifically say we do not want the default to first check inside the proxy
            retVal = cert.getDIRACGroup(ignoreDefault=True)
            if retVal["OK"] and "Value" in retVal and retVal["Value"]:
                return retVal

        # No DIRAC group found, try to get the default one
        return self.getCertInChain(self.__firstProxyStep)["Value"].getDIRACGroup(ignoreDefault=ignoreDefault)

    @needCertList
    def hasExpired(self):
        """
        Check whether any element of the chain has expired

        :returns: S_OK(boolean)
        """
        for cert in reversed(self._certList):
            res = cert.hasExpired()
            if not res["OK"]:
                return res
            # If True, it means the cert has expired
            if res["Value"]:
                return S_OK(True)

        return S_OK(False)

    @needCertList
    def getNotAfterDate(self):
        """
        Get the smallest not after date

        :returns: S_OK(datetime.datetime)
        """
        notAfter = self._certList[0].getNotAfterDate()
        if not notAfter["OK"]:
            return notAfter
        notAfter = notAfter["Value"]

        for cert in reversed(self._certList):
            res = cert.getNotAfterDate()
            if not res["OK"]:
                return res
            stepNotAfter = res["Value"]

            # If the current cert has already expired
            # we return this as notAfter date
            res = cert.hasExpired()
            if not res["OK"]:
                return res
            if res["Value"]:
                return S_OK(stepNotAfter)

            # if the current cert has a shorter lifetime
            # as the current reference, take it as new reference
            notAfter = min(notAfter, stepNotAfter)

        return S_OK(notAfter)

    @needCertList
    def generateProxyRequest(self, bitStrength=DEFAULT_PROXY_STRENGTH, limited=False):
        """
        Generate a proxy request.
        See :py:meth:`DIRAC.Core.Security.m2crypto.X509Certificate.X509Certificate.generateProxyRequest`

        Return S_OK( X509Request ) / S_ERROR
        """

        # We use the first certificate of the chain to do the proxy request
        x509 = self._certList[0]
        return x509.generateProxyRequest(bitStrength, limited)

    @needCertList
    def getStrength(self):
        """
        Returns the strength in bit of the key of the first certificate in the chain
        """
        x509 = self._certList[0]
        return x509.getStrength()

    @needCertList
    @needPKey
    def generateChainFromRequestString(self, pemData, lifetime=86400, requireLimited=False, diracGroup=False):
        """
        Generate a x509 chain from a request.

        :param pemData: PEM encoded request
        :param lifetime: lifetime of the delegated proxy in seconds (default 1 day)
        :param requireLimited: if True, requires a limited proxy
        :param diracGroup: DIRAC group to put in the proxy
        :param rfc: placeholder for compatibility, ignored

        :returns: S_OK( X509 chain pem encoded string ) / S_ERROR. The new chain will have been signed
                  with the public key included in the request

        """
        try:
            req = M2Crypto.X509.load_request_string(pemData, format=M2Crypto.X509.FORMAT_PEM)

        except Exception as e:
            return S_ERROR(DErrno.ECERTREAD, "Can't load request data: %s" % repr(e).replace(",)", ")"))

        # I am not sure this test makes sense.
        # You can't request a limit proxy if you are yourself not limited ?!
        # I think it should be a "or" instead of "and"
        limited = requireLimited and self.isLimitedProxy().get("Value", False)
        return self.generateProxyToString(lifetime, diracGroup, None, limited, req.get_pubkey())

    @needCertList
    def getRemainingSecs(self):
        """
        Get remaining time (minimum of all cert in the chain)

        :returns: S_OK(time left in seconds)
        """
        remainingSecs = self.getCertInChain(0)["Value"].getRemainingSecs()["Value"]
        for cert in self._certList[1:]:
            stepRS = cert.getRemainingSecs()["Value"]
            remainingSecs = min(remainingSecs, stepRS)

        return S_OK(remainingSecs)

    @needCertList
    def dumpAllToString(self):
        """
        Dump the current chain as a PEM encoded string
        The order would be:

          * first certificate
          * private key (without passphrase)
          * other certificates

        :returns: S_OK(PEM encoded chain with private key)
        """
        data = self._certList[0].asPem()
        if self._keyObj:
            data += self._keyObj.as_pem(cipher=None, callback=M2Crypto.util.no_passphrase_callback).decode("ascii")
        for cert in self._certList[1:]:
            data += cert.asPem()
        return S_OK(data)

    def dumpAllToFile(self, filename=False):
        """
        Dump all to file.

        :param filename: If not specified, a temporary one will be created

        :returns: S_OK(filename)/S_ERROR
        """
        retVal = self.dumpAllToString()
        if not retVal["OK"]:
            return retVal
        pemData = retVal["Value"]
        try:
            if not filename:
                fd, filename = tempfile.mkstemp()
                os.close(fd)
            with open(filename, "w") as fp:
                fp.write(pemData)
        except Exception as e:
            return S_ERROR(DErrno.EWF, "{} :{}".format(filename, repr(e).replace(",)", ")")))
        try:
            os.chmod(filename, stat.S_IRUSR | stat.S_IWUSR)
        except Exception as e:
            return S_ERROR(DErrno.ESPF, "{} :{}".format(filename, repr(e).replace(",)", ")")))
        return S_OK(filename)

    @needCertList
    def dumpChainToString(self):
        """
        Dump only cert chain to string, without the PKey

        :returns: S_OK(pem chain)
        """
        return S_OK("".join(cert.asPem() for cert in self._certList))

    @needPKey
    def dumpPKeyToString(self):
        """
        Dump only the key to string, not encoded

        :returns: S_OK(PEM encoded key)

        """
        return S_OK(self._keyObj.as_pem(cipher=None, callback=M2Crypto.util.no_passphrase_callback).decode("ascii"))

    def __str__(self):
        """String representation"""
        repStr = "<X509Chain"
        if self._certList:
            repStr += " %s certs " % len(self._certList)
            for cert in self._certList:
                repStr += "[%s]" % str(cert.getSubjectDN()["Value"])
        if self._keyObj:
            repStr += " with key"
        repStr += ">"
        return repStr

    def __repr__(self):
        """Object representation"""
        return self.__str__()

    def isPUSP(self):
        """Checks whether the current chain is a PUSP

        :returns: S_OK(boolean).
                  If True, the S_OK structure is enriched with:
                  * Indentity: the DN
                  * SubProxyUser: name of the user
        """
        if self.__isProxy:
            # Check if we have a subproxy
            dn = self._certList[self.__firstProxyStep].getSubjectDN()
            if not dn["OK"]:
                return dn
            dn = dn["Value"]

            subproxyUser = isPUSPdn(dn)
            if subproxyUser:
                result = S_OK(True)
                result["Identity"] = dn
                result["SubproxyUser"] = subproxyUser
                return result

        return S_OK(False)

    @needCertList
    def getCredentials(self, ignoreDefault=False, withRegistryInfo=True):
        """Returns a summary of the credentials contained in the current chain

        :params ignoreDefault: (default False) If True and if no DIRAC group is found in the proxy, lookup the CS
        :params withRegistryInfo: (default True) if set to True, will enhance the returned dict with info
                                  from the registry


        :returns: S_OK with the credential dict. Some parameters of the dict are always there, other depends
                on the nature of the Chain

                Always present:
                  * subject: str. The last DN in the chain
                  * issuer: str. The issuer of the last cert in the chain
                  * secondsLeft: validity of the chain in seconds (see :py:meth:`.getRemainingSecs`)
                  * isProxy: boolean (see :py:meth:`.isProxy`)
                  * isLimitedProxy: boolean (see :py:meth:`.isLimitedProxy`)
                  * validDN: boolean if the DN is known to DIRAC
                  * validGroup: False (see further definition)
                  * DN: either the DN of the host, or the DN of the user corresponding to the proxy


                Only for proxy:
                  * identity: If it is a normal proxy, it is the DN of the certificate.
                              If it is a PUSP, it contains the identity as in :py:meth:`.isPUSP`
                  * username: DIRAC username associated to the DN (needs withRegistryInfo)
                              (see :py:func:`DIRAC.ConfigurationSystem.Client.Helpers.Registry.getUsernameForDN`)
                  * group: DIRAC group, depending on ignoreDefault param(see :py:meth:`.getDIRACGroup`)
                  * validGroup: True if the group found is in the list of groups the user belongs to
                  * groupProperty: (only if validGroup) get the properties of the group

                For Host certificate (needs withRegistryInfo):
                  * group: always `hosts`
                  * hostname: name of the host as registered in the CS
                             (see :py:func:`DIRAC.ConfigurationSystem.Client.Helpers.Registry.getHostnameForDN`)
                  * validGroup: True
                  * groupProperties: host options
                                    (see :py:func:`DIRAC.ConfigurationSystem.Client.Helpers.Registry.getHostOption`)

                If it is a user certificate (needs withRegistryInfo):
                  * username: like for proxy
                  * validDN: like proxy
        """
        credDict = {
            "subject": str(self._certList[0].getSubjectDN()["Value"]),  # ['Value'] :(
            "issuer": self._certList[0].getIssuerDN()["Value"],  # ['Value'] :(
            "secondsLeft": self.getRemainingSecs()["Value"],
            "isProxy": self.__isProxy,
            "isLimitedProxy": self.__isProxy and self.__isLimitedProxy,
            "validDN": False,
            "validGroup": False,
        }

        # Add the DN entry as the subject.
        credDict["DN"] = credDict["subject"]
        if self.__isProxy:
            credDict["identity"] = str(
                self._certList[self.__firstProxyStep + 1].getSubjectDN()["Value"]
            )  # ['Value'] :(
            # if the chain is a proxy, then the DN we want to work with is the real one of the
            # user, not the one of his proxy
            credDict["DN"] = credDict["identity"]

            # Check if we have the PUSP case
            result = self.isPUSP()
            if result["OK"] and result["Value"]:
                credDict["identity"] = result["Identity"]
                credDict["subproxyUser"] = result["SubproxyUser"]

            if withRegistryInfo:
                retVal = Registry.getUsernameForDN(credDict["identity"])
                if not retVal["OK"]:
                    return S_OK(credDict)
                credDict["username"] = retVal["Value"]
                credDict["validDN"] = True
            retVal = self.getDIRACGroup(ignoreDefault=ignoreDefault)
            if retVal["OK"]:
                diracGroup = retVal["Value"]
                credDict["group"] = diracGroup
                if withRegistryInfo:
                    retVal = Registry.getGroupsForUser(credDict["username"])
                    if retVal["OK"] and diracGroup in retVal["Value"]:
                        credDict["validGroup"] = True
                        credDict["groupProperties"] = Registry.getPropertiesForGroup(diracGroup)
        elif withRegistryInfo:
            retVal = Registry.getHostnameForDN(credDict["subject"])
            if retVal["OK"]:
                credDict["group"] = "hosts"
                credDict["hostname"] = retVal["Value"]
                credDict["validDN"] = True
                credDict["validGroup"] = True
                credDict["groupProperties"] = Registry.getHostOption(credDict["hostname"], "Properties")

            retVal = Registry.getUsernameForDN(credDict["subject"])
            if retVal["OK"]:
                credDict["username"] = retVal["Value"]
                credDict["validDN"] = True
        return S_OK(credDict)

    @needCertList
    def hash(self):
        """Get a hash of the chain
        In practice, this is only used to index the chain in a DictCache

        :returns: S_OK(string hash)
        """
        if self.__hash:
            return S_OK(self.__hash)
        sha1 = hashlib.sha1()
        for cert in self._certList:
            sha1.update(str(cert.getSubjectNameObject()["Value"]).encode())
        sha1.update(str(self.getRemainingSecs()["Value"] / 3600).encode())
        sha1.update(self.getDIRACGroup()["Value"].encode())
        if self.isVOMS():
            sha1.update(b"VOMS")
            from DIRAC.Core.Security.VOMS import VOMS

            result = VOMS().getVOMSAttributes(self)
            if result["OK"]:
                for attribute in result["Value"]:
                    sha1.update(attribute.encode())
        self.__hash = sha1.hexdigest()
        return S_OK(self.__hash)


def isPUSPdn(userDN):
    """Evaluate if the DN is of the PUSP type or not

    :param str userDN: user DN string

    :returns: the subproxy user name or None
    """
    lastEntry = userDN.split("/")[-1].split("=")
    if lastEntry[0] == "CN" and lastEntry[1].startswith("user:"):
        return userDN.split("/")[-1].split(":")[1]
    return None
