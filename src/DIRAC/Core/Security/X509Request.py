""" X509Request is a class for managing X509 requests with their Pkeys.
It's main use is for proxy delegation.
"""
import re

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import DEFAULT_PROXY_STRENGTH
from DIRAC.Core.Security import asn1_utils
from DIRAC.Core.Security.X509Chain import _PEM_KEY_PATTERN, dumpPrivateKeyPEM
from DIRAC.Core.Utilities import DErrno

# pylint: disable=broad-except


class X509Request:
    """
    Class representing X509 Certificate Request. it is used for delegation.
    Please see :ref:`about_proxies` for detailed explanations on delegation,
    and :py:class:`DIRAC.Core.Security.X509Chain` for code examples.

    """

    def __init__(self, reqObj=None, pkeyObj=None):
        """C'tor

        :param reqObj: cryptography.x509.CertificateSigningRequest object. Never used. Shall be removed
        :param pkeyObj: cryptography private key object. Never used. Shall be removed
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
        self.__pkeyObj = rsa.generate_private_key(public_exponent=65537, key_size=bitStrength)

        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "limited proxy" if limited else "proxy")])
        self.__reqObj = (
            x509.CertificateSigningRequestBuilder().subject_name(subject).sign(self.__pkeyObj, hashes.SHA256())
        )
        self.__valid = True

    def dumpRequest(self):
        """
        Get the request as a string

        :returns: S_OK(pem encoded request)
        """
        if not self.__valid:
            return S_ERROR(DErrno.ENOCERT)
        try:
            reqStr = self.__reqObj.public_bytes(serialization.Encoding.PEM).decode("ascii")
        except Exception as e:
            return S_ERROR(DErrno.EX509, f"Can't serialize request: {e}")
        return S_OK(reqStr)

    def getPKey(self):
        """
        Get PKey Internal

        :returns: cryptography private key object
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
            pkeyStr = dumpPrivateKeyPEM(self.__pkeyObj)
        except Exception as e:
            return S_ERROR(DErrno.EX509, f"Can't serialize pkey: {e}")
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
            return S_ERROR(DErrno.EX509, f"Can't serialize request: {req['Message']}")
        if not pkey["OK"]:
            return S_ERROR(DErrno.EX509, f"Can't serialize pkey: {pkey['Message']}")
        return S_OK(f"{req['Value']}{pkey['Value']}")

    def loadAllFromString(self, pemData):
        """load the Request and key argument from a PEM encoded string.

        :param pemData: PEM encoded string containing Request and PKey

        :returns: S_OK()
        """
        if isinstance(pemData, bytes):
            pemData = pemData.decode("ascii")
        try:
            self.__reqObj = x509.load_pem_x509_csr(pemData.encode("ascii"))
        except Exception as e:
            return S_ERROR(DErrno.ENOCERT, str(e))
        try:
            keyBlocks = re.findall(_PEM_KEY_PATTERN, pemData)
            if not keyBlocks:
                raise ValueError("No private key found in the pem data")
            self.__pkeyObj = serialization.load_pem_private_key(keyBlocks[0].encode("ascii"), password=None)
        except Exception as e:
            return S_ERROR(DErrno.ENOPKEY, str(e))
        self.__valid = True
        return S_OK()

    def getSubjectDN(self):
        """
        Get subject DN of the request as a string

        :return: S_OK( string )/S_ERROR
        """
        if not self.__valid:
            return S_ERROR(DErrno.ENOCERT)
        return S_OK(asn1_utils.nameToDN(self.__reqObj.subject))

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

        chainPubKey = chainPubKey["Value"].public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        reqPubKey = self.__reqObj.public_key().public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        if not chainPubKey == reqPubKey:
            return S_ERROR(DErrno.EX509, "Public keys do not match")

        return S_OK(True)

    def getStrength(self):
        """
        Get the length of the key of the request in bit

        :returns: S_OK( size )/S_ERROR
        """

        try:
            return S_OK(self.__pkeyObj.key_size)
        except Exception as e:
            return S_ERROR(f"Cannot get request strength: {e}")
