""" X509CRL is a class for managing X509CRL
This class is used to manage the revoked certificates....
"""
import datetime

from cryptography import x509

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.pyca import asn1_utils
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.File import secureOpenForWrite

# pylint: disable=broad-except


class X509CRL:
    def __init__(self, cert=None):
        self.__pemData = ""

        if cert:
            self.__loadedCert = True
            self.__revokedCert = cert
        else:
            self.__loadedCert = False

    @classmethod
    def instanceFromFile(cls, crlLocation):
        """Instance a X509CRL from a file"""
        crl = cls()
        result = crl.loadCRLFromFile(crlLocation)
        if not result["OK"]:
            return result
        return S_OK(crl)

    def loadCRLFromFile(self, crlLocation):
        """
        Load a x509CRL certificate from a pem file
        Return : S_OK / S_ERROR
        """
        self.__loadedCert = False
        try:
            with open(crlLocation, "rb") as crlFile:
                pemData = crlFile.read()
            self.__revokedCert = x509.load_pem_x509_crl(pemData)
        except Exception as e:
            return S_ERROR(DErrno.ECERTREAD, f"{repr(e).replace(',)', ')')}")
        self.__loadedCert = True
        self.__pemData = pemData.decode("ascii")
        return S_OK()

    def __bytes__(self):
        if not self.__loadedCert:
            return b"No certificate loaded"
        return self.__pemData.encode("ascii")

    def __str__(self):
        return self.__pemData

    def dumpAllToString(self):
        """
        Dump all to string
        """
        if not self.__loadedCert:
            return S_ERROR(DErrno.ECERTREAD, "No certificate loaded")
        return S_OK(self.__pemData)

    def dumpAllToFile(self, filename=False):
        """
        Dump all to file. If no filename specified a temporal one will be created
        """
        if not self.__loadedCert:
            return S_ERROR("No certificate loaded")
        try:
            with secureOpenForWrite(filename) as (fd, filename):
                fd.write(self.__pemData)
        except Exception as e:
            return S_ERROR(DErrno.EWF, f"{filename}: {repr(e).replace(',)', ')')}")
        return S_OK(filename)

    def hasExpired(self):
        if not self.__loadedCert:
            return S_ERROR("No certificate loaded")
        nextUpdate = self.__revokedCert.next_update_utc
        if nextUpdate is None:
            return S_OK(False)
        return S_OK(datetime.datetime.now(datetime.timezone.utc) > nextUpdate)

    def getIssuer(self):
        if not self.__loadedCert:
            return S_ERROR("No certificate loaded")
        return S_OK(asn1_utils.nameToDN(self.__revokedCert.issuer))

    def __repr__(self):
        repStr = "<X509CRL"
        if self.__loadedCert:
            repStr += self.getIssuer().get("Value", "")
        repStr += ">"
        return repStr
