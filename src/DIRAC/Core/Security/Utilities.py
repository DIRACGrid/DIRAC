"""

This module module is used to generate the CAs and CRLs (revoked certificates)

Example::

  from DIRAC.Core.Security import Utilities

  retVal = Utilities.generateRevokedCertsFile()
  if retVal['OK']:
    cl = Elasticsearch( self.__url,
                        timeout = self.__timeout,
                        use_ssl = True,
                        verify_certs = True,
                        ca_certs = retVal['Value'] )

or::

  retVal = Utilities.generateCAFile('/WebApp/HTTPS/Cert')
  if retVal['OK']:
    sslops = dict( certfile = CertificateMgmt.getCert(/WebApp/HTTPS/Cert),
                   keyfile = CertificateMgmt.getCert(/WebApp/HTTPS/Key),
                   cert_reqs = ssl.CERT_OPTIONAL,
                   ca_certs = retVal['Value'],
                   ssl_version = ssl.PROTOCOL_TLSv1 )

  srv = tornado.httpserver.HTTPServer( self.__app, ssl_options = sslops, xheaders = True )

Note: If you wan to make sure that the CA is up to date, better to use the BundleDeliveryClient.

"""
import os
import tempfile

from DIRAC.Core.Security import X509Chain, X509CRL
from DIRAC.Core.Security import Locations
from DIRAC import gLogger, S_OK, S_ERROR


def generateCAFile(location=None):
    """

    Generate/find a single CA file with all the PEMs

    :param str location: we can specify a specific CS location
                         where it's written a directory where to find the CAs and CRLs
    :return: directory where the file cas.pem which contains all certificates is found/created

    """
    caDir = Locations.getCAsLocation()
    if not caDir:
        return S_ERROR("No CAs dir found")

    # look in what's normally /etc/grid-security/certificates
    if os.path.isfile(os.path.join(os.path.dirname(caDir), "cas.pem")):
        return S_OK(os.path.join(os.path.dirname(caDir), "cas.pem"))

    # look in what's normally /opt/dirac/etc/grid-security
    diracCADirPEM = os.path.join(os.path.dirname(Locations.getHostCertificateAndKeyLocation(location)[0]), "cas.pem")
    if os.path.isfile(diracCADirPEM):
        return S_OK(diracCADirPEM)

    # Now we create it in tmpdir
    fn = tempfile.mkstemp(prefix="cas.", suffix=".pem")[1]
    try:
        with open(fn, "w") as fd:
            for caFile in os.listdir(caDir):
                caFile = os.path.join(caDir, caFile)
                chain = X509Chain.X509Chain()
                result = chain.loadChainFromFile(caFile)
                if not result["OK"]:
                    continue

                expired = chain.hasExpired()
                if not expired["OK"] or expired["Value"]:
                    continue
                fd.write(chain.dumpAllToString()["Value"])

        gLogger.info(f"CAs used from: {str(fn)}")
        return S_OK(fn)
    except OSError as err:
        gLogger.warn(err)

    return S_ERROR("Could not find/generate CAs")


def generateRevokedCertsFile(location=None):
    """

    Generate a single CA file with all the PEMs

    :param str location: we can specify a specific CS location
                         where it's written a directory where to find the CAs and CRLs
    :return: directory where the file crls.pem which contains all CRLs is created

    """
    caDir = Locations.getCAsLocation()
    if not caDir:
        return S_ERROR("No CAs dir found")

    # look in what's normally /etc/grid-security/certificates
    if os.path.isfile(os.path.join(os.path.dirname(caDir), "crls.pem")):
        return S_OK(os.path.join(os.path.dirname(caDir), "crls.pem"))

    # look in what's normally /opt/dirac/etc/grid-security
    diracCADirPEM = os.path.join(os.path.dirname(Locations.getHostCertificateAndKeyLocation(location)[0]), "crls.pem")
    if os.path.isfile(diracCADirPEM):
        return S_OK(diracCADirPEM)

    # Now we create it in tmpdir
    fn = tempfile.mkstemp(prefix="crls", suffix=".pem")[1]
    try:
        with open(fn, "w") as fd:
            for caFile in os.listdir(caDir):
                caFile = os.path.join(caDir, caFile)
                result = X509CRL.X509CRL.instanceFromFile(caFile)
                if not result["OK"]:
                    continue
                chain = result["Value"]
                fd.write(chain.dumpAllToString()["Value"])
            return S_OK(fn)
    except OSError as err:
        gLogger.warn(err)

    return S_ERROR("Could not find/generate CRLs")
