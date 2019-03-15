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

  Generate a single CA file with all the PEMs

  :param str location: we can specify a specific location in CS
  :return: file cas.pem which contains all certificates

  """
  caDir = Locations.getCAsLocation()
  for fn in (os.path.join(os.path.dirname(caDir), "cas.pem"),
             os.path.join(os.path.dirname(Locations.getHostCertificateAndKeyLocation(location)[0]), "cas.pem"),
             False):
    if not fn:
      fn = tempfile.mkstemp(prefix="cas.", suffix=".pem")[1]

    try:

      with open(fn, "w") as fd:
        for caFile in os.listdir(caDir):
          caFile = os.path.join(caDir, caFile)
          chain = X509Chain.X509Chain()
          result = chain.loadChainFromFile(caFile)
          if not result['OK']:
            continue

          expired = chain.hasExpired()
          if not expired['OK'] or expired['Value']:
            continue
          fd.write(chain.dumpAllToString()['Value'])

      gLogger.info("CAs used from: %s" % str(fn))
      return S_OK(fn)
    except IOError as err:
      gLogger.warn(err)

  return S_ERROR(caDir)


def generateRevokedCertsFile(location=None):
  """

  Generate a single CA file with all the PEMs

  :param str location: we can specify a specific location in CS
  :return: file crls.pem which contains all revoked certificates

  """
  caDir = Locations.getCAsLocation()
  for fn in (os.path.join(os.path.dirname(caDir), "crls.pem"),
             os.path.join(os.path.dirname(Locations.getHostCertificateAndKeyLocation(location)[0]), "crls.pem"),
             False):
    if not fn:
      fn = tempfile.mkstemp(prefix="crls", suffix=".pem")[1]
    try:
      with open(fn, "w") as fd:
        for caFile in os.listdir(caDir):
          caFile = os.path.join(caDir, caFile)
          result = X509CRL.X509CRL.instanceFromFile(caFile)
          if not result['OK']:
            continue
          chain = result['Value']
          fd.write(chain.dumpAllToString()['Value'])
        return S_OK(fn)
    except IOError:
      continue

  return S_ERROR(caDir)
