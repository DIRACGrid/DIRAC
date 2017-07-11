"""

DIRAC CertificateMgmt module is used to generate the CAs and revoked certificates

Example:

from DIRAC.Core.Utilities import CertificateMgmt

cl = Elasticsearch( self.__url,
                    timeout = self.__timeout,
                    use_ssl = True,
                    verify_certs = True,
                    ca_certs = CertificateMgmt.generateCAFile() )
                    
or 

sslops = dict( certfile = CertificateMgmt.getCert(/WebApp/HTTPS/Cert),
               keyfile = CertificateMgmt.getCert(/WebApp/HTTPS/Key),
               cert_reqs = ssl.CERT_OPTIONAL,
               ca_certs = CertificateMgmt.generateCAFile('/WebApp/HTTPS/Cert'),
               ssl_version = ssl.PROTOCOL_TLSv1 ) 
...                                  
srv = tornado.httpserver.HTTPServer( self.__app, ssl_options = sslops, xheaders = True )

"""
import os
import tempfile

from DIRAC.Core.Security import Locations, X509Chain, X509CRL
from DIRAC import gLogger, gConfig


def getCert( specificLocation = None ):
  """
  get the host certificate
  
  :param str specificLocation: we can specify the location where the host certificate located. For example: /WebApp/HTTPS/Cert
  
  """
  cert = Locations.getHostCertificateAndKeyLocation()
  if cert:
    cert = cert[0]
  else:
    cert = "/opt/dirac/etc/grid-security/hostcert.pem"
  if specificLocation:
    cert = gConfig.getValue( specificLocation, cert )
  return cert

def getKey( specificLocation = None ):
  key = Locations.getHostCertificateAndKeyLocation()
  if key:
    key = key[1]
  else:
    key = "/opt/dirac/etc/grid-security/hostkey.pem"
  if specificLocation:
    key = gConfig.getValue( specificLocation, key )
  
  return key

def generateCAFile( location = None ):
  """
  
  Generate a single CA file with all the PEMs
  
  :param str location: we can specify a specific location in CS
  :return file cas.pem which contains all certificates
  
  """
  caDir = Locations.getCAsLocation()
  for fn in ( os.path.join( os.path.dirname( caDir ), "cas.pem" ),
              os.path.join( os.path.dirname( getCert( location ) ), "cas.pem" ),
              False ):
    if not fn:
      fn = tempfile.mkstemp( prefix = "cas.", suffix = ".pem" )[1]

    try:

      with open( fn, "w" ) as fd:
        for caFile in os.listdir( caDir ):
          caFile = os.path.join( caDir, caFile )
          result = X509Chain.X509Chain.instanceFromFile( caFile )
          if not result[ 'OK' ]:
            continue
          chain = result[ 'Value' ]
          expired = chain.hasExpired()
          if not expired[ 'OK' ] or expired[ 'Value' ]:
            continue
          fd.write( chain.dumpAllToString()[ 'Value' ] )

      gLogger.info( "CAs used from: %s" % str( fn ) )
      return fn
    except IOError as err:
      gLogger.warn( err )

  return False

def generateRevokedCertsFile( location = None ):
  """
  
  Generate a single CA file with all the PEMs
  
  :param str location: we can specify a specific location in CS
  :return file allRevokedCerts.pem which contains all revoked certificates
  
  """
  caDir = Locations.getCAsLocation()
  for fn in ( os.path.join( os.path.dirname( caDir ), "allRevokedCerts.pem" ),
              os.path.join( os.path.dirname( getCert( location ) ), "allRevokedCerts.pem" ),
              False ):
    if not fn:
      fn = tempfile.mkstemp( prefix = "allRevokedCerts", suffix = ".pem" )[1]
    try:
      fd = open( fn, "w" )
    except IOError:
      continue
    for caFile in os.listdir( caDir ):
      caFile = os.path.join( caDir, caFile )
      result = X509CRL.X509CRL.instanceFromFile( caFile )
      if not result[ 'OK' ]:
        continue
      chain = result[ 'Value' ]    
      fd.write( chain.dumpAllToString()[ 'Value' ] )
    fd.close()
    return fn
  return False
