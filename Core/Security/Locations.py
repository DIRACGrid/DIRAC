# $HeadURL$
__RCSID__ = "$Id$"

import os
import DIRAC
from DIRAC import gConfig
g_SecurityConfPath = "/DIRAC/Security"

def getProxyLocation():
  """ Get the path of the currently active grid proxy file
  """

  for envVar in [ 'GRID_PROXY_FILE', 'X509_USER_PROXY' ]:
    if os.environ.has_key( envVar ):
      proxyPath = os.path.realpath( os.environ[ envVar ] )
      if os.path.isfile( proxyPath ):
        return proxyPath
  #/tmp/x509up_u<uid>
  proxyName = "x509up_u%d" % os.getuid()
  if os.path.isfile( "/tmp/%s" % proxyName ):
    return "/tmp/%s" % proxyName

  #No gridproxy found
  return False

#Retrieve CA's location
def getCAsLocation():
  """ Retrieve the CA's files location
  """
  #Grid-Security
  retVal = gConfig.getOption( '%s/Grid-Security' % g_SecurityConfPath )
  if retVal[ 'OK' ]:
    casPath = "%s/certificates" % retVal[ 'Value' ]
    if os.path.isdir( casPath ):
      return casPath
  #CAPath
  retVal = gConfig.getOption( '%s/CALocation' % g_SecurityConfPath )
  if retVal[ 'OK' ]:
    casPath = retVal[ 'Value' ]
    if os.path.isdir( casPath ):
      return casPath
  # Look up the X509_CERT_DIR environment variable
  if os.environ.has_key( 'X509_CERT_DIR' ):
    casPath = os.environ[ 'X509_CERT_DIR' ]
    return casPath
  #rootPath./etc/grid-security/certificates
  casPath = "%s/etc/grid-security/certificates" % DIRAC.rootPath
  if os.path.isdir( casPath ):
    return casPath
  #/etc/grid-security/certificates
  casPath = "/etc/grid-security/certificates"
  if os.path.isdir( casPath ):
    return casPath
  #No CA's location found
  return False

#Retrieve CA's location
def getCAsDefaultLocation():
  """ Retrievethe CAs Location inside DIRAC etc directory
  """
  #rootPath./etc/grid-security/certificates
  casPath = "%s/etc/grid-security/certificates" % DIRAC.rootPath
  return casPath

#TODO: Static depending on files specified on CS
#Retrieve certificate
def getHostCertificateAndKeyLocation():
  """ Retrieve the host certificate files location
  """

  fileDict = {}
  for fileType in ( "cert", "key" ):
    #Direct file in config
    retVal = gConfig.getOption( '%s/%sFile' % ( g_SecurityConfPath, fileType.capitalize() ) )
    if retVal[ 'OK' ]:
      fileDict[ fileType ] = retVal[ 'Value' ]
      continue
    fileFound = False
    for filePrefix in ( "server", "host", "dirac", "service" ):
      #Possible grid-security's
      paths = []
      retVal = gConfig.getOption( '%s/Grid-Security' % g_SecurityConfPath )
      if retVal[ 'OK' ]:
        paths.append( retVal[ 'Value' ] )
      paths.append( "%s/etc/grid-security/" % DIRAC.rootPath )
      #paths.append( os.path.expanduser( "~/.globus" ) )
      for path in paths:
        filePath = os.path.realpath( "%s/%s%s.pem" % ( path, filePrefix, fileType ) )
        if os.path.isfile( filePath ):
          fileDict[ fileType ] = filePath
          fileFound = True
          break
      if fileFound:
        break
  if "cert" not in fileDict.keys() or "key" not in fileDict.keys():
    return False
  return ( fileDict[ "cert" ], fileDict[ "key" ] )

def getCertificateAndKeyLocation():
  """ Get the locations of the user X509 certificate and key pem files
  """

  certfile = ''
  if os.environ.has_key( "X509_USER_CERT" ):
    if os.path.exists( os.environ["X509_USER_CERT"] ):
      certfile = os.environ["X509_USER_CERT"]
  if not certfile:
    if os.path.exists( os.environ["HOME"] + '/.globus/usercert.pem' ):
      certfile = os.environ["HOME"] + '/.globus/usercert.pem'

  if not certfile:
    return False

  keyfile = ''
  if os.environ.has_key( "X509_USER_KEY" ):
    if os.path.exists( os.environ["X509_USER_KEY"] ):
      keyfile = os.environ["X509_USER_KEY"]
  if not keyfile:
    if os.path.exists( os.environ["HOME"] + '/.globus/userkey.pem' ):
      keyfile = os.environ["HOME"] + '/.globus/userkey.pem'

  if not keyfile:
    return False

  return ( certfile, keyfile )

def getDefaultProxyLocation():
  """ Get the location of a possible new grid proxy file
  """

  for envVar in [ 'GRID_PROXY_FILE', 'X509_USER_PROXY' ]:
    if os.environ.has_key( envVar ):
      proxyPath = os.path.realpath( os.environ[ envVar ] )
      return proxyPath

  #/tmp/x509up_u<uid>
  proxyName = "x509up_u%d" % os.getuid()
  return "/tmp/%s" % proxyName
