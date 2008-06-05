
import os
import GSI

requiredGSIVersion = "0.3.4"
if GSI.version.__version__ < requiredGSIVersion:
  raise Exception( "pyGSI is not the latest version (installed %s required %s)" % ( GSI.version.__version__, requiredGSIVersion ) )

GSI.SSL.set_thread_safe()

nid = GSI.crypto.create_oid( "1.2.42.42", "diracGroup", "DIRAC group" )
GSI.crypto.add_x509_extension_alias( nid, 78 ) #Alias to netscape comment, text based extension
nid = GSI.crypto.create_oid( "1.3.6.1.4.1.8005.100.100.5", "voms", "VOMS extension" )
GSI.crypto.add_x509_extension_alias( nid, 78 ) #Alias to netscape comment, text based extension

import DIRAC
from DIRAC import gConfig, gLogger

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
    gLogger.debug( "Trying %s for CAs" % casPath )
    if os.path.isdir( casPath ):
      gLogger.debug( "Using %s/Grid-Security + /certificates as location for CA's" % g_SecurityConfPath )
      return casPath
  #CAPath
  retVal = gConfig.getOption( '%s/CALocation' % g_SecurityConfPath )
  if retVal[ 'OK' ]:
    casPath = retVal[ 'Value' ]
    gLogger.debug( "Trying %s for CAs" % casPath )
    if os.path.isdir( casPath ):
      gLogger.debug( "Using %s/CALocation as location for CA's" % g_SecurityConfPath )
      return casPath
  # Look up the X509_CERT_DIR environment variable
  if os.environ.has_key( 'X509_CERT_DIR' ):
    gLogger.debug( "Using X509_CERT_DIR env var as location for CA's" )
    casPath = os.environ[ 'X509_CERT_DIR' ]
    return casPath
  #rootPath./etc/grid-security/certificates
  casPath = "%s/etc/grid-security/certificates" % DIRAC.rootPath
  gLogger.debug( "Trying %s for CAs" % casPath )
  if os.path.isdir( casPath ):
    gLogger.debug( "Using <DIRACRoot>/etc/grid-security/certificates as location for CA's" )
    return casPath
  #/etc/grid-security/certificates
  casPath = "/etc/grid-security/certificates"
  gLogger.debug( "Trying %s for CAs" % casPath )
  if os.path.isdir( casPath ):
    gLogger.debug( "Using autodiscovered %s location for CA's" % casPath )
    return casPath
  #No CA's location found
  return False

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
      gLogger.debug( 'Using %s/%sFile' % ( g_SecurityConfPath, fileType.capitalize() ) )
      fileDict[ fileType ] = retVal[ 'Value' ]
      continue
    else:
      gLogger.debug( '%s/%sFile is not defined' % ( g_SecurityConfPath, fileType.capitalize() ) )
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
        gLogger.debug( "Trying %s for %s file" % ( filePath, fileType ) )
        if os.path.isfile( filePath ):
          gLogger.debug( "Using %s for %s" % ( filePath, fileType ) )
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
  if os.environ.has_key("X509_USER_CERT"):
    if os.path.exists(os.environ["X509_USER_CERT"]):
      certfile = os.environ["X509_USER_CERT"]
  if not certfile:
    if os.path.exists(os.environ["HOME"]+'/.globus/usercert.pem'):
      certfile = os.environ["HOME"]+'/.globus/usercert.pem'

  if not certfile:
    return False

  keyfile = ''
  if os.environ.has_key("X509_USER_KEY"):
    if os.path.exists(os.environ["X509_USER_KEY"]):
      keyfile = os.environ["X509_USER_KEY"]
  if not keyfile:
    if os.path.exists(os.environ["HOME"]+'/.globus/userkey.pem'):
      keyfile = os.environ["HOME"]+'/.globus/userkey.pem'

  if not keyfile:
     return False

  return (certfile,keyfile)

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

from DIRAC.Core.Security.X509Certificate import X509Certificate
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.Core.Security.X509Request import X509Request

g_X509ChainType = type( X509Chain )