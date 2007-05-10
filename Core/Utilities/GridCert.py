# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Attic/GridCert.py,v 1.4 2007/05/10 18:44:58 acasajus Exp $
__RCSID__ = "$Id: GridCert.py,v 1.4 2007/05/10 18:44:58 acasajus Exp $"

import os
import os.path
import threading
import socket
import time

import DIRAC
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger

securityConfPath = "/DIRAC/Security"

# Retrieve grid proxy location
def getGridProxy():
  #UserProxy
  retVal = gConfig.getOption( '%s/UserProxy' % securityConfPath )
  if retVal[ 'OK' ]:
    filePath = os.path.realpath( retVal[ 'Value' ] )
    if os.path.isfile( filePath ):
      gLogger.verbose( "Using %s/UserProxy value for grid proxy" % securityConfPath )
      return retVal[ 'Value' ]
  #UserProxyPath
  proxyName = "x509up_u%d" % os.getuid()
  retVal = gConfig.getOption( '%s/UserProxyPath' % securityConfPath )
  if retVal[ 'OK' ]:
    for proxyPath in [ "%s/%s" % ( retVal[ 'Value' ], proxyName ), "%s/tmp/%s" % ( retVal[ 'Value' ], proxyName ) ]:
      proxyPath = os.path.realpath( proxyPath )
      if os.path.isfile( proxyPath ):
        gLogger.verbose( "Using %s/UserProxyPath value for grid proxy (%s)" % ( securityConfPath, proxyPath ) )
        return proxyPath
  #Environment vars
  for envVar in [ 'GRID_PROXY_FILE', 'X509_USER_PROXY' ]:
    if os.environ.has_key( envVar ):
      proxyPath = os.path.realpath( os.environ[ envVar ] )
      if os.path.isfile( proxyPath ):
        gLogger.verbose( "Using %s env var for grid proxy" % proxyPath )
        return proxyPath
  #/tmp/x509up_u<uid>
  if os.path.isfile( "/tmp/%s" % proxyName ):
    gLogger.verbose( "Using auto-discovered proxy in /tmp/%s" % proxyName )
    return "/tmp/%s" % sGridProxyName
  #No gridproxy found
  raise Exception( "No grid proxy found." )

#Retrieve CA's location
def getCAsLocation():
  #Grid-Security
  retVal = gConfig.getOption( '%s/Grid-Security' % securityConfPath )
  if retVal[ 'OK' ]:
    casPath = "%s/certificates" % retVal[ 'Value' ]
    gLogger.debug( "Trying %s for CAs" % casPath )
    if os.path.isdir( casPath ):
      gLogger.verbose( "Using %s/Grid-Security + /certificates as location for CA's" % securityConfPath )
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
  raise Exception( "No CA's location found" )

#TODO: Static depending on files specified on CS
#Retrieve certificate
def getCertificateAndKey():
  fileDict = {}
  for fileType in ( "cert", "key" ):
    #Direct file in config
    retVal = gConfig.getOption( '%s/%sFile' % ( securityConfPath, fileType.capitalize() ) )
    if retVal[ 'OK' ]:
      gLogger.verbose( 'Using %s/%sFile' % ( securityConfPath, fileType.capitalize() ) )
      fileDict[ fileType ] = retVal[ 'Value' ]
      continue
    else:
      gLogger.debug( '%s/%sFile is not defined' % ( securityConfPath, fileType.capitalize() ) )
    fileFound = False
    for filePrefix in ( "server", "host", "dirac", "service" ):
      #Possible grid-security's
      paths = []
      retVal = gConfig.getOption( '%s/Grid-Security' % securityConfPath )
      if retVal[ 'OK' ]:
        paths.append( retVal[ 'Value' ] )
      paths.append( "%s/etc/grid-security/" % DIRAC.rootPath )
      #paths.append( os.path.expanduser( "~/.globus" ) )
      for path in paths:
        filePath = os.path.realpath( "%s/%s%s.pem" % ( path, filePrefix, fileType ) )
        gLogger.debug( "Trying %s for %s file" % ( filePath, fileType ) )
        if os.path.isfile( filePath ):
          gLogger.verbose( "Using %s for %s" % ( filePath, fileType ) )
          fileDict[ fileType ] = filePath
          fileFound = True
          break
      if fileFound:
        break
  if "cert" not in fileDict.keys() or "key" not in fileDict.keys():
    raise Exception( "No certificate or key found" )
  return ( fileDict[ "cert" ], fileDict[ "key" ] )
