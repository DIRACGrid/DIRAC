# $HeadURL$
"""
   Collection of DIRAC useful network related modules
   by default on Error they return None

   getAllInterfaces and getAddressFromInterface do not work in MAC
"""
__RCSID__ = "$Id$"

import socket
import struct
import array
import os
import fcntl
import platform
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

def discoverInterfaces():
  max_possible = 128
  maxBytes = max_possible * 32
  mySocket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
  names = array.array( 'B', '\0' * maxBytes )
  #0x8912 SICGIFCONF
  fcntlOut = fcntl.ioctl( mySocket.fileno(), 0x8912, struct.pack( 'iL', maxBytes, names.buffer_info()[0] ) )
  outbytes = struct.unpack( 'iL', fcntlOut )[0]
  namestr = names.tostring()
  ifaces = {}
  arch = platform.architecture()[0]
  if arch.find( '32' ) == 0:
    for i in range( 0, outbytes, 32 ):
      name = namestr[i:i + 32].split( '\0', 1 )[0]
      ip = namestr[i + 20:i + 24]
      ifaces[ name ] = { 'ip' : socket.inet_ntoa( ip ), 'mac' : getMACFromInterface( name ) }
  else:
    for i in range( 0, outbytes, 40 ):
      name = namestr[i:i + 16].split( '\0', 1 )[0]
      ip = namestr[i + 20:i + 24]
      ifaces[ name ] = { 'ip' : socket.inet_ntoa( ip ), 'mac' : getMACFromInterface( name ) }
  return ifaces

#FIXME: UNUSED ( DIRAC, LHCbDIRAC, VMDIRAC )!
def getAllInterfaces():
  max_possible = 128  # arbitrary. raise if needed.
  maxBytes = max_possible * 32
  mySocket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
  names = array.array( 'B', '\0' * maxBytes )
  outbytes = struct.unpack( 
                            'iL',
                            fcntl.ioctl( 
                                         mySocket.fileno(),
                                         0x8912, # SIOCGIFCONF
                                         struct.pack( 'iL',
                                                      maxBytes,
                                                      names.buffer_info()[0] )
                                       )
                          )[0]
  namestr = names.tostring()
  return [namestr[i:i + 32].split( '\0', 1 )[0] for i in range( 0, outbytes, 32 )]

#FIXME: UNUSED ( DIRAC, LHCbDIRAC, VMDIRAC )!
def getAddressFromInterface( ifName ):
  try:
    mySocket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    return socket.inet_ntoa( fcntl.ioctl( 
                                          mySocket.fileno(),
                                          0x8915, # SIOCGIFADDR
                                          struct.pack( '256s', ifName[:15] )
                                        )[20:24] )
  except Exception:
    return False

def getMACFromInterface( ifname ):
  mySocket = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
  info = fcntl.ioctl( mySocket.fileno(), 0x8927, struct.pack( '256s', ifname[:15] ) )
  return ''.join( ['%02x:' % ord( char ) for char in info[18:24]] )[:-1]

def getFQDN():
  sFQDN = socket.getfqdn()
  if sFQDN.find( 'localhost' ) > -1:
    sFQDN = os.uname()[1]
    socket.getfqdn( sFQDN )
  return sFQDN

def splitURL( url ):
  protocolEnd = url.find( "://" )
  if protocolEnd == -1:
    return S_ERROR( "'%s' URL is malformed" % url )
  protocol = url[ : protocolEnd ]
  url = url[ protocolEnd + 3: ]
  pathStart = url.find( "/" )
  if pathStart > -1:
    host = url[ :pathStart ]
    path = url[ pathStart + 1: ]
  else:
    host = url
    path = "/"
  if path[-1] == "/":
    path = path[:-1]
  portStart = host.find( ":" )
  if portStart > -1:
    port = int( host[ portStart + 1: ] )
    host = host[ :portStart ]
  else:
    port = 0
  return S_OK( ( protocol, host, port, path ) )

def getIPsForHostName( hostName ):
  try:
    ips = [ t[4][0] for t in socket.getaddrinfo( hostName, 0 ) ]
  except Exception, e:
    return S_ERROR( "Can't get info for host %s: %s" % ( hostName, str( e ) ) )
  uniqueIPs = []
  for ip in ips:
    if ip not in uniqueIPs:
      uniqueIPs.append( ip )
  return S_OK( uniqueIPs )

def checkHostsMatch( host1, host2 ):
  ipLists = []
  for host in ( host1, host2 ):
    result = getIPsForHostName( host )
    if not result[ 'OK' ]:
      return result
    ipLists.append( result[ 'Value' ] )
  #Check
  for ip1 in ipLists[0]:
    if ip1 in ipLists[1]:
      return S_OK( True )
  return S_OK( False )




