# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Network.py,v 1.4 2007/05/21 15:19:07 acasajus Exp $
__RCSID__ = "$Id: Network.py,v 1.4 2007/05/21 15:19:07 acasajus Exp $"
"""
   Collection of DIRAC useful network related modules
   by default on Error they return None

   getAllInterfaces and getAddressFromInterface do not work in MAC
"""
import socket
import struct
import array
import os
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

def getAllInterfaces():
  import fcntl
  max_possible = 128  # arbitrary. raise if needed.
  bytes = max_possible * 32
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  names = array.array('B', '\0' * bytes)
  outbytes = struct.unpack(
                            'iL',
                            fcntl.ioctl(
                                         s.fileno(),
                                         0x8912,  # SIOCGIFCONF
                                         struct.pack( 'iL',
                                                      bytes,
                                                      names.buffer_info()[0] )
                                       )
                          )[0]
  namestr = names.tostring()
  return [namestr[i:i+32].split('\0', 1)[0] for i in range(0, outbytes, 32)]

def getAddressFromInterface( ifName ):
  import fcntl
  try:
    s = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    return socket.inet_ntoa( fcntl.ioctl(
                                          s.fileno(),
                                          0x8915,  # SIOCGIFADDR
                                          struct.pack('256s', ifName[:15] )
                                        )[20:24] )
  except:
    return False

def getFQDN():
  sFQDN = socket.getfqdn()
  if sFQDN.find( 'localhost' ) > -1:
    sFQDN = os.uname()[1]
    socket.getfqdn( sFQDN )
  return sFQDN

def splitURL( URL ):
  protocolEnd = URL.find( "://" )
  if protocolEnd == -1:
    return S_ERROR( "'%s' URL is malformed" % URL )
  protocol = URL[ : protocolEnd ]
  URL = URL[ protocolEnd + 3: ]
  pathStart = URL.find( "/" )
  if pathStart > -1:
    host = URL[ :pathStart ]
    path = URL[ pathStart + 1: ]
  else:
    host = URL
    path = "/"
  if path[-1] == "/":
    path = path[:-1]
  portStart = host.find( ":" )
  if portStart > -1:
    port = int( host[ portStart+1: ] )
    host = host[ :portStart ]
  else:
    port = 0
  return S_OK( ( protocol, host, port, path ) )
