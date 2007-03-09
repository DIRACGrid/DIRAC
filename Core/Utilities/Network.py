# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Network.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $
__RCSID__ = "$Id: Network.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $"
"""
   Collection of DIRAC useful network related modules
   by default on Error they return None
"""
import socket
import fcntl
import struct
import array
import os

def getAllInterfaces():
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
