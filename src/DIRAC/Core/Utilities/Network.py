"""
   Collection of DIRAC useful network related modules
   by default on Error they return None
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import socket
import six
from six.moves.urllib import parse as urlparse
import os
import struct
import array
import fcntl
import platform

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


def discoverInterfaces():
  max_possible = 128
  maxBytes = max_possible * 32
  mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  names = array.array('B', b'\0' * maxBytes)
  # 0x8912 SICGIFCONF
  fcntlOut = fcntl.ioctl(mySocket.fileno(), 0x8912, struct.pack('iL', maxBytes, names.buffer_info()[0]))
  outbytes = struct.unpack('iL', fcntlOut)[0]
  namestr = names.tobytes() if six.PY3 else names.tostring()  # pylint: disable=no-member

  if "32" in platform.architecture()[0]:
    step = 32
    offset = 32
  else:
    step = 40
    offset = 16

  ifaces = {}
  for i in range(0, outbytes, step):
    name = namestr[i:i + offset].split(b'\0', 1)[0].decode()
    ip = namestr[i + 20:i + 24]
    ifaces[name] = {'ip': socket.inet_ntoa(ip), 'mac': getMACFromInterface(name)}
  return ifaces


def getMACFromInterface(ifname):
  mySocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  # bytearray is only needed here for Python 2
  info = bytearray(fcntl.ioctl(mySocket.fileno(), 0x8927, struct.pack('256s', ifname[:15].encode())))
  return ''.join(['%02x:' % char for char in info[18:24]])[:-1]


def getFQDN():
  sFQDN = socket.getfqdn()
  if sFQDN.find('localhost') > -1:
    sFQDN = os.uname()[1]
    socket.getfqdn(sFQDN)
  return sFQDN


def splitURL(url):
  o = urlparse.urlparse(url)
  if o.scheme == "":
    return S_ERROR("'%s' URL is missing protocol" % url)
  path = o.path
  path = path.lstrip("/")
  return S_OK((o.scheme, o.hostname or "", o.port or 0, path))


def getIPsForHostName(hostName):
  try:
    ips = [t[4][0] for t in socket.getaddrinfo(hostName, 0)]
  except Exception as e:
    return S_ERROR("Can't get info for host %s: %s" % (hostName, str(e)))
  uniqueIPs = []
  for ip in ips:
    if ip not in uniqueIPs:
      uniqueIPs.append(ip)
  return S_OK(uniqueIPs)


def checkHostsMatch(host1, host2):
  ipLists = []
  for host in (host1, host2):
    result = getIPsForHostName(host)
    if not result['OK']:
      return result
    ipLists.append(result['Value'])
  # Check
  for ip1 in ipLists[0]:
    if ip1 in ipLists[1]:
      return S_OK(True)
  return S_OK(False)
