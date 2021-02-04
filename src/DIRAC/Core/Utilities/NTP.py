from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from socket import socket, AF_INET, SOCK_DGRAM
import struct
import time as time
import datetime
from DIRAC import S_OK, S_ERROR

TIME1970 = 2208988800
gDefaultNTPServers = ["pool.ntp.org"]


def getNTPUTCTime(serverList=None, retries=2):
  data = '\x1b' + 47 * '\0'
  if not serverList:
    serverList = gDefaultNTPServers
  for server in serverList:
    client = socket(AF_INET, SOCK_DGRAM)
    client.settimeout(1)
    worked = False
    while retries >= 0 and not worked:
      try:
        client.sendto(data, (server, 123))
        data, address = client.recvfrom(1024)
        worked = True
      except Exception:
        retries -= 1
    if not worked:
      continue
    if data:
      myTime = struct.unpack('!12I', data)[10]
      myTime -= TIME1970
      return S_OK(datetime.datetime(*time.gmtime(myTime)[:6]))
  return S_ERROR("Could not get NTP time")


def getClockDeviation(serverList=None):
  result = getNTPUTCTime(serverList)
  if not result['OK']:
    return result
  td = datetime.datetime.utcnow() - result['Value']
  return S_OK(abs(td.days * 86400 + td.seconds))
