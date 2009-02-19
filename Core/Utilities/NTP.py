
from socket import *
import struct
import sys
import time as time
import datetime
from DIRAC import S_OK, S_ERROR

TIME1970 = 2208988800L
defaultNTPServers = [ "pool.ntp.org" ]

def getNTPUTCTime( servers = [], retries = 2 ):
  data = '\x1b' + 47 * '\0'
  if not servers:
    servers = defaultNTPServers
  for server in servers:
    client = socket( AF_INET, SOCK_DGRAM )
    client.settimeout( 1 )
    worked = False
    while retries >= 0 and not worked:
      try:
        client.sendto( data, ( server, 123 ) )
        data, address = client.recvfrom( 1024 )
        worked = True
      except Exception, e:
        retries -= 1
    if not worked:
      continue
    if data:
      t = struct.unpack( '!12I', data )[10]
      t -= TIME1970
      return S_OK( datetime.datetime( *time.gmtime(t)[:6] ) )
  return S_ERROR( "Could not get NTP time" )

def getClockDeviation( servers = [] ):
  result = getNTPUTCTime( servers )
  if not result[ 'OK' ]:
    return result
  td = datetime.datetime.utcnow() - result[ 'Value' ]
  return S_OK( abs( td.days*86400 + td.seconds ) )