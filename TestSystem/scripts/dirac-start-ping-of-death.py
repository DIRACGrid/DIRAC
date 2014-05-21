#!/usr/bin/env python

from DIRAC.Core.Base import Script

Script.parseCommandLine()

from DIRAC.Core.DISET.RPCClient import RPCClient

try:
  numPings = int( Script.getPositionalArgs()[0] )
except (IndexError, ValueError):
  numPings = 10

RPCClient( "Test/PingPongMind" ).startPingOfDeath( numPings )
