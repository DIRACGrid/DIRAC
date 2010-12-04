#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-submit-pilot-for-job
# Author :  Ricardo Graciani
########################################################################
__RCSID__ = "$Id$"
import sys
import DIRAC

import sys
import time
import random
import types

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC.Core.DISET.RPCClient import RPCClient


parseCommandLine( initializeMonitor = False )
rpcClient = RPCClient( "WorkloadManagement/Matcher" )

print "Getting TQs.."
result = rpcClient.getActiveTaskQueues()
if not result[ 'OK' ]:
  print 'ERROR: %s' % result['Message']
  sys.exit( 1 )

tqDict = result[ 'Value' ]
for tqId in sorted( tqDict ):
  print "* TQ %s" % tqId
  tqData = tqDict[ tqId ]
  for key in sorted( tqData ):
    value = tqData[ key ]
    if type( value ) == types.ListType:
      print "  %15s: %s" % ( key, ", ".join( value ) )
    else:
      print "  %15s: %s" % ( key, value )
