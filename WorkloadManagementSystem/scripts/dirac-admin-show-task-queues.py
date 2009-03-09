#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/scripts/dirac-admin-show-task-queues.py,v 1.2 2009/03/09 13:49:49 acasajus Exp $
# File :   dirac-admin-submit-pilot-for-job
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: dirac-admin-show-task-queues.py,v 1.2 2009/03/09 13:49:49 acasajus Exp $"
__VERSION__ = "$Revision: 1.2 $"
import sys
from DIRACEnvironment import DIRAC

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
  sys.exit(1)

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