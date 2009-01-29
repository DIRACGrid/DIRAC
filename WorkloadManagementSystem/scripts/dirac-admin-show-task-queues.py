#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/scripts/dirac-admin-show-task-queues.py,v 1.1 2009/01/29 11:10:24 acasajus Exp $
# File :   dirac-admin-submit-pilot-for-job
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: dirac-admin-show-task-queues.py,v 1.1 2009/01/29 11:10:24 acasajus Exp $"
__VERSION__ = "$Revision: 1.1 $"
import sys
from DIRACEnvironment import DIRAC

import sys
import time
import random
import types

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


parseCommandLine( initializeMonitor = False )
tqDB = TaskQueueDB()
cleanOrphaned = False

if cleanOrphaned:
  result = tqDB.cleanOrphanedTaskQueues()
  if not result[ 'OK' ]:
    print 'ERROR: %s' % result['Message']
    sys.exit(1)

print "Getting TQs.."
result = tqDB.retrieveTaskQueues()
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