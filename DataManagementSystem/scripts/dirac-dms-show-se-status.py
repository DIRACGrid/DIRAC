#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-show-se-status.py,v 1.1 2009/09/16 14:33:02 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-show-se-status.py,v 1.1 2009/09/16 14:33:02 acsmith Exp $"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC                                            import gConfig,gLogger
from DIRAC.Core.Utilities.List                        import sortList

storageCFGBase = "/Resources/StorageElements"
res = gConfig.getSections(storageCFGBase,True)
if not res['OK']:
  gLogger.error("Failed to get storage element info")
  gLogger.error(res['Message'])
  DIRAC.exit(-1)
gLogger.info("%s %s %s" % ('Storage Element'.ljust(25),'Read Status'.rjust(15),'Write Status'.rjust(15)))
for se in sortList(res['Value']):
  res = gConfig.getOptionsDict("%s/%s" % (storageCFGBase,se))
  if not res['OK']:
    gLogger.error("Failed to get options dict for %s" % se)
  else:
    readState = 'Active'
    if res['Value'].has_key("ReadAccess"):
      readState = res["Value"]["ReadAccess"]
    writeState = 'Active'
    if res['Value'].has_key("WriteAccess"):
      writeState = res["Value"]["WriteAccess"]
    gLogger.info("%s %s %s" % (se.ljust(25),readState.rjust(15),writeState.rjust(15)))
DIRAC.exit(0)
