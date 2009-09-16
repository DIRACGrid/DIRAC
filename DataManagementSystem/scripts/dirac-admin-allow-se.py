#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-admin-allow-se.py,v 1.1 2009/09/16 13:23:40 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-admin-allow-se.py,v 1.1 2009/09/16 13:23:40 acsmith Exp $"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base import Script

read = True
write = True
site = ''
Script.registerSwitch( "r", "AllowRead","      Allow only reading from the storage element")
Script.registerSwitch( "w", "AllowWrite","     Allow only writing to the storage element")
Script.registerSwitch( "c:", "Site=", "        Allow all SEs associated to site")
Script.parseCommandLine(ignoreErrors = True)

ses = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "r" or switch[0].lower() == "allowread":
    write = False
  if switch[0].lower() == "w" or switch[0].lower() == "allowwrite":
    read = False
  if switch[0].lower() == "c" or switch[0].lower() == "site":
    site = switch[1]

def usage():
  gLogger.info(' Type "%s --help" for the available options and syntax' % Script.scriptName)
  DIRAC.exit(-1)

from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient  import NotificationClient
from DIRAC                                            import gConfig,gLogger
from DIRAC.Core.Utilities.List                        import intListToString
csAPI = CSAPI()

if not type(ses) == type([]):
  usage()
if site:
  res = gConfig.getOptionsDict('/Resources/Sites/LCG/%s' % site)
  if not res['OK']:
    gLogger.error("The provided site (%s) is not known." % site)
    DIRAC.exit(-1) 
  ses.extend(res['Value']['SE'].replace(' ','').split(','))
if not ses:
  gLogger.error("There were no SEs provided")
  DIRAC.exit()

readAllowed = []
writeAllowed = []
storageCFGBase = "/Resources/StorageElements"
for se in ses:
  res = gConfig.getOptionsDict("%s/%s" % (storageCFGBase,se))
  if not res['OK']:
    gLogger.error("Storage Element %s does not exist" % se)
    continue
  if read:
    res = csAPI.setOption("%s/%s/ReadAccess" % (storageCFGBase,se),"Active")
    if not res['OK']:
      gLogger.error("Failed to update %s read access to Active" % se)
    else:
      gLogger.debug("Successfully updated %s read access to Active" % se)
      readAllowed.append(se)
  if write:
    res = csAPI.setOption("%s/%s/WriteAccess" % (storageCFGBase,se),"Active")
    if not res['OK']:
      gLogger.error("Failed to update %s write access to Active" % se)
    else:
      gLogger.debug("Successfully updated %s write access to Active" % se)
      writeAllowed.append(se)
res = csAPI.commitChanges()
if not res['OK']:
  gLogger.error("Failed to commit changes to CS",res['Message'])
  DIRAC.exit(-1)

if not (writeAllowed or readAllowed):
  gLogger.info("No storage elements were allowed")
  DIRAC.exit(-1)

subject = '%s storage elements allowed for use' % len(ses)
address = gConfig.getValue('/Operations/EMail/Production','lhcb-grid@cern.ch')
body = ''
if read:
  body = "%s\n\nThe following storage elements were allowed for reading:" % body
  for se in readAllowed:
    body = "%s\n%s" % (body,se)
if write:
  body = "%s\n\nThe following storage elements were allowed for writing:" % body
  for se in writeAllowed:
    body = "%s\n%s" % (body,se)

res = NotificationClient().sendMail(address,subject,body,'lhcb-datamanagement@cern.ch')
DIRAC.exit(0)
