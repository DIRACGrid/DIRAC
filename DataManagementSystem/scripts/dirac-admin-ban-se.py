#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-admin-ban-se.py,v 1.3 2009/11/04 09:58:41 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-admin-ban-se.py,v 1.3 2009/11/04 09:58:41 acsmith Exp $"
__VERSION__ = "$Revision: 1.3 $"
import DIRAC
from DIRAC.Core.Base                                   import Script

read = True
write = True
site = ''
Script.registerSwitch( "r", "BanRead","      Ban only reading from the storage element")
Script.registerSwitch( "w", "BanWrite","     Ban writing to the storage element")
Script.registerSwitch( "c:", "Site=", "      Ban all SEs associate to site")
Script.parseCommandLine(ignoreErrors = True)

ses = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "r" or switch[0].lower() == "banread":
    write = False
  if switch[0].lower() == "w" or switch[0].lower() == "banwrite":
    read = False
  if switch[0].lower() == "c" or switch[0].lower() == "site":
    site = switch[1]

def usage():
  gLogger.info(' Type "%s --help" for the available options and syntax' % Script.scriptName)
  DIRAC.exit(-1)

from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient  import NotificationClient
from DIRAC.Core.Security.Misc                         import getProxyInfo
from DIRAC                                            import gConfig,gLogger
from DIRAC.Core.Utilities.List                        import intListToString
csAPI = CSAPI()

res = getProxyInfo()
if not res['OK']:
  gLogger.error("Failed to get proxy information",res['Message'])
  DIRAC.exit(2)
userName = res['Value']['username']
group = res['Value']['group']
if group != 'diracAdmin':
  gLogger.error("You must be diracAdmin to execute this script")
  gLogger.info("Please issue 'lhcb-proxy-init -g diracAdmin'")
  DIRAC.exit(2)

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

readBanned = []
writeBanned = []
storageCFGBase = "/Resources/StorageElements"
for se in ses:
  res = gConfig.getOptionsDict("%s/%s" % (storageCFGBase,se))
  if not res['OK']:
    gLogger.error("Storage Element %s does not exist" % se)
    continue
  if read:
    res = csAPI.setOption("%s/%s/ReadAccess" % (storageCFGBase,se),"InActive")
    if not res['OK']:
      gLogger.error("Failed to update %s read access to InActive" % se)
    else:
      gLogger.debug("Successfully updated %s read access to InActive" % se)
      readBanned.append(se)
  if write:
    res = csAPI.setOption("%s/%s/WriteAccess" % (storageCFGBase,se),"InActive")
    if not res['OK']:
      gLogger.error("Failed to update %s write access to InActive" % se)
    else:
      gLogger.debug("Successfully updated %s write access to InActive" % se)
      writeBanned.append(se)
res = csAPI.commitChanges()
if not res['OK']:
  gLogger.error("Failed to commit changes to CS",res['Message'])
  DIRAC.exit(-1)

if not (writeBanned or readBanned):
  gLogger.info("No storage elements were banned")
  DIRAC.exit(-1)

subject = '%s storage elements banned for use' % len(ses)
address = gConfig.getValue('/Operations/EMail/Production','lhcb-grid@cern.ch')
body = ''
if read:
  body = "%s\n\nThe following storage elements were banned for reading:" % body
  for se in readBanned:
    body = "%s\n%s" % (body,se)
if write:
  body = "%s\n\nThe following storage elements were banned for writing:" % body
  for se in writeBanned:
    body = "%s\n%s" % (body,se)

NotificationClient().sendMail(address,subject,body,'%s@cern.ch' % userName)
DIRAC.exit(0)
