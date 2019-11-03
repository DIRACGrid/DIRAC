#!/usr/bin/env python
########################################################################
# File :    dirac-info
# Author :  Andrei Tsaregorodtsev
########################################################################
"""
  Report info about local DIRAC installation
"""

from __future__ import print_function
__RCSID__ = "$Id$"

import os

import DIRAC
from DIRAC import gConfig
from DIRAC.Core.Base import Script
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Utilities.PrettyPrint import printTable

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... Site' % Script.scriptName, ]))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

records = []

records.append(('Setup', gConfig.getValue('/DIRAC/Setup', 'Unknown')))
records.append(('ConfigurationServer', gConfig.getValue('/DIRAC/Configuration/Servers', [])))
records.append(('Installation path', DIRAC.rootPath))

if os.path.exists(os.path.join(DIRAC.rootPath, DIRAC.getPlatform(), 'bin', 'mysql')):
  records.append(('Installation type', 'server'))
else:
  records.append(('Installation type', 'client'))

records.append(('Platform', DIRAC.getPlatform()))

ret = getProxyInfo(disableVOMS=True)
if ret['OK']:
  if 'group' in ret['Value']:
    vo = getVOForGroup(ret['Value']['group'])
  else:
    vo = getVOForGroup('')
  if not vo:
    vo = "None"
  records.append(('VirtualOrganization', vo))
  if 'identity' in ret['Value']:
    records.append(('User DN', ret['Value']['identity']))
  if 'secondsLeft' in ret['Value']:
    records.append(('Proxy validity, secs', {'Value': str(ret['Value']['secondsLeft']), 'Just': 'L'}))

if gConfig.getValue('/DIRAC/Security/UseServerCertificate', True):
  records.append(('Use Server Certificate', 'Yes'))
else:
  records.append(('Use Server Certificate', 'No'))
if gConfig.getValue('/DIRAC/Security/SkipCAChecks', False):
  records.append(('Skip CA Checks', 'Yes'))
else:
  records.append(('Skip CA Checks', 'No'))

records.append(('DIRAC version', DIRAC.version))

fields = ['Option', 'Value']

print()
printTable(fields, records, numbering=False)
print()
