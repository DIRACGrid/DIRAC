#!/usr/bin/env python
from __future__ import print_function
import os

from DIRAC.Core.Base import Script

Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
csAPI = CSAPI()

csAPI.setOption('Resources/FileCatalogs/FileCatalog/Master', 'False')
csAPI.setOption('Systems/DataManagement/Production/Services/MultiVOFileCatalog/SecurityManager',
                'NoSecurityManager')

# Final action: commit in CS
res = csAPI.commit()
if not res['OK']:
  print(res['Message'])
  exit(1)
