#!/usr/bin/env python
""" update local cfg
"""

from __future__ import print_function
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgFile] ... DB ...' % Script.scriptName,
                                  'Arguments:',
                                  '  setup: Name of the build setup (mandatory)']))

Script.parseCommandLine()

args = Script.getPositionalArgs()

# Setup the DFC
#
# DataManagement
# {
#   Production
#   {
#     Services
#     {
#       FileCatalog
#       {
#         DirectoryManager = DirectoryClosure
#         FileManager = FileManagerPS
#         SecurityManager = FullSecurityManager
#       }
#     }
#     Databases
#       {
#         FileCatalogDB
#         {
#           DBName = FileCatalogDB
#         }
#       }
#   }
# }

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
csAPI = CSAPI()

for sct in ['Systems/DataManagement/Production/Services',
            'Systems/DataManagement/Production/Services/FileCatalog']:
  res = csAPI.createSection(sct)
  if not res['OK']:
    print(res['Message'])
    exit(1)

csAPI.setOption('Systems/DataManagement/Production/Services/FileCatalog/DirectoryManager', 'DirectoryClosure')
csAPI.setOption('Systems/DataManagement/Production/Services/FileCatalog/FileManager', 'FileManagerPs')
csAPI.setOption(
    'Systems/DataManagement/Production/Services/FileCatalog/OldSecurityManager',
    'DirectorySecurityManagerWithDelete')
csAPI.setOption('Systems/DataManagement/Production/Services/FileCatalog/SecurityManager', 'PolicyBasedSecurityManager')
csAPI.setOption('Systems/DataManagement/Production/Services/FileCatalog/SecurityPolicy',
                'DIRAC/DataManagementSystem/DB/FileCatalogComponents/SecurityPolicies/VOMSPolicy')
csAPI.setOption('Systems/DataManagement/Production/Services/FileCatalog/UniqueGUID', True)

csAPI.commit()
