#!/usr/bin/env python
""" update local cfg
"""

from __future__ import print_function
#import os

from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgFile] ' % Script.scriptName]))

Script.parseCommandLine()
args = Script.getPositionalArgs()

csAPI = CSAPI()


# set FileMetadata and DirectoryMetadata options:
#
# DataManagement
# {
#   Production
#   {
#     Services
#     {
#       FileCatalog
#       {
# .....
# new options here:
#         FileMetadata = MultiVOFileMetadata
#         DirectoryMetadata = MultiVODirectoryMetadata
#       }
#     }
#     Databases
#     {
#       FileCatalogDB
#       {
#         DBName = FileCatalogDB
#       }
#     }
#   }
# }


#for sct in ['Systems/DataManagement',
#            'Systems/DataManagement/Production',
#            'Systems/DataManagement/Production/Databases',
#            'Systems/DataManagement/Production/Databases/FileCatalogDB']:
#  res = csAPI.createSection(sct)
#  if not res['OK']:
#    print(res['Message'])
#    exit(1)

#dbHost = os.environ['DB_HOST']
#dbPort = os.environ['DB_PORT']

res = csAPI.setOption('Systems/DataManagement/Production/Services/FileCatalog/FileMetadata','MultiVOFileMetadata')
if not res['OK']:
  print(res['Message'])
  exit(1)

res = csAPI.setOption('Systems/DataManagement/Production/Services/FileCatalog/DirectoryMetadata','MultiVODirectoryMetadata')
if not res['OK']:
  print(res['Message'])
  exit(1)

#csAPI.setOption('Systems/DataManagement/Production/Databases/FileCatalogDB/DBName', 'FileCatalogDB')
#csAPI.setOption('Systems/DataManagement/Production/Databases/FileCatalogDB/Host', dbHost)
#csAPI.setOption('Systems/DataManagement/Production/Databases/FileCatalogDB/Port', dbPort)

# Commit
csAPI.commit()
