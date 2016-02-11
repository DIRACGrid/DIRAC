#!/usr/bin/env python
""" update local cfg
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgFile] ' % Script.scriptName] ) )

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

for sct in ['Systems/DataManagement',
            'Systems/DataManagement/Production',
            'Systems/DataManagement/Production/Databases',
            'Systems/DataManagement/Production/Databases/FileCatalogDB' ]:
  res = csAPI.createSection( sct )
  if not res['OK']:
    print res['Message']
    exit( 1 )

csAPI.setOption( 'Systems/DataManagement/Production/Databases/FileCatalogDB/DBName', 'FileCatalogDB' )
csAPI.setOption( 'Systems/DataManagement/Production/Databases/FileCatalogDB/Host', 'db-50098.cern.ch' )
csAPI.setOption( 'Systems/DataManagement/Production/Databases/FileCatalogDB/Port', '5501' )

csAPI.commit()
