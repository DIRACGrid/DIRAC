#!/usr/bin/env python
""" update local cfg
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgFile] ... DB ...' % Script.scriptName] ) )

Script.parseCommandLine()

args = Script.getPositionalArgs()
setupName = args[0]

import os

# Where to store outputs
if not os.path.isdir( '%s/sandboxes' % setupName ):
  os.makedirs( '%s/sandboxes' % setupName )

# now updating the CS

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
csAPI = CSAPI()

csAPI.setOption( 'Systems/WorkloadManagement/Production/Services/SandboxStore/BasePath', '%s/sandboxes' % setupName )
csAPI.setOption( 'Systems/WorkloadManagement/Production/Services/SandboxStore/LogLevel', 'DEBUG' )

# Now setting a SandboxSE as the following:
#     ProductionSandboxSE
#     {
#       BackendType = DISET
#       AccessProtocol.1
#       {
#         Host = localhost
#         Port = 9196
#         ProtocolName = DIP
#         Protocol = dips
#         Path = /scratch/workspace/%s/sandboxes % setupName
#         Access = remote
#         SpaceToken =
#         WSUrl =
#       }
#     }
res = csAPI.createSection( 'Resources/StorageElements/' )
if not res['OK']:
  print res['Message']
  exit( 1 )

res = csAPI.createSection( 'Resources/StorageElements/ProductionSandboxSE' )
if not res['OK']:
  print res['Message']
  exit( 1 )
csAPI.setOption( 'Resources/StorageElements/ProductionSandboxSE/BackendType', 'DISET' )

res = csAPI.createSection( 'Resources/StorageElements/ProductionSandboxSE/AccessProtocol.1' )
if not res['OK']:
  print res['Message']
  exit( 1 )
csAPI.setOption( 'Resources/StorageElements/ProductionSandboxSE/AccessProtocol.1/Host', 'localhost' )
csAPI.setOption( 'Resources/StorageElements/ProductionSandboxSE/AccessProtocol.1/Port', '9196' )
csAPI.setOption( 'Resources/StorageElements/ProductionSandboxSE/AccessProtocol.1/ProtocolName', 'DIP' )
csAPI.setOption( 'Resources/StorageElements/ProductionSandboxSE/AccessProtocol.1/Path', '%s/sandboxes' % setupName )
csAPI.setOption( 'Resources/StorageElements/ProductionSandboxSE/AccessProtocol.1/Access', 'remote' )


# Now setting a FileCatalogs section as the following:
#     FileCatalogs
#     {
#       FileCatalog
#       {
#         AccessType = Read-Write
#         Status = Active
#         Master = True
#       }
#     }
res = csAPI.createSection( 'Resources/FileCatalogs/' )
if not res['OK']:
  print res['Message']
  exit( 1 )
res = csAPI.createSection( 'Resources/FileCatalogs/FileCatalog' )
if not res['OK']:
  print res['Message']
  exit( 1 )

csAPI.setOption( 'Resources/FileCatalogs/FileCatalog/AccessType', 'Read-Write' )
csAPI.setOption( 'Resources/FileCatalogs/FileCatalog/Status', 'Active' )
csAPI.setOption( 'Resources/FileCatalogs/FileCatalog/Master', 'True' )

# Now setting a the following option:
#     Resources
#     {
#       Sites
#       {
#         DIRAC
#         {
#           DIRAC.Jenkins.org
#           {
#             CEs
#             {
#               some.CE.org
#               {
#                 CEType = SSH
#               }
#             }
#           }
#         }
#       }

res = csAPI.createSection( 'Resources/Sites/DIRAC/DIRAC.Jenkins.org/CEs/' )
if not res['OK']:
  print res['Message']
  exit( 1 )
res = csAPI.createSection( 'Resources/Sites/DIRAC/DIRAC.Jenkins.org/CEs/some.CE.org/' )
if not res['OK']:
  print res['Message']
  exit( 1 )

csAPI.setOption( 'Resources/Sites/DIRAC/DIRAC.Jenkins.org/CEs/some.CE.org/CEType', 'SSH' )


# Now setting a RSS section as the following inside operations:
#
#     ResourceStatus
#     {
#       Policies
#       {
#         AlwaysActiveForResource
#         {
#           matchParams
#           {
#             element = Resource
#           }
#           policyType = AlwaysActive
#         }
#         AlwaysBannedForSE1SE2
#         {
#           matchParams
#           {
#             name = SE1,SE2
#           }
#           policyType = AlwaysBanned
#         }
#         AlwaysBannedForSite
#         {
#           matchParams
#           {
#             element = Site
#           }
#           policyType = AlwaysBanned
#         }
#       }
#     }
res = csAPI.createSection( 'Operations/' )
if not res['OK']:
  print res['Message']
  exit( 1 )
res = csAPI.createSection( 'Operations/ResourceStatus' )
if not res['OK']:
  print res['Message']
  exit( 1 )
res = csAPI.createSection( 'Operations/ResourceStatus/Policies' )
if not res['OK']:
  print res['Message']
  exit( 1 )

res = csAPI.createSection( 'Operations/ResourceStatus/Policies/AlwaysActiveForResource' )
if not res['OK']:
  print res['Message']
  exit( 1 )
csAPI.setOption( 'Operations/ResourceStatus/Policies/AlwaysActiveForResource/policyType', 'AlwaysActive' )
res = csAPI.createSection( 'Operations/ResourceStatus/Policies/AlwaysActiveForResource/matchParams' )
if not res['OK']:
  print res['Message']
  exit( 1 )
csAPI.setOption( 'Operations/ResourceStatus/Policies/AlwaysActiveForResource/matchParams/element', 'Resource' )

res = csAPI.createSection( 'Operations/ResourceStatus/Policies/AlwaysBannedForSE1SE2' )
if not res['OK']:
  print res['Message']
  exit( 1 )
csAPI.setOption( 'Operations/ResourceStatus/Policies/AlwaysBannedForSE1SE2/policyType', 'AlwaysBanned' )
res = csAPI.createSection( 'Operations/ResourceStatus/Policies/AlwaysBannedForSE1SE2/matchParams' )
if not res['OK']:
  print res['Message']
  exit( 1 )
csAPI.setOption( 'Operations/ResourceStatus/Policies/AlwaysBannedForSE1SE2/matchParams/name', 'SE1,SE2' )

res = csAPI.createSection( 'Operations/ResourceStatus/Policies/AlwaysBannedForSite' )
if not res['OK']:
  print res['Message']
  exit( 1 )
res = csAPI.createSection( 'Operations/ResourceStatus/Policies/AlwaysBannedForSite/matchParams' )
csAPI.setOption( 'Operations/ResourceStatus/Policies/AlwaysBannedForSite/policyType', 'AlwaysBanned' )
if not res['OK']:
  print res['Message']
  exit( 1 )
csAPI.setOption( 'Operations/ResourceStatus/Policies/AlwaysBannedForSite/matchParams/element', 'Site' )


# Final action: commit in CS
csAPI.commit()
