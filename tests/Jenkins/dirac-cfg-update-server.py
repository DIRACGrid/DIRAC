#!/usr/bin/env python
""" update local cfg
"""

from __future__ import print_function
import os

from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgFile] ... DB ...' % Script.scriptName]))

Script.parseCommandLine()

args = Script.getPositionalArgs()
setupName = args[0]

# Where to store outputs
if not os.path.isdir('%s/sandboxes' % setupName):
  os.makedirs('%s/sandboxes' % setupName)

# now updating the CS

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
csAPI = CSAPI()

csAPI.setOption('Systems/WorkloadManagement/Production/Services/SandboxStore/BasePath', '%s/sandboxes' % setupName)
csAPI.setOption('Systems/WorkloadManagement/Production/Services/SandboxStore/LogLevel', 'DEBUG')

# Now setting a SandboxSE as the following:
#     ProductionSandboxSE
#     {
#       BackendType = DISET
#       AccessProtocol = dips
#       DIP
#       {
#         Host = localhost
#         Port = 9196
#         ProtocolName = DIP
#         Protocol = dips
#         Path = /scratch/workspace/%s/sandboxes % setupName
#         Access = remote
#       }
#     }
res = csAPI.createSection('Resources/StorageElements/')
if not res['OK']:
  print(res['Message'])
  exit(1)

res = csAPI.createSection('Resources/StorageElements/ProductionSandboxSE')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Resources/StorageElements/ProductionSandboxSE/BackendType', 'DISET')
csAPI.setOption('Resources/StorageElements/ProductionSandboxSE/AccessProtocol', 'dips')

res = csAPI.createSection('Resources/StorageElements/ProductionSandboxSE/DIP')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Resources/StorageElements/ProductionSandboxSE/DIP/Host', 'localhost')
csAPI.setOption('Resources/StorageElements/ProductionSandboxSE/DIP/Port', '9196')
csAPI.setOption('Resources/StorageElements/ProductionSandboxSE/DIP/ProtocolName', 'DIP')
csAPI.setOption('Resources/StorageElements/ProductionSandboxSE/DIP/Protocol', 'dips')
csAPI.setOption('Resources/StorageElements/ProductionSandboxSE/DIP/Access', 'remote')
csAPI.setOption('Resources/StorageElements/ProductionSandboxSE/DIP/Path', '%s/sandboxes' % setupName)

# Now setting a FileCatalogs section as the following:
#     FileCatalogs
#     {
#       FileCatalog
#       {
#         AccessType = Read-Write
#         Status = Active
#         Master = True
#       }
#       TSCatalog
#       {
#         CatalogType = TSCatalog
#         AccessType = Write
#         Status = Active
#        CatalogURL = Transformation/TransformationManager
#       }
#     }

res = csAPI.createSection('Resources/FileCatalogs/')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Resources/FileCatalogs/FileCatalog')
if not res['OK']:
  print(res['Message'])
  exit(1)

csAPI.setOption('Resources/FileCatalogs/FileCatalog/AccessType', 'Read-Write')
csAPI.setOption('Resources/FileCatalogs/FileCatalog/Status', 'Active')
csAPI.setOption('Resources/FileCatalogs/FileCatalog/Master', 'True')

res = csAPI.createSection('Resources/FileCatalogs/TSCatalog')
if not res['OK']:
  print(res['Message'])
  exit(1)

csAPI.setOption('Resources/FileCatalogs/TSCatalog/CatalogType', 'TSCatalog')
csAPI.setOption('Resources/FileCatalogs/TSCatalog/AccessType', 'Write')
csAPI.setOption('Resources/FileCatalogs/TSCatalog/Status', 'Active')
csAPI.setOption('Resources/FileCatalogs/TSCatalog/CatalogURL', 'Transformation/TransformationManager')

res = csAPI.createSection('Resources/FileCatalogs/MultiVOFileCatalog')
if not res['OK']:
  print(res['Message'])
  exit(1)

csAPI.setOption('Resources/FileCatalogs/MultiVOFileCatalog/CatalogType', 'FileCatalog')
csAPI.setOption('Resources/FileCatalogs/MultiVOFileCatalog/AccessType', 'Read-Write')
csAPI.setOption('Resources/FileCatalogs/MultiVOFileCatalog/Status', 'Active')
csAPI.setOption('Resources/FileCatalogs/MultiVOFileCatalog/Master', 'True')
csAPI.setOption('Resources/FileCatalogs/MultiVOFileCatalog/CatalogURL', 'DataManagement/MultiVOFileCatalog')

# Now setting up the following option:
#     Resources
#     {
#       Sites
#       {
#         DIRAC
#         {
#           DIRAC.Jenkins.ch
#           {
#             CEs
#             {
#               jenkins.cern.ch
#               {
#                 CEType = Test
#                 Queues
#                 {
#                   jenkins-queue_not_important
#                   {
#                     maxCPUTime = 200000
#                     SI00 = 2400
#                   }
#                 }
#               }
#             }
#           }
#         }
#       }

for st in ['Resources/Sites/DIRAC/',
           'Resources/Sites/DIRAC/DIRAC.Jenkins.ch',
           'Resources/Sites/DIRAC/DIRAC.Jenkins.ch/jenkins.cern.ch',
           'Resources/Sites/DIRAC/DIRAC.Jenkins.ch/jenkins.cern.ch/Queues'
           'Resources/Sites/DIRAC/DIRAC.Jenkins.ch/jenkins.cern.ch/Queues/jenkins-queue_not_important',
           'Resources/StorageElements',
           'Resources/StorageElements/SE-1',
           'Resources/StorageElements/SE-1/DIP',
           'Resources/StorageElements/SE-2',
           'Resources/StorageElements/SE-2/DIP',
           ]:
  res = csAPI.createSection(st)
if not res['OK']:
  print(res['Message'])
  exit(1)

csAPI.setOption('Resources/Sites/DIRAC/DIRAC.Jenkins.ch/CEs/jenkins.cern.ch/CEType', 'Test')
csAPI.setOption(
    'Resources/Sites/DIRAC/DIRAC.Jenkins.ch/CEs/jenkins.cern.ch/Queues/jenkins-queue_not_important/maxCPUTime',
    '200000')
csAPI.setOption('Resources/Sites/DIRAC/DIRAC.Jenkins.ch/CEs/jenkins.cern.ch/Queues/jenkins-queue_not_important/SI00',
                '2400')

csAPI.setOption('Resources/StorageElements/SE-1/AccessProtocol', 'dips')
csAPI.setOption('Resources/StorageElements/SE-1/DIP/Host', 'server')
csAPI.setOption('Resources/StorageElements/SE-1/DIP/Port', '9148')
csAPI.setOption('Resources/StorageElements/SE-1/DIP/Protocol', 'dips')
csAPI.setOption('Resources/StorageElements/SE-1/DIP/Path', '/DataManagement/SE-1')
csAPI.setOption('Resources/StorageElements/SE-1/DIP/Access', 'remote')

csAPI.setOption('Resources/StorageElements/SE-2/AccessProtocol', 'dips')
csAPI.setOption('Resources/StorageElements/SE-2/DIP/Host', 'server')
csAPI.setOption('Resources/StorageElements/SE-2/DIP/Port', '9147')
csAPI.setOption('Resources/StorageElements/SE-2/DIP/Protocol', 'dips')
csAPI.setOption('Resources/StorageElements/SE-2/DIP/Path', '/DataManagement/SE-2')
csAPI.setOption('Resources/StorageElements/SE-2/DIP/Access', 'remote')


# Now setting up the following option:
#     Resources
#     {
#       FTSEndpoints
#       {
#         FTS3
#         {
#           JENKINS-FTS3 = https://jenkins-fts3.cern.ch:8446
#         }
#       }

for st in ['Resources/FTSEndpoints/',
           'Resources/FTSEndpoints/FTS3/']:
  res = csAPI.createSection(st)
if not res['OK']:
  print(res['Message'])
  exit(1)

csAPI.setOption('Resources/FTSEndpoints/FTS3/JENKINS-FTS3', 'https://jenkins-fts3.cern.ch:8446')


# Now setting a RSS section as the following inside /Operations/Defaults:
#
#     ResourceStatus
#     {
#       Config
#       {
#         Cache = 600
#         State = Active
#         FromAddress = fstagni@cern.ch
#         notificationGroups = ShiftersGroup
#         StatusTypes
#         {
#           default = all
#           StorageElement = ReadAccess
#           StorageElement += WriteAccess
#           StorageElement += CheckAccess
#           StorageElement += RemoveAccess
#         }
#       }
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
res = csAPI.createSection('Operations/')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Operations/Defaults')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Operations/Defaults/ResourceStatus')
if not res['OK']:
  print(res['Message'])
  exit(1)

res = csAPI.createSection('Operations/Defaults/ResourceStatus/Config')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Operations/Defaults/ResourceStatus/Config/Cache', '600')
csAPI.setOption('Operations/Defaults/ResourceStatus/Config/State', 'Active')
csAPI.setOption('Operations/Defaults/ResourceStatus/Config/FromAddress', 'fstagni@cern.ch')
csAPI.setOption('Operations/Defaults/ResourceStatus/Config/notificationGroups', 'ShiftersGroup')
res = csAPI.createSection('Operations/Defaults/ResourceStatus/Config/StatusTypes')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Operations/Defaults/ResourceStatus/Config/StatusTypes/default', 'all')
csAPI.setOption('Operations/Defaults/ResourceStatus/Config/StatusTypes/StorageElement',
                'ReadAccess,WriteAccess,CheckAccess,RemoveAccess')

res = csAPI.createSection('Operations/Defaults/ResourceStatus/Policies')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Operations/Defaults/ResourceStatus/Policies/AlwaysActiveForResource')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Operations/Defaults/ResourceStatus/Policies/AlwaysActiveForResource/policyType', 'AlwaysActive')
res = csAPI.createSection('Operations/Defaults/ResourceStatus/Policies/AlwaysActiveForResource/matchParams')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Operations/Defaults/ResourceStatus/Policies/AlwaysActiveForResource/matchParams/element', 'Resource')

res = csAPI.createSection('Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSE1SE2')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSE1SE2/policyType', 'AlwaysBanned')
res = csAPI.createSection('Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSE1SE2/matchParams')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSE1SE2/matchParams/name', 'SE1,SE2')

res = csAPI.createSection('Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite/matchParams')
csAPI.setOption('Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite/policyType', 'AlwaysBanned')
csAPI.setOption('Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite/matchParams/element', 'Site')


# Now setting the catalog list in Operations/Defults/Services/Catalogs/CatalogList
#
#     Services
#     {
#       Catalogs
#       {
#         CatalogList = FileCatalog, TSCatalog, MultiVOFileCatalog
#       }
#     }

res = csAPI.createSection('Operations/Defaults/Services')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Operations/Defaults/Services/Catalogs')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Operations/Defaults/Services/Catalogs/CatalogList')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Operations/Defaults/Services/Catalogs/CatalogList',
                'FileCatalog, TSCatalog, MultiVOFileCatalog')


# Now setting the Registry section
#
#     Registry
#     {
#       VO
#       {
#         Jenkins
#         {
#           VOMSName = myVOMS
#         }
#       }
#     }

res = csAPI.createSection('Registry')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Registry/VO/')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Registry/VO/Jenkins')
if not res['OK']:
  print(res['Message'])
  exit(1)
res = csAPI.createSection('Registry/VO/Jenkins/VOMSName')
if not res['OK']:
  print(res['Message'])
  exit(1)
csAPI.setOption('Registry/VO/Jenkins/VOMSName', 'myVOMS')

csAPI.setOption('Registry/Groups/jenkins_fcadmin/VO', 'Jenkins')
csAPI.setOption('Registry/Groups/jenkins_user/VO', 'Jenkins')


# Final action: commit in CS
res = csAPI.commit()
if not res['OK']:
  print(res['Message'])
  exit(1)
