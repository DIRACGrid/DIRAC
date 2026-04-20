#!/usr/bin/env python
"""update local cfg"""

import os
import sys

from DIRAC.Core.Base.Script import Script

Script.setUsageMessage("\n".join([__doc__.split("\n")[0], "Usage:", f"  {Script.scriptName} [options] ... DB ..."]))

Script.parseCommandLine()

# Where to store outputs
if not os.path.isdir("sandboxes"):
    os.makedirs("sandboxes")

# now updating the CS

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

csAPI = CSAPI()

csAPI.setOption("Systems/WorkloadManagement/Services/SandboxStore/BasePath", "sandboxes")
csAPI.setOption("Systems/WorkloadManagement/Services/SandboxStore/LogLevel", "DEBUG")
csAPI.setOption("Systems/WorkloadManagement/Services/TornadoSandboxStore/BasePath", "sandboxes")
csAPI.setOption("Systems/WorkloadManagement/Services/TornadoSandboxStore/LogLevel", "DEBUG")

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
res = csAPI.createSection("Resources/StorageElements/")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

res = csAPI.createSection("Resources/StorageElements/ProductionSandboxSE")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Resources/StorageElements/ProductionSandboxSE/BackendType", "DISET")
csAPI.setOption("Resources/StorageElements/ProductionSandboxSE/AccessProtocol", "dips")

res = csAPI.createSection("Resources/StorageElements/ProductionSandboxSE/DIP")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Resources/StorageElements/ProductionSandboxSE/DIP/Host", "localhost")
csAPI.setOption("Resources/StorageElements/ProductionSandboxSE/DIP/Port", "9196")
csAPI.setOption("Resources/StorageElements/ProductionSandboxSE/DIP/ProtocolName", "DIP")
csAPI.setOption("Resources/StorageElements/ProductionSandboxSE/DIP/Protocol", "dips")
csAPI.setOption("Resources/StorageElements/ProductionSandboxSE/DIP/Access", "remote")
csAPI.setOption("Resources/StorageElements/ProductionSandboxSE/DIP/Path", "sandboxes")

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

res = csAPI.createSection("Resources/FileCatalogs/")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Resources/FileCatalogs/FileCatalog")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

csAPI.setOption("Resources/FileCatalogs/FileCatalog/AccessType", "Read-Write")
csAPI.setOption("Resources/FileCatalogs/FileCatalog/Status", "Active")
csAPI.setOption("Resources/FileCatalogs/FileCatalog/Master", "True")

res = csAPI.createSection("Resources/FileCatalogs/TSCatalog")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

csAPI.setOption("Resources/FileCatalogs/TSCatalog/CatalogType", "TSCatalog")
csAPI.setOption("Resources/FileCatalogs/TSCatalog/AccessType", "Write")
csAPI.setOption("Resources/FileCatalogs/TSCatalog/Status", "Active")
csAPI.setOption("Resources/FileCatalogs/TSCatalog/CatalogURL", "Transformation/TransformationManager")

res = csAPI.createSection("Resources/FileCatalogs/MultiVOFileCatalog")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

csAPI.setOption("Resources/FileCatalogs/MultiVOFileCatalog/CatalogType", "FileCatalog")
csAPI.setOption("Resources/FileCatalogs/MultiVOFileCatalog/AccessType", "Read-Write")
csAPI.setOption("Resources/FileCatalogs/MultiVOFileCatalog/Status", "Active")
csAPI.setOption("Resources/FileCatalogs/MultiVOFileCatalog/CatalogURL", "DataManagement/MultiVOFileCatalog")

# Now setting up the following option:
#     Resources
#     {
#       Sites
#       {
#         DIRAC
#         {
#           DIRAC.Jenkins.ch
#           {
#             SE = SE-1
#             SE += S3-DIRECT
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

for st in [
    "Resources/Sites/DIRAC/",
    "Resources/Sites/DIRAC/DIRAC.Jenkins.ch",
    "Resources/Sites/DIRAC/DIRAC.Jenkins.ch/jenkins.cern.ch",
    "Resources/Sites/DIRAC/DIRAC.Jenkins.ch/jenkins.cern.ch/Queues"
    "Resources/Sites/DIRAC/DIRAC.Jenkins.ch/jenkins.cern.ch/Queues/jenkins-queue_not_important",
    "Resources/StorageElements",
    "Resources/StorageElements/SE-1",
    "Resources/StorageElements/SE-1/DIP",
    "Resources/StorageElements/SE-2",
    "Resources/StorageElements/SE-2/DIP",
]:
    res = csAPI.createSection(st)
    if not res["OK"]:
        print(res["Message"])
        sys.exit(1)

csAPI.setOption("Resources/Sites/DIRAC/DIRAC.Jenkins.ch/SE", "SE-1,S3-DIRECT")
csAPI.setOption("Resources/Sites/DIRAC/DIRAC.Jenkins.ch/CEs/jenkins.cern.ch/CEType", "Test")
csAPI.setOption(
    "Resources/Sites/DIRAC/DIRAC.Jenkins.ch/CEs/jenkins.cern.ch/Queues/jenkins-queue_not_important/maxCPUTime", "200000"
)
csAPI.setOption(
    "Resources/Sites/DIRAC/DIRAC.Jenkins.ch/CEs/jenkins.cern.ch/Queues/jenkins-queue_not_important/SI00", "2400"
)
csAPI.setOption("Resources/StorageElements/SE-1/AccessProtocol", "dips")
csAPI.setOption("Resources/StorageElements/SE-1/DIP/Host", "server")
csAPI.setOption("Resources/StorageElements/SE-1/DIP/Port", "9148")
csAPI.setOption("Resources/StorageElements/SE-1/DIP/Protocol", "dips")
csAPI.setOption("Resources/StorageElements/SE-1/DIP/Path", "/DataManagement/SE-1")
csAPI.setOption("Resources/StorageElements/SE-1/DIP/Access", "remote")

csAPI.setOption("Resources/StorageElements/SE-2/AccessProtocol", "dips")
csAPI.setOption("Resources/StorageElements/SE-2/DIP/Host", "server")
csAPI.setOption("Resources/StorageElements/SE-2/DIP/Port", "9147")
csAPI.setOption("Resources/StorageElements/SE-2/DIP/Protocol", "dips")
csAPI.setOption("Resources/StorageElements/SE-2/DIP/Path", "/DataManagement/SE-2")
csAPI.setOption("Resources/StorageElements/SE-2/DIP/Access", "remote")


# Setting up S3 resources for the Test_Resources_S3.py

# Resources
# {
#   StorageElements
#   {
#     S3-DIRECT
#     {
#       AccessProtocols = s3
#       WriteProtocols = s3
#       S3
#       {
#         Host = s3-direct
#         Port = 9090
#         Protocol = s3
#         Path = my-first-bucket
#         Access = remote
#         SecureConnection = False
#         Aws_access_key_id = fakeId #useless
#         Aws_secret_access_key = fakeKey #useles
#       }
#     }
#   }
# }

for st in [
    "Resources/StorageElements",
    "Resources/StorageElements/S3-DIRECT",
    "Resources/StorageElements/S3-DIRECT/S3",
]:
    res = csAPI.createSection(st)
    if not res["OK"]:
        print(res["Message"])
        sys.exit(1)

csAPI.setOption("Resources/StorageElements/S3-DIRECT/AccessProtocols", "s3")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/WriteProtocols", "s3")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/S3/Host", "s3-direct")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/S3/Port", "9090")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/S3/Protocol", "s3")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/S3/Path", "my-first-bucket")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/S3/Access", "remote")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/S3/SecureConnection", "False")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/S3/Aws_access_key_id", "FakeId")
csAPI.setOption("Resources/StorageElements/S3-DIRECT/S3/Aws_secret_access_key", "True")


# Setting up S3 indirect resources for the Test_Resources_S3.py
# The Aws_access_key_id and Aws_secret_access_key have to be in the server local file only
# so cannot be added here
# Resources
# {
#   StorageElements
#   {
#     S3-INDIRECT
#     {
#       AccessProtocols = s3
#       WriteProtocols = s3
#       S3
#       {
#         Host = s3-direct
#         Port = 9090
#         Protocol = s3
#         Path = my-first-bucket
#         Access = remote
#         SecureConnection = False
#       }
#     }
#   }
# }

for st in [
    "Resources/StorageElements",
    "Resources/StorageElements/S3-INDIRECT",
    "Resources/StorageElements/S3-INDIRECT/S3",
]:
    res = csAPI.createSection(st)
    if not res["OK"]:
        print(res["Message"])
        sys.exit(1)

csAPI.setOption("Resources/StorageElements/S3-INDIRECT/AccessProtocols", "s3")
csAPI.setOption("Resources/StorageElements/S3-INDIRECT/WriteProtocols", "s3")
csAPI.setOption("Resources/StorageElements/S3-INDIRECT/S3/Host", "s3-direct")
csAPI.setOption("Resources/StorageElements/S3-INDIRECT/S3/Port", "9090")
csAPI.setOption("Resources/StorageElements/S3-INDIRECT/S3/Protocol", "s3")
csAPI.setOption("Resources/StorageElements/S3-INDIRECT/S3/Path", "my-first-bucket")
csAPI.setOption("Resources/StorageElements/S3-INDIRECT/S3/Access", "remote")
csAPI.setOption("Resources/StorageElements/S3-INDIRECT/S3/SecureConnection", "False")


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

for st in ["Resources/FTSEndpoints/", "Resources/FTSEndpoints/FTS3/"]:
    res = csAPI.createSection(st)
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

csAPI.setOption("Resources/FTSEndpoints/FTS3/JENKINS-FTS3", "https://jenkins-fts3.cern.ch:8446")


# Now setting a RSS section as the following inside /Operations/Defaults:
#
#     ResourceStatus
#     {
#       Config
#       {
#         Cache = 600
#         FromAddress = fstagni@cern.ch
#         notificationGroups = ShiftersGroup
#       }
#       Policies
#       {
#         Downtime
#         {
#           hours = 0
#         }
#         FreeDiskSpace
#         {
#           Unit               = TB
#           Banned_threshold   = 0.1
#           Degraded_threshold = 5
#         }
#         AlwaysActiveForResource
#         {
#           policyType = AlwaysActive
#           matchParams
#           {
#             element = Resource
#           }
#         }
#         AlwaysBannedForSE1SE2
#         {
#           policyType = AlwaysBanned
#           matchParams
#           {
#             name = SE1,SE2
#           }
#         }
#         AlwaysBannedForSite
#         {
#           policyType = AlwaysBanned
#           matchParams
#           {
#             element = Site
#           }
#         }
#         AlwaysBannedForSite2
#         {
#           policyType = AlwaysBanned
#           matchParams
#           {
#             element = Site
#             domain  = test
#           }
#         }
#         AlwaysDegradedForResource
#         {
#           policyType = AlwaysDegraded
#           matchParams
#           {
#             element = Resource
#           }
#         }
#         SpecificFreeDiskSpace
#         {
#           policyType         = FreeDiskSpace
#           Unit               = GB
#           Banned_threshold   = 10
#           Degraded_threshold = 50
#           matchParams
#           {
#             element    = Resource
#             name       = LogSE
#             statusType = WriteAccess
#           }
#         }
#         PropagationForSite
#         {
#           policyType = Propagation
#           matchParams
#           {
#             element = Site
#           }
#         }
#       }
#       PolicyActions
#       {
#         LogStatusAction
#         {
#         }
#         LogPolicyResultAction
#         {
#         }
#       }
#     }
res = csAPI.createSection("Operations/")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Operations/Defaults")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Operations/Defaults/ResourceStatus")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Config")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Config/Cache", "600")
csAPI.setOption("Operations/Defaults/ResourceStatus/Config/FromAddress", "fstagni@cern.ch")
csAPI.setOption("Operations/Defaults/ResourceStatus/Config/notificationGroups", "ShiftersGroup")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/Downtime")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/Downtime/hours", "0")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/FreeDiskSpace")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/FreeDiskSpace/Unit", "TB")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/FreeDiskSpace/Banned_threshold", "0.1")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/FreeDiskSpace/Degraded_threshold", "5")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysActiveForResource")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysActiveForResource/policyType", "AlwaysActive")
res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysActiveForResource/matchParams")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysActiveForResource/matchParams/element", "Resource")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSE1SE2")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSE1SE2/policyType", "AlwaysBanned")
res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSE1SE2/matchParams")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSE1SE2/matchParams/name", "SE1,SE2")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite/matchParams")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite/policyType", "AlwaysBanned")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite/matchParams/element", "Site")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite2")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite2/matchParams")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite2/policyType", "AlwaysBanned")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite2/matchParams/element", "Site")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysBannedForSite2/matchParams/domain", "test")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysDegradedForResource")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysDegradedForResource/policyType", "AlwaysDegraded")
res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/AlwaysDegradedForResource/matchParams")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/AlwaysDegradedForResource/matchParams/element", "Resource")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace/policyType", "FreeDiskSpace")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace/Unit", "GB")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace/Banned_threshold", "10")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace/Degraded_threshold", "50")
res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace/matchParams")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace/matchParams/element", "Resource")
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace/matchParams/name", "LogSE")
csAPI.setOption(
    "Operations/Defaults/ResourceStatus/Policies/SpecificFreeDiskSpace/matchParams/statusType", "WriteAccess"
)

res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/PropagationForSite")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/PropagationForSite/policyType", "Propagation")
res = csAPI.createSection("Operations/Defaults/ResourceStatus/Policies/PropagationForSite/matchParams")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/ResourceStatus/Policies/PropagationForSite/matchParams/element", "Site")

res = csAPI.createSection("Operations/Defaults/ResourceStatus/PolicyActions")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Operations/Defaults/ResourceStatus/PolicyActions/LogStatusAction")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Operations/Defaults/ResourceStatus/PolicyActions/LogPolicyResultAction")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)


# Now setting the catalog list in Operations/Defaults/Services/Catalogs/CatalogList
#
#     Services
#     {
#       Catalogs
#       {
#         CatalogList = FileCatalog, TSCatalog, MultiVOFileCatalog
#       }
#     }

res = csAPI.createSection("Operations/Defaults/Services")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Operations/Defaults/Services/Catalogs")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Operations/Defaults/Services/Catalogs/CatalogList")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/Services/Catalogs/CatalogList", "FileCatalog, TSCatalog, MultiVOFileCatalog")


# Adding InputDataPolicy section of Operations
# Operations
# {
#   Defaults
#   {
#     InputDataPolicy
#     {
#      Download = DIRAC.WorkloadManagementSystem.Client.DownloadInputData
#      InputDataModule = DIRAC.WorkloadManagementSystem.Client.DownloadInputData
#     }
#   }
# }

res = csAPI.createSection("Operations/Defaults/InputDataPolicy")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption(
    "Operations/Defaults/InputDataPolicy/Download", "DIRAC.WorkloadManagementSystem.Client.DownloadInputData"
)
csAPI.setOption(
    "Operations/Defaults/InputDataPolicy/InputDataModule", "DIRAC.WorkloadManagementSystem.Client.DownloadInputData"
)


# Adding DataManagement section of Operations
# Operations
# {
#   Defaults
#   {
#     DataManagement
#     {
#       RegistrationProtocols = srm,dips,s3
#     }
#   }
# }

res = csAPI.createSection("Operations/Defaults/DataManagement")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/DataManagement/RegistrationProtocols", "srm,dips,s3")

# Adding Transformations section of Operations
# Operations
# {
#   Defaults
#   {
#     Transformations
#     {
#       DataProcessing = MCSimulation, Merge, DataReprocessing
#     }
#   }
# }

res = csAPI.createSection("Operations/Defaults/Transformations")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Operations/Defaults/Transformations/DataProcessing", "MCSimulation, Merge, DataReprocessing")


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
#       Groups
#       {
#         jenkins_fcadmin
#         {
#           VO = Jenkins
#         }
#         jenkins_user
#         {
#           VO = Jenkins
#         }
#       Hosts
#       {
#         server
#         {
#           Properties = TrustedHost,CSAdministrator,JobAdministrator,FullDelegation,ProxyManagement,Operator,ProductionManagement,GenericPilot
#       }
#     }

res = csAPI.createSection("Registry")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Registry/VO/")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Registry/VO/Jenkins")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
res = csAPI.createSection("Registry/VO/Jenkins/VOMSName")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
csAPI.setOption("Registry/VO/Jenkins/VOMSName", "myVOMS")

csAPI.setOption("Registry/Groups/jenkins_fcadmin/VO", "Jenkins")
csAPI.setOption("Registry/Groups/jenkins_user/VO", "Jenkins")

csAPI.setOption(
    "Registry/Hosts/server/Properties",
    "TrustedHost,SiteManager,CSAdministrator,ServiceAdministrator,JobAdministrator,FullDelegation,ProxyManagement,Operator,ProductionManagement,GenericPilot",
)

# Setting Systems/WorkloadManagement/Executors/Optimizers/JobScheduling/RescheduleDelays
# to avoid having to wait while testing rescheduling
csAPI.setOption("Systems/WorkloadManagement/Executors/Optimizers/JobScheduling/RescheduleDelays", "0")

csAPI.createSection("Systems/WorkloadManagement/JobWrapper/")
csAPI.setOption("Systems/WorkloadManagement/JobWrapper/MinOutputDataBufferGB", 1)

# Final action: commit in CS
res = csAPI.commit()
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)
