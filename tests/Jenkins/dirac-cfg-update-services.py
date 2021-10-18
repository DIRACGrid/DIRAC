#!/usr/bin/env python
""" update local cfg
"""

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script

Script.setUsageMessage(
    "\n".join(
        [
            __doc__.split("\n")[1],
            "Usage:",
            "  %s [options] ... DB ..." % Script.scriptName,
            "Arguments:",
            "  setup: Name of the build setup (mandatory)",
        ]
    )
)

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

for sct in [
    "Systems/DataManagement/Production/Services",
    "Systems/DataManagement/Production/Services/FileCatalog",
    "Systems/DataManagement/Production/Services/MultiVOFileCatalog",
]:
    res = csAPI.createSection(sct)
    if not res["OK"]:
        print(res["Message"])
        exit(1)

csAPI.setOption("Systems/DataManagement/Production/Services/FileCatalog/DirectoryManager", "DirectoryClosure")
csAPI.setOption("Systems/DataManagement/Production/Services/FileCatalog/FileManager", "FileManagerPs")
csAPI.setOption("Systems/DataManagement/Production/Services/FileCatalog/SecurityManager", "VOMSSecurityManager")
csAPI.setOption("Systems/DataManagement/Production/Services/FileCatalog/UniqueGUID", True)

csAPI.setOption("Systems/DataManagement/Production/Services/MultiVOFileCatalog/DirectoryManager", "DirectoryClosure")
csAPI.setOption("Systems/DataManagement/Production/Services/MultiVOFileCatalog/FileManager", "FileManagerPs")
csAPI.setOption("Systems/DataManagement/Production/Services/MultiVOFileCatalog/SecurityManager", "NoSecurityManager")
csAPI.setOption("Systems/DataManagement/Production/Services/MultiVOFileCatalog/UniqueGUID", True)
# configure MultiVO metadata related options:
res = csAPI.setOption(
    "Systems/DataManagement/Production/Services/MultiVOFileCatalog/FileMetadata", "MultiVOFileMetadata"
)
if not res["OK"]:
    print(res["Message"])
    exit(1)

res = csAPI.setOption(
    "Systems/DataManagement/Production/Services/MultiVOFileCatalog/DirectoryMetadata", "MultiVODirectoryMetadata"
)
if not res["OK"]:
    print(res["Message"])
    exit(1)

csAPI.commit()
