#!/usr/bin/env python
""" update local cfg
"""
import os
import sys

from DIRAC.Core.Base.Script import Script

Script.setUsageMessage(
    "\n".join(
        [
            __doc__.split("\n")[1],
            "Usage:",
            f"  {Script.scriptName} [options] ... DB ...",
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

fc = "FileCatalog"
multiFC = "MultiVOFileCatalog"

if os.environ.get("TEST_HTTPS", "Yes") == "Yes":
    fc = f"Tornado{fc}"
    multiFC = f"Tornado{multiFC}"

for sct in [
    "Systems/DataManagement/Services",
    f"Systems/DataManagement/Services/{fc}",
    f"Systems/DataManagement/Services/{multiFC}",
]:
    res = csAPI.createSection(sct)
    if not res["OK"]:
        print(res["Message"])
        sys.exit(1)

csAPI.setOption(f"Systems/DataManagement/Services/{fc}/DirectoryManager", "DirectoryClosure")
csAPI.setOption(f"Systems/DataManagement/Services/{fc}/FileManager", "FileManagerPs")
csAPI.setOption(f"Systems/DataManagement/Services/{fc}/SecurityManager", "VOMSSecurityManager")
csAPI.setOption(f"Systems/DataManagement/Services/{fc}/UniqueGUID", True)

csAPI.setOption(f"Systems/DataManagement/Services/{multiFC}/DirectoryManager", "DirectoryClosure")
csAPI.setOption(f"Systems/DataManagement/Services/{multiFC}/FileManager", "FileManagerPs")
csAPI.setOption(f"Systems/DataManagement/Services/{multiFC}/SecurityManager", "NoSecurityManager")
csAPI.setOption(f"Systems/DataManagement/Services/{multiFC}/UniqueGUID", True)
# configure MultiVO metadata related options:
res = csAPI.setOption(f"Systems/DataManagement/Services/{multiFC}/FileMetadata", "MultiVOFileMetadata")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

res = csAPI.setOption(f"Systems/DataManagement/Services/{multiFC}/DirectoryMetadata", "MultiVODirectoryMetadata")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

csAPI.commit()
