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

if os.environ["TEST_HTTPS"] == "Yes":
    fc = f"Tornado{fc}"
    multiFC = f"Tornado{multiFC}"

for sct in [
    "Systems/DataManagement/Production/Services",
    f"Systems/DataManagement/Production/Services/{fc}",
    f"Systems/DataManagement/Production/Services/{multiFC}",
]:
    res = csAPI.createSection(sct)
    if not res["OK"]:
        print(res["Message"])
        sys.exit(1)

csAPI.setOption(f"Systems/DataManagement/Production/Services/{fc}/DirectoryManager", "DirectoryClosure")
csAPI.setOption(f"Systems/DataManagement/Production/Services/{fc}/FileManager", "FileManagerPs")
csAPI.setOption(f"Systems/DataManagement/Production/Services/{fc}/SecurityManager", "VOMSSecurityManager")
csAPI.setOption(f"Systems/DataManagement/Production/Services/{fc}/UniqueGUID", True)

csAPI.setOption(f"Systems/DataManagement/Production/Services/{multiFC}/DirectoryManager", "DirectoryClosure")
csAPI.setOption(f"Systems/DataManagement/Production/Services/{multiFC}/FileManager", "FileManagerPs")
csAPI.setOption(f"Systems/DataManagement/Production/Services/{multiFC}/SecurityManager", "NoSecurityManager")
csAPI.setOption(f"Systems/DataManagement/Production/Services/{multiFC}/UniqueGUID", True)
# configure MultiVO metadata related options:
res = csAPI.setOption(f"Systems/DataManagement/Production/Services/{multiFC}/FileMetadata", "MultiVOFileMetadata")
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

res = csAPI.setOption(
    f"Systems/DataManagement/Production/Services/{multiFC}/DirectoryMetadata", "MultiVODirectoryMetadata"
)
if not res["OK"]:
    print(res["Message"])
    sys.exit(1)

csAPI.commit()
