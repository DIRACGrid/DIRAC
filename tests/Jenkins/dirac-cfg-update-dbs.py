#!/usr/bin/env python
""" update local cfg
"""
import os

from DIRAC.Core.Base.Script import Script
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

Script.setUsageMessage("\n".join([__doc__.split("\n")[1], "Usage:", f"  {Script.scriptName} [options] "]))

Script.parseCommandLine()
args = Script.getPositionalArgs()

csAPI = CSAPI()


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
#     {
#       FileCatalogDB
#       {
#         DBName = FileCatalogDB
#       }
#     }
#   }
# }


for sct in [
    "Systems/DataManagement",
    "Systems/DataManagement/Databases",
    "Systems/DataManagement/Databases/FileCatalogDB",
    "Systems/DataManagement/Databases/MultiVOFileCatalogDB",
]:
    res = csAPI.createSection(sct)
    if not res["OK"]:
        print(res["Message"])
        exit(1)

dbHost = os.environ["DB_HOST"]
dbPort = os.environ["DB_PORT"]

csAPI.setOption("Systems/DataManagement/Databases/FileCatalogDB/DBName", "FileCatalogDB")
csAPI.setOption("Systems/DataManagement/Databases/FileCatalogDB/Host", dbHost)
csAPI.setOption("Systems/DataManagement/Databases/FileCatalogDB/Port", dbPort)

csAPI.setOption("Systems/DataManagement/Databases/MultiVOFileCatalogDB/DBName", "MultiVOFileCatalogDB")
csAPI.setOption("Systems/DataManagement/Databases/MultiVOFileCatalogDB/Host", dbHost)
csAPI.setOption("Systems/DataManagement/Databases/MultiVOFileCatalogDB/Port", dbPort)

# Setup other DBs (this is for LHCb - innocuous!)
#
# Bookkeeping
# {
#   Production
#   {
#     Databases
#     {
#       BookkeepingDB
#       {
#         LHCbDIRACBookkeepingTNS =
#         LHCbDIRACBookkeepingUser =
#         LHCbDIRACBookkeepingPassword =
#         LHCbDIRACBookkeepingServer =
#       }
#     }
#   }
# }

for sct in [
    "Systems/Bookkeeping",
    "Systems/Bookkeeping/Databases",
    "Systems/Bookkeeping/Databases/BookkeepingDB",
]:
    res = csAPI.createSection(sct)
    if not res["OK"]:
        print(res["Message"])
        exit(1)

csAPI.setOption("Systems/Bookkeeping/Databases/BookkeepingDB/LHCbDIRACBookkeepingTNS", "FILL_ME")
csAPI.setOption("Systems/Bookkeeping/Databases/BookkeepingDB/LHCbDIRACBookkeepingUser", "FILL_ME")
csAPI.setOption("Systems/Bookkeeping/Databases/BookkeepingDB/LHCbDIRACBookkeepingPassword", "FILL_ME")
csAPI.setOption("Systems/Bookkeeping/Databases/BookkeepingDB/LHCbDIRACBookkeepingServer", "FILL_ME")

# Commit
csAPI.commit()
