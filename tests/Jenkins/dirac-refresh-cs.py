#!/usr/bin/env python
""" refresh CS
"""

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script

Script.parseCommandLine()

from DIRAC.ConfigurationSystem.private.Refresher import gRefresher

res = gRefresher.forceRefresh()
if not res["OK"]:
    print(res["Message"])
