#!/usr/bin/env python
""" refresh CS
"""
import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.ConfigurationSystem.private.Refresher import gRefresher

res = gRefresher.forceRefresh()
if not res["OK"]:
    print(res["Message"])
