#!/usr/bin/env python
""" refresh CS
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
res = gRefresher.forceRefresh()
if not res['OK']:
  print res['Message']
