#!/usr/bin/env python
""" Drop DBs from the MySQL server
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC.Core.Base import Script
Script.registerArgument(["DB: Name of the Database"])
Script.parseCommandLine()
args = Script.getPositionalArgs()

from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
gComponentInstaller.getMySQLPasswords()
for db in args:
  print(gComponentInstaller.execMySQL("DROP DATABASE IF EXISTS %s" % db))
