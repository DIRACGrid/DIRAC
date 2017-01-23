#!/usr/bin/env python

from DIRAC.Core.Base import Script

Script.parseCommandLine()

from TestDIRAC.TestSystem.DB.AtomDB import AtomDB
from DIRAC import gConfig, gLogger

gLogger.notice( "CREATE ATOMDB" )
adb = AtomDB()
gLogger.notice( "ATOM DB CREATED" )
gLogger.notice( "INSERTION OF SOMETHING %s" % adb.addStuf( "SOMETHING" ) )
