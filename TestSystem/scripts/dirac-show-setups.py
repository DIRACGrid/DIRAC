#!/usr/bin/env python

from DIRAC.Core.Base import Script

Script.parseCommandLine()

from DIRAC import gConfig, gLogger

gLogger.notice( "Current setup: %s" % gConfig.getValue( "/DIRAC/Setup" ) )
gLogger.notice( "Available setups are:" )
for setup in gConfig.getSections( "/DIRAC/Setups" )[ 'Value' ]:
  gLogger.notice( " - %s" % setup )

