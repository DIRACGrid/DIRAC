#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-configuration-cli
# Author : Adria Casajus
########################################################################
"""
  Command line interface to DIRAC Configuration Server
"""
__RCSID__   = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.CSCLI import CSCLI

Script.localCfg.addDefaultEntry( "LogLevel", "fatal" )
Script.setUsageMessage('\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ] )   )
Script.parseCommandLine()

CSCLI().start()
