#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/scripts/dirac-configuration-cli.py,v 1.4 2008/10/17 13:04:01 rgracian Exp $
# File :   dirac-configuration-cli
# Author : Adria Casajus
########################################################################
__RCSID__   = "$Id: dirac-configuration-cli.py,v 1.4 2008/10/17 13:04:01 rgracian Exp $"
__VERSION__ = "$Revision: 1.4 $"

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.CSCLI import CSCLI

Script.localCfg.addDefaultEntry( "LogLevel", "fatal" )
Script.parseCommandLine()

CSCLI().start()
