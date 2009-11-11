#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-configuration-cli
# Author : Adria Casajus
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.4 $"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.CSCLI import CSCLI

Script.localCfg.addDefaultEntry( "LogLevel", "fatal" )
Script.parseCommandLine()

CSCLI().start()
