#!/usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################

""" Command to launch the Transformation Shell
"""

__RCSID__   = "$Id: $"

from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Launch the Transformation shell

Usage:
   %s [option]
""" % Script.scriptName)


Script.parseCommandLine( ignoreErrors = False )

from DIRAC.TransformationSystem.Client.TransformationCLI import TransformationCLI

cli = TransformationCLI()
cli.cmdloop()
