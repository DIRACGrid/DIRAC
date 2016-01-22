#!/usr/bin/env python
""" Command to launch the Transformation Shell
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Launch the Transformation shell

Usage:
   %s [option]
""" % Script.scriptName )


Script.parseCommandLine( ignoreErrors = False )

from DIRAC.TransformationSystem.Client.TransformationCLI import TransformationCLI

cli = TransformationCLI()
cli.cmdloop()
