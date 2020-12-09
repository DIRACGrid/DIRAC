#!/usr/bin/env python
""" Command to launch the Transformation Shell
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Launch the Transformation shell

Usage:
   %s [option]
""" % Script.scriptName)


Script.parseCommandLine(ignoreErrors=False)

from DIRAC.TransformationSystem.Client.TransformationCLI import TransformationCLI

cli = TransformationCLI()
cli.cmdloop()
