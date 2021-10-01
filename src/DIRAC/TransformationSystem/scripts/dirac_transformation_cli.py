#!/usr/bin/env python
"""
Command to launch the Transformation Shell
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=False)

    from DIRAC.TransformationSystem.Client.TransformationCLI import TransformationCLI

    cli = TransformationCLI()
    cli.cmdloop()


if __name__ == "__main__":
    main()
