#!/usr/bin/env python
"""
Command to launch the Transformation Shell
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=False)

    from DIRAC.TransformationSystem.Client.TransformationCLI import TransformationCLI

    cli = TransformationCLI()
    cli.cmdloop()


if __name__ == "__main__":
    main()
