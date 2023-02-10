#!/usr/bin/env python
"""
Configures ~/dirac/dcommands.conf. If run without arguments, lists contents of configuration file.

Examples:
    $ dconfig -m (creates minimal config file in ~/dirac/dcommands.conf)
    $ dconfig (lists contents of ~/dirac/dcommands.conf)
"""
import DIRAC
from DIRAC import S_OK
from DIRAC.Core.Base.Script import Script

from DIRAC.Interfaces.Utilities.DCommands import (
    DConfig,
    createMinimalConfig,
)
from DIRAC.Interfaces.Utilities.DCommands import getDNFromProxy


class Params:
    def __init__(self):
        self.minimal = False

    def setMinimal(self, arg):
        self.minimal = True
        return S_OK()

    def getMinimal(self):
        return self.minimal


@Script()
def main():
    params = Params()

    Script.registerArgument(
        [
            "section[.option[=value]]: section:              display all options in section\n"
            "                          section.option:       display option\n"
            "                          section.option=value: set option value"
        ],
        mandatory=False,
    )
    Script.registerSwitch("m", "minimal", "verify and fill minimal configuration", params.setMinimal)

    Script.disableCS()

    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()

    if params.minimal:
        createMinimalConfig()

    dconfig = DConfig()
    modified = False

    if not args:
        sections = dconfig.sections()
        for s in sections:
            retVal = dconfig.get(s, None)
            if not retVal["OK"]:
                print("Error:", retVal["Message"])
                DIRAC.exit(-1)
            print(f"[{s}]")
            for o, v in retVal["Value"]:
                print(o, "=", v)
            print
        DIRAC.exit(0)

    for arg in args:
        value = None
        section = None
        option = None
        if "=" in arg:
            arg, value = arg.split("=", 1)
        if "." in arg:
            section, option = arg.split(".", 1)
        else:
            section = arg

        if value != None:
            dconfig.set(section, option, value)
            modified = True
        else:
            retVal = dconfig.get(section, option)
            if not retVal["OK"]:
                print("Error:", retVal["Message"])
                DIRAC.exit(-1)
            ret = retVal["Value"]
            if isinstance(ret, list):
                print(f"[{section}]")
                for o, v in ret:
                    print(o, "=", v)
            else:
                print(option, "=", ret)

    if modified:
        dconfig.write()


if __name__ == "__main__":
    main()
