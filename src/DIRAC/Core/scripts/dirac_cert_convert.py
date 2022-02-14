#!/usr/bin/env python
"""
Script converts the user certificate in the p12 format into a standard .globus usercert.pem and userkey.pem files.
Creates the necessary directory, $HOME/.globus, if needed. Backs-up old pem files if any are found.
"""
import os
import sys
import shutil
from datetime import datetime

from DIRAC import gLogger
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerArgument("P12: user certificate in the p12")
    _, args = Script.parseCommandLine(ignoreErrors=True)

    p12 = args[0]
    if not os.path.isfile(p12):
        gLogger.fatal("%s does not exist." % p12)
        sys.exit(1)

    globus = os.path.join(os.environ["HOME"], ".globus")
    if not os.path.isdir(globus):
        gLogger.notice(f"Creating {globus} directory")
        os.mkdir(globus)

    cert = os.path.join(globus, "usercert.pem")
    key = os.path.join(globus, "userkey.pem")

    nowPrefix = "." + datetime.now().isoformat()
    for old in [cert, key]:
        if os.path.isfile(old):
            gLogger.notice(f"Back up {old} file to {old + nowPrefix}.")
            shutil.move(old, old + nowPrefix)

    # new OpenSSL version require OPENSSL_CONF to point to some accessible location',
    gLogger.notice("Converting p12 key to pem format")
    result = shellCall(900, f"export OPENSSL_CONF=/tmp && openssl pkcs12 -nocerts -in {p12} -out {key}")
    # The last command was successful
    if result["OK"] and result["Value"][0] == 0:
        gLogger.notice("Converting p12 certificate to pem format")
        result = shellCall(900, f"export OPENSSL_CONF=/tmp && openssl pkcs12 -clcerts -nokeys -in {p12} -out {cert}")
    # Something went wrong
    if not result["OK"] or result["Value"][0] != 0:
        gLogger.fatal(result.get("Message", result["Value"][2]))
        for old in [cert, key]:
            if os.path.isfile(old + nowPrefix):
                gLogger.notice(f"Restore {old} file from the {old + nowPrefix}")
                shutil.move(old + nowPrefix, old)
        sys.exit(1)

    os.chmod(key, 0o400)
    os.chmod(cert, 0o644)

    gLogger.notice(f"{os.path.basename(cert)} and {os.path.basename(key)} was created in the {globus}")


if __name__ == "__main__":
    main()
