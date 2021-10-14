#!/usr/bin/env python
########################################################################
# File :   dirac-cert-convert
# Author : Andrii
########################################################################
"""
Script converts the user certificate in the p12 format into a standard .globus usercert.pem and userkey.pem files.
Creates the necessary directory, $HOME/.globus, if needed. Backs-up old pem files if any are found.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import sys
import shutil
from getpass import getpass
from datetime import datetime

from DIRAC import gLogger
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    Script.registerArgument("P12:  certificate in the p12")
    _, args = Script.parseCommandLine(ignoreErrors=True)

    if len(args) == 0:
        gLogger.fatal("User Certificate P12 file is not given.")
        sys.exit(1)

    p12 = args[0]
    if not os.path.isfile(p12):
        gLogger.fatal("%s does not exist." % p12)
        sys.exit(1)

    globus = os.path.join(os.environ["HOME"], ".globus")
    if not os.path.isdir(globus):
        gLogger.notice("Creating '~/.globus' directory")
        os.mkdir(globus)

    p12Path = os.path.join(globus, os.path.basename(p12))
    cert = os.path.join(globus, "usercert.pem")
    key = os.path.join(globus, "userkey.pem")

    nowPrefix = "." + datetime.now().isoformat()
    for old in [p12Path, cert, key]:
        if os.path.isfile(old):
            gLogger.notice("Back up %s file." % old)
            shutil.copy(old, old + nowPrefix)

    shutil.copy(p12, p12Path)

    password = getpass("Enter password for p12:")
    password = " -password pass:%s" % password

    # new OpenSSL version require OPENSSL_CONF to point to some accessible location',
    gLogger.notice("echo Converting p12 key to pem format")
    result = shellCall(
        900, 'export OPENSSL_CONF=/tmp && openssl pkcs12 -nocerts -in "%s" -out "%s"%s' % (p12Path, key, password)
    )
    if result["OK"]:
        gLogger.notice("Converting p12 certificate to pem format")
        result = shellCall(
            900,
            'export OPENSSL_CONF=/tmp && openssl pkcs12 -clcerts -nokeys -in "%s" -out "%s"%s'
            % (p12Path, cert, password),
        )
    if not result["OK"]:
        gLogger.fatal(result["Message"])
        for old in [p12Path, cert, key]:
            if os.path.isfile(old + nowPrefix):
                gLogger.notice("Move %s file back to %s" % (old + nowPrefix, old))
                shutil.move(old + nowPrefix, old)
        sys.exit(1)

    os.chmod(key, 0o400)
    os.chmod(cert, 0o644)


if __name__ == "__main__":
    main()
