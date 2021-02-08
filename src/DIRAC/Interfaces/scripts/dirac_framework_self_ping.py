#!/usr/bin/env python

""" Performs a DIPS ping on a given target and exit with the return code.
It uses the local host certificate
The target is specified as ""<port>/System/Service"
The script does not print anything, and just exists with 0 in case of success,
or 1 in case of error """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"


import sys
import os
import time
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  with open(os.devnull, 'w') as redirectStdout, open(os.devnull, 'w') as redirectStderr:
    from DIRAC import gLogger
    from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
    gConfigurationData.setOptionInCFG('/DIRAC/Security/UseServerCertificate', 'true')
    gLogger.setLevel('FATAL')
    from DIRAC.Core.DISET.RPCClient import RPCClient

    rpc = RPCClient('dips://localhost:%s' % sys.argv[1])
    res = rpc.ping()
    time.sleep(0.1)
    sys.exit(0 if res['OK'] else 1)


if __name__ == "__main__":
  main()
