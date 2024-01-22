#!/usr/bin/env python
"""
Print list of users with proxies.

Example:
  $ dirac-admin-users-with-proxy
  * vhamar
  DN         : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  group      : dirac_admin
  not after  : 2011-06-29 12:04:25
  -
  DN         : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  group      : dirac_pilot
  not after  : 2011-06-29 12:04:27
  -
  DN         : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  group      : dirac_user
  not after  : 2011-06-29 12:04:30
"""
import datetime

import DIRAC
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager


class Params:
    limited = False
    proxyPath = False
    proxyLifeTime = 3600

    def setProxyLifeTime(self, arg):
        try:
            fields = [f.strip() for f in arg.split(":")]
            self.proxyLifeTime = int(fields[0]) * 3600 + int(fields[1]) * 60
        except Exception:
            print(f"Can't parse {arg} time! Is it a HH:MM?")
            return DIRAC.S_ERROR("Can't parse time argument")
        return DIRAC.S_OK()

    def registerCLISwitches(self):
        Script.registerSwitch("v:", "valid=", "Required HH:MM for the users", self.setProxyLifeTime)


@Script()
def main():
    params = Params()
    params.registerCLISwitches()
    Script.parseCommandLine(ignoreErrors=True)
    result = gProxyManager.getDBContents()
    if not result["OK"]:
        print(f"Can't retrieve list of users: {result['Message']}")
        DIRAC.exit(1)

    keys = result["Value"]["ParameterNames"]
    records = result["Value"]["Records"]
    dataDict = {}
    now = datetime.datetime.utcnow()
    for record in records:
        expirationDate = record[3]
        dt = expirationDate - now
        secsLeft = dt.days * 86400 + dt.seconds
        if secsLeft > params.proxyLifeTime:
            userName, userDN, userGroup, _ = record
            if userName not in dataDict:
                dataDict[userName] = []
            dataDict[userName].append((userDN, userGroup, expirationDate))

    for userName in dataDict:
        print(f"* {userName}")
        for iP in range(len(dataDict[userName])):
            data = dataDict[userName][iP]
            print(f" DN         : {data[0]}")
            print(f" group      : {data[1]}")
            print(f" not after  : {TimeUtilities.toString(data[2])}")
            if iP < len(dataDict[userName]) - 1:
                print(" -")

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
