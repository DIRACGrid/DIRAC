#!/usr/bin/env python
########################################################################
# File :    dirac_info.py
# Author :  Andrei Tsaregorodtsev
########################################################################
"""
Report info about local DIRAC installation

Example:
  $ dirac-info

  Option                 Value
  ============================
  Setup                  Dirac-Production
  ConfigurationServer    dips://ccdiracli08.in2p3.fr:9135/Configuration/Server
  Installation path      /opt/dirac/versions/v7r2-pre33_1613239204
  Installation type      client
  Platform               Linux_x86_64_glibc-2.17
  VirtualOrganization    dteam
  User DN                /DC=org/DC=ugrid/O=people/O=BITP/CN=Andrii Lytovchenko
  Proxy validity, secs   0
  Use Server Certificate Yes
  Skip CA Checks         No
  DIRAC version          v7r2-pre33
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    import os

    import DIRAC
    from DIRAC import gConfig
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
    from DIRAC.Core.Utilities.PrettyPrint import printTable

    def version(arg):
        Script.disableCS()
        print(DIRAC.version)
        DIRAC.exit(0)

    def platform(arg):
        Script.disableCS()
        print(DIRAC.getPlatform())
        DIRAC.exit(0)

    Script.registerSwitch("v", "version", "print version of current DIRAC installation", version)
    Script.registerSwitch("p", "platform", "print platform of current DIRAC installation", platform)
    Script.parseCommandLine(ignoreErrors=True)

    records = []

    records.append(("Setup", gConfig.getValue("/DIRAC/Setup", "Unknown")))
    records.append(
        (
            "AuthorizationServer",
            gConfig.getValue(
                "/DIRAC/Security/Authorization/issuer", "/DIRAC/Security/Authorization/issuer option is absent"
            ),
        )
    )
    records.append(("ConfigurationServer", gConfig.getValue("/DIRAC/Configuration/Servers", "None found")))
    records.append(("Installation path", DIRAC.rootPath))

    if os.path.exists(os.path.join(DIRAC.rootPath, DIRAC.getPlatform(), "bin", "mysql")):
        records.append(("Installation type", "server"))
    else:
        records.append(("Installation type", "client"))

    records.append(("Platform", DIRAC.getPlatform()))

    ret = getProxyInfo(disableVOMS=True)
    if ret["OK"]:
        if "group" in ret["Value"]:
            vo = getVOForGroup(ret["Value"]["group"])
        else:
            vo = getVOForGroup("")
        if not vo:
            vo = "None"
        records.append(("VirtualOrganization", vo))
        if "identity" in ret["Value"]:
            records.append(("User DN", ret["Value"]["identity"]))
        if "secondsLeft" in ret["Value"]:
            records.append(("Proxy validity, secs", {"Value": str(ret["Value"]["secondsLeft"]), "Just": "L"}))

    if gConfig.getValue("/DIRAC/Security/UseServerCertificate", True):
        records.append(("Use Server Certificate", "Yes"))
    else:
        records.append(("Use Server Certificate", "No"))
    if gConfig.getValue("/DIRAC/Security/UseTokens", "false").lower() in ("y", "yes", "true"):
        records.append(("Use tokens", "Yes"))
    else:
        records.append(("Use tokens", "No"))
    if gConfig.getValue("/DIRAC/Security/SkipCAChecks", False):
        records.append(("Skip CA Checks", "Yes"))
    else:
        records.append(("Skip CA Checks", "No"))

    records.append(("DIRAC version", DIRAC.version))

    fields = ["Option", "Value"]

    print()
    printTable(fields, records, numbering=False)
    print()


if __name__ == "__main__":
    main()
