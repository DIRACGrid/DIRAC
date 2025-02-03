#!/usr/bin/env python
########################################################################
# File :    dirac-framework-ping-service
# Author :  Stuart Paterson
########################################################################
"""
Ping the given DIRAC Service

Example:
  $ dirac-framework-ping-service WorkloadManagement PilotManager
  {'OK': True,
   'Value': {'cpu times': {'children system time': 0.0,
                           'children user time': 0.0,
                           'elapsed real time': 8778481.7200000007,
                           'system time': 54.859999999999999,
                           'user time': 361.06999999999999},
             'host uptime': 4485212L,
             'load': '3.44 3.90 4.02',
             'name': 'WorkloadManagement/PilotManager',
             'service start time': datetime.datetime(2011, 2, 21, 8, 58, 35, 521438),
             'service uptime': 85744,
             'service url': 'dips://dirac.in2p3.fr:9171/WorkloadManagement/PilotManager',
             'time': datetime.datetime(2011, 3, 14, 11, 47, 40, 394957),
             'version': 'v5r12-pre9'},
   'rpcStub': (('WorkloadManagement/PilotManager',
                {'delegatedDN': '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar',
                 'delegatedGroup': 'dirac_user',
                 'skipCACheck': True,
                 'timeout': 120}),
               'ping',
               ())}
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        (
            "URL:            URL of the service to ping (instead of System and Service)",
            "System/Service: Full component name (ie: WorkloadManagement/Matcher)",
            "System:         Name of the DIRAC system (ie: WorkloadManagement)",
        )
    )
    Script.registerArgument(" Service:        Name of the DIRAC service (ie: Matcher)", mandatory=False)
    _, args = Script.parseCommandLine(ignoreErrors=True)
    system = None
    service = None
    url = None
    if len(args) == 1:
        # it is a URL
        if args[0].startswith(("dips://", "https://")):
            url = args[0]
        # It is System/Service
        else:
            sys_serv = args[0].split("/")
            if len(sys_serv) != 2:
                Script.showHelp(exitCode=1)
            else:
                system, service = sys_serv

    elif len(args) == 2:
        system, service = args[0], args[1]
    else:
        Script.showHelp(exitCode=1)

    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac()
    exitCode = 0

    result = dirac.pingService(system, service, printOutput=True, url=url)

    if not result:
        print("ERROR: Null result from ping()")
        exitCode = 2
    elif not result["OK"]:
        print("ERROR: ", result["Message"])
        exitCode = 1

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
