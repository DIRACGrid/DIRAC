#!/usr/bin/env python
"""
Remove Site from Active mask for current Setup

Example:
  $ dirac-admin-ban-site LCG.IN2P3.fr "Pilot installation problems"
"""
import time

from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("E:", "email=", "Boolean True/False (True by default)")
    Script.registerSwitch(
        "", "days=", "Number of days the token is valid for. Default is 1 day. 0 or less days denotes forever."
    )
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("Site:     Name of the Site")
    Script.registerArgument("Comment:  Reason of the action")
    Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import exit as DIRACExit
    from DIRAC import gConfig, gLogger
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    def getBoolean(value):
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            Script.showHelp()

    email = True
    days = 1
    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "email":
            email = getBoolean(switch[1])
        if switch[0] == "days":
            days = int(switch[1])

    diracAdmin = DiracAdmin()
    exitCode = 0
    errorList = []

    # result = promptUser(
    #     'All the elements that are associated with this site will be banned,'
    #     'are you sure about this action?'
    # )
    # if not result['OK'] or result['Value'] is 'n':
    #  print 'Script stopped'
    #  DIRACExit( 0 )

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    site, comment = Script.getPositionalArgs(group=True)
    result = diracAdmin.banSite(site, comment, printOutput=True, days=days)
    if not result["OK"]:
        errorList.append((site, result["Message"]))
        exitCode = 2
    else:
        if email and not gConfig.getValue("/DIRAC/Security/UseServerCertificate"):
            userName = diracAdmin._getCurrentUser()
            if not userName["OK"]:
                gLogger.error("Could not obtain current username from proxy")
                exitCode = 2
                DIRACExit(exitCode)
            userName = userName["Value"]
            subject = f"{site} is banned"
            body = "Site {} is removed from site mask by {} on {}.\n\n".format(
                site,
                userName,
                time.asctime(),
            )
            body += f"Comment:\n{comment}"

            addressPath = "EMail/Production"
            address = Operations().getValue(addressPath, "")
            if not address:
                gLogger.notice(f"'{addressPath}' not defined in Operations, can not send Mail\n", body)
            else:
                fromAddress = Operations().getValue("ResourceStatus/Config/FromAddress", "")
                result = diracAdmin.sendMail(address, subject, body, fromAddress=fromAddress)
        else:
            gLogger.warn("Automatic email disabled by flag.")

    for error in errorList:
        gLogger.error(error)

    DIRACExit(exitCode)


if __name__ == "__main__":
    main()
