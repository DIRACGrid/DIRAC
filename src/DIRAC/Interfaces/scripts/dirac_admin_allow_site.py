#!/usr/bin/env python
########################################################################
# File :    dirac-admin-allow-site
# Author :  Stuart Paterson
########################################################################
"""
Add Site to Active mask for current Setup

Example:
  $ dirac-admin-allow-site LCG.IN2P3.fr "FRANCE"
"""
import time

from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("E:", "email=", "Boolean True/False (True by default)")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("Site:     Name of the Site")
    Script.registerArgument("Comment:  Reason of the action")
    Script.parseCommandLine(ignoreErrors=True)

    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    from DIRAC import exit as DIRACExit, gConfig, gLogger

    def getBoolean(value):
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            Script.showHelp()

    email = True
    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "email":
            email = getBoolean(switch[1])

    diracAdmin = DiracAdmin()
    exitCode = 0
    errorList = []
    setup = gConfig.getValue("/DIRAC/Setup", "")
    if not setup:
        print("ERROR: Could not contact Configuration Service")
        exitCode = 2
        DIRACExit(exitCode)

    # result = promptUser(
    #     'All the elements that are associated with this site will be active, '
    #     'are you sure about this action?'
    # )
    # if not result['OK'] or result['Value'] is 'n':
    #  print 'Script stopped'
    #  DIRACExit( 0 )

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    site, comment = Script.getPositionalArgs(group=True)
    result = diracAdmin.allowSite(site, comment, printOutput=True)
    if not result["OK"]:
        errorList.append((site, result["Message"]))
        exitCode = 2
    else:
        if email:
            userName = diracAdmin._getCurrentUser()
            if not userName["OK"]:
                print("ERROR: Could not obtain current username from proxy")
                exitCode = 2
                DIRACExit(exitCode)
            userName = userName["Value"]
            subject = f"{site} is added in site mask for {setup} setup"
            body = "Site {} is added to the site mask for {} setup by {} on {}.\n\n".format(
                site,
                setup,
                userName,
                time.asctime(),
            )
            body += "Comment:\n%s" % comment
            addressPath = "EMail/Production"
            address = Operations().getValue(addressPath, "")
            if not address:
                gLogger.notice("'%s' not defined in Operations, can not send Mail\n" % addressPath, body)
            else:
                result = diracAdmin.sendMail(address, subject, body)
        else:
            print("Automatic email disabled by flag.")

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRACExit(exitCode)


if __name__ == "__main__":
    main()
