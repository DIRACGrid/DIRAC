""" A computing element class that uses sudo
"""
import os
import pwd
import stat
import errno

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler

from DIRAC.Resources.Computing.ComputingElement import ComputingElement


class SudoComputingElement(ComputingElement):

    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.submittedJobs = 0
        self.runningJobs = 0

        self.processors = int(self.ceParameters.get("NumberOfProcessors", 1))
        self.ceParameters["MaxTotalJobs"] = 1

    #############################################################################
    def submitJob(self, executableFile, proxy=None, **kwargs):
        """Method to submit job, overridden from super-class.

        :param str executableFile: file to execute via systemCall.
                                   Normally the JobWrapperTemplate when invoked by the JobAgent.
        :param str proxy: the proxy used for running the job (the payload). It will be dumped to a file.
        """
        payloadProxy = ""
        if proxy:
            self.log.verbose("Setting up proxy for payload")
            result = self.writeProxyToFile(proxy)
            if not result["OK"]:
                return result

            payloadProxy = result["Value"]  # payload proxy file location

            if "X509_USER_PROXY" not in os.environ:
                self.log.error("X509_USER_PROXY variable for pilot proxy not found in local environment")
                return S_ERROR(DErrno.EPROXYFIND, "X509_USER_PROXY not found")

            # See if a fixed value has been given
            payloadUsername = self.ceParameters.get("PayloadUser")

            if payloadUsername:
                self.log.info("Payload username %s from PayloadUser in ceParameters" % payloadUsername)
            else:
                # First username in the sequence to use when running payload job
                # If first is pltXXp00 then have pltXXp01, pltXXp02, ...
                try:
                    baseUsername = self.ceParameters.get("BaseUsername")
                    baseCounter = int(baseUsername[-2:])
                    self.log.info("Base username from BaseUsername in ceParameters : %s" % baseUsername)
                except Exception:
                    # Last chance to get PayloadUsername
                    if "USER" not in os.environ:
                        self.log.error('PayloadUser, BaseUsername and os.environ["USER"] are not properly defined')
                        return S_ERROR(errno.EINVAL, "No correct payload username provided")

                    baseUsername = os.environ["USER"] + "00p00"
                    baseCounter = 0
                    self.log.info("Base username from $USER + 00p00 : %s" % baseUsername)

                # Next one in the sequence
                payloadUsername = baseUsername[:-2] + ("%02d" % (baseCounter + self.submittedJobs))
                self.log.info("Payload username set to %s using jobs counter" % payloadUsername)

            try:
                payloadUID = pwd.getpwnam(payloadUsername).pw_uid
                payloadGID = pwd.getpwnam(payloadUsername).pw_gid
            except KeyError:
                error = S_ERROR('User "' + str(payloadUsername) + '" does not exist!')
                return error

            self.log.verbose("Starting process for monitoring payload proxy")
            gThreadScheduler.addPeriodicTask(
                self.proxyCheckPeriod,
                self.monitorProxy,
                taskArgs=(payloadProxy, payloadUsername, payloadUID, payloadGID),
                executions=0,
                elapsedTime=0,
            )

        # Submit job
        self.log.info("Changing permissions of executable (%s) to 0755" % executableFile)
        self.submittedJobs += 1

        try:
            os.chmod(
                os.path.abspath(executableFile),
                stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
            )
        except OSError as x:
            self.log.error("Failed to change permissions of executable to 0755 with exception", "\n%s" % (x))

        result = self.sudoExecute(
            os.path.abspath(executableFile), payloadProxy, payloadUsername, payloadUID, payloadGID
        )
        self.runningJobs -= 1
        if not result["OK"]:
            self.log.error("Failed sudoExecute", result)
            return result

        self.log.debug("Sudo CE result OK")

        return S_OK()

    #############################################################################
    def sudoExecute(self, executableFile, payloadProxy, payloadUsername, payloadUID, payloadGID):
        """Run sudo with checking of the exit status code."""
        # We now implement a file giveaway using groups, to avoid any need to sudo to root.
        # Each payload user must have their own group. The pilot user must be a member
        # of all of these groups. This allows the pilot user to set the group of the
        # payloadProxy file to be that of the payload user. The payload user can then
        # read it and make a copy of it (/tmp/x509up_uNNNN) that it owns. Some grid
        # commands check that the proxy is owned by the current user so the copy stage
        # is necessary.

        # 1) Make sure the payload user can read its proxy via its per-user group
        os.chown(payloadProxy, -1, payloadGID)
        os.chmod(payloadProxy, stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP)

        # 2) Now create a copy of the proxy owned by the payload user
        result = shellCall(
            0,
            '/usr/bin/sudo -u %s sh -c "cp -f %s /tmp/x509up_u%d ; chmod 0400 /tmp/x509up_u%d"'
            % (payloadUsername, payloadProxy, payloadUID, payloadUID),
            callbackFunction=self.sendOutput,
        )

        # 3) Make sure the current directory is +rwx by the pilot's group
        #    (needed for InstallDIRAC but not for LHCbInstallDIRAC, for example)
        os.chmod(".", os.stat(".").st_mode | stat.S_IRWXG)

        # Run the executable (the wrapper in fact)
        cmd = "/usr/bin/sudo -u %s " % payloadUsername
        cmd += "PATH=$PATH "
        cmd += "DIRACSYSCONFIG=/scratch/%s/pilot.cfg " % os.environ.get("USER", "")
        cmd += "LD_LIBRARY_PATH=$LD_LIBRARY_PATH "
        cmd += "PYTHONPATH=$PYTHONPATH "
        cmd += "X509_CERT_DIR=$X509_CERT_DIR "
        cmd += "X509_USER_PROXY=/tmp/x509up_u%d sh -c '%s'" % (payloadUID, executableFile)
        self.log.info("CE submission command is: %s" % cmd)
        self.runningJobs += 1
        result = shellCall(0, cmd, callbackFunction=self.sendOutput)
        self.runningJobs -= 1
        if not result["OK"]:
            result["Value"] = (0, "", "")
            return result

        resultTuple = result["Value"]
        status = resultTuple[0]
        stdOutput = resultTuple[1]
        stdError = resultTuple[2]
        self.log.info("Status after the sudo execution is %s" % str(status))
        if status > 128:
            error = S_ERROR(status)
            error["Value"] = (status, stdOutput, stdError)
            return error

        return result

    #############################################################################
    def getCEStatus(self):
        """Method to return information on running and pending jobs."""
        result = S_OK()
        result["SubmittedJobs"] = self.submittedJobs
        result["RunningJobs"] = self.runningJobs
        result["WaitingJobs"] = 0
        # processors
        result["AvailableProcessors"] = self.processors
        return result

    #############################################################################
    def monitorProxy(self, payloadProxy, payloadUsername, payloadUID, payloadGID):
        """Monitor the payload proxy and renew as necessary."""
        retVal = self._monitorProxy(payloadProxy)
        if not retVal["OK"]:
            # Failed to renew the proxy, nothing else to be done
            return retVal

        if not retVal["Value"]:
            # No need to renew the proxy, nothing else to be done
            return retVal

        self.log.info("Re-executing sudo to make renewed payload proxy available as before")

        # New version of the proxy file, so we have to do the copy again

        # 1) Make sure the payload user can read its proxy via its per-user group
        os.chown(payloadProxy, -1, payloadGID)
        os.chmod(payloadProxy, stat.S_IRUSR + stat.S_IWUSR + stat.S_IRGRP)

        # 2) Now recreate the copy of the proxy owned by the payload user
        cmd = '/usr/bin/sudo -u %s sh -c "cp -f %s /tmp/x509up_u%d ; chmod 0400 /tmp/x509up_u%d"' % (
            payloadUsername,
            payloadProxy,
            payloadUID,
            payloadUID,
        )
        result = shellCall(0, cmd, callbackFunction=self.sendOutput)
        if not result["OK"]:
            self.log.error("Could not recreate the copy of the proxy", "CMD: {}; {}".format(cmd, result["Message"]))

        return S_OK("Proxy checked")
