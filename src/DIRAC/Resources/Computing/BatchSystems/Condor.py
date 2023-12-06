""" Condor.py is a DIRAC independent class representing Condor batch system.
    Condor objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import re
import tempfile
import subprocess
import shlex
import os


# Cannot use the PilotStatus module here as Condor is meant to be executed on a remote machine
# DIRAC might not be available
STATES_MAP = {
    1: "Waiting",
    2: "Running",
    3: "Aborted",
    4: "Done",
    5: "Failed",
}

HOLD_REASON_SUBCODE = "55"

subTemplate = """
# Environment
# -----------
# There exist many universe:
# https://htcondor.readthedocs.io/en/latest/users-manual/choosing-an-htcondor-universe.html
universe = %(targetUniverse)s

# Inputs/Outputs
# --------------
# Inputs: executable to submit
executable = %(executable)s

# Directory that will contain the outputs
initialdir = %(initialDir)s

# Outputs: stdout, stderr, log
output = $(Cluster).$(Process).out
error = $(Cluster).$(Process).err
log = $(Cluster).$(Process).log

# No other files are to be transferred
transfer_output_files = ""

# Transfer outputs, even if the job is failed
should_transfer_files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT

# Environment variables to pass to the job
environment = "DIRAC_PILOT_STAMP=$(stamp) %(environment)s"

# Credentials
# -----------
%(useCredentials)s

# Requirements
# ------------
request_cpus = %(processors)s

# Exit options
# ------------
# Specify the signal sent to the job when HTCondor needs to vacate the worker node
kill_sig=SIGTERM
# By default, HTCondor marked jobs as completed regardless of its status
# This option allows to mark jobs as Held if they don't finish successfully
on_exit_hold = ExitCode != 0
# A subcode of our choice to identify who put the job on hold
on_exit_hold_subcode = %(holdReasonSubcode)s
# Jobs are then deleted from the system after N days if they are not idle or running
periodic_remove = (JobStatus != 1) && (JobStatus != 2) && ((time() - EnteredCurrentStatus) > (%(daysToKeepRemoteLogs)s * 24 * 3600))

# Specific options
# ----------------
# Local vs Remote schedd
%(scheddOptions)s
# CE-specific options
%(extraString)s


Queue stamp in %(pilotStampList)s
"""


def parseCondorStatus(lines, jobID):
    """parse the condor_q or condor_history output for the job status

    :param lines: list of lines from the output of the condor commands, each line is a tuple of jobID, statusID, and holdReasonCode
    :type lines: python:list
    :param str jobID: jobID of condor job, e.g.: 123.53
    :returns: Status as known by DIRAC, and a reason if the job is being held
    """
    jobID = str(jobID)

    holdReason = ""
    status = None
    for line in lines:
        l = line.strip().split()

        # Make sure the job ID exists
        if len(l) < 1 or l[0] != jobID:
            continue

        # Make sure the status is present and is an integer
        try:
            status = int(l[1])
        except (ValueError, IndexError):
            break

        # Stop here if the status is not held (5): result should be found in STATES_MAP
        if status != 5:
            break

        # A job can be held for various reasons,
        # we need to further investigate with the holdReasonCode & holdReasonSubCode
        # Details in:
        # https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html#HoldReasonCode

        # By default, a held (5) job is defined as Aborted in STATES_MAP, but there might be some exceptions
        status = 3
        try:
            holdReasonCode = l[2]
            holdReasonSubcode = l[3]
            holdReason = " ".join(l[4:])
        except IndexError:
            # This should not happen in theory
            # Just set the status to unknown such as
            status = None
            holdReasonCode = "undefined"
            holdReasonSubcode = "undefined"
            break

        # If holdReasonCode is 3 (The PERIODIC_HOLD expression evaluated to True. Or, ON_EXIT_HOLD was true)
        # And subcode is HOLD_REASON_SUBCODE, then it means the job failed by itself, it needs to be marked as Failed
        if holdReasonCode == "3" and holdReasonSubcode == HOLD_REASON_SUBCODE:
            status = 5
        # If holdReasonCode is 16 (Input files are being spooled), the job should be marked as Waiting
        elif holdReasonCode == "16":
            status = 1

    return (STATES_MAP.get(status, "Unknown"), holdReason)


class Condor(object):
    def submitJob(self, **kwargs):
        """Submit nJobs to the Condor batch system"""

        resultDict = {}

        MANDATORY_PARAMETERS = ["Executable", "OutputDir", "SubmitOptions"]

        for argument in MANDATORY_PARAMETERS:
            if argument not in kwargs:
                resultDict["Status"] = -1
                resultDict["Message"] = "No %s" % argument
                return resultDict

        nJobs = kwargs.get("NJobs")
        if not nJobs:
            nJobs = 1
        stamps = kwargs["JobStamps"]
        numberOfProcessors = kwargs.get("NumberOfProcessors")
        outputDir = kwargs["OutputDir"]
        executable = kwargs["Executable"]
        submitOptions = kwargs["SubmitOptions"]
        preamble = kwargs.get("Preamble")

        jdlFile = tempfile.NamedTemporaryFile(dir=outputDir, suffix=".jdl", mode="wt")
        scheddOptions = 'requirements = OpSys == "LINUX"\n'
        scheddOptions += "gentenv = False"
        jdlFile.write(
            subTemplate
            % dict(
                targetUniverse="vanilla",
                executable=executable,
                initialDir=outputDir,
                environment="CONDOR_JOBID=$(Cluster).$(Process)",
                useCredentials="",
                processors=numberOfProcessors,
                holdReasonSubcode=HOLD_REASON_SUBCODE,
                daysToKeepRemoteLogs=1,
                scheddOptions="",
                extraString="",
                pilotStampList=",".join(stamps),
            )
        )

        jdlFile.flush()

        cmd = "%s; " % preamble if preamble else ""
        cmd += "condor_submit %s %s" % (submitOptions, jdlFile.name)
        sp = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, error = sp.communicate()
        status = sp.returncode

        jdlFile.close()

        if status != 0:
            resultDict["Status"] = status
            resultDict["Message"] = error
            return resultDict

        submittedJobs = 0
        cluster = ""
        lines = output.split("\n")
        for line in lines:
            if "cluster" in line:
                result = re.match(r"(\d+) job.*cluster (\d+)\.", line)
                if result:
                    submittedJobs, cluster = result.groups()
                    try:
                        submittedJobs = int(submittedJobs)
                    except BaseException:
                        submittedJobs = 0

        if submittedJobs > 0 and cluster:
            resultDict["Status"] = 0
            resultDict["Jobs"] = []
            for i in range(submittedJobs):
                resultDict["Jobs"].append(".".join([cluster, str(i)]))
            # Executable is transferred afterward
            # Inform the caller that Condor cannot delete it before the end of the execution
            resultDict["ExecutableToKeep"] = executable
        else:
            resultDict["Status"] = status
            resultDict["Message"] = error
        return resultDict

    def killJob(self, **kwargs):
        """Kill jobs in the given list"""

        resultDict = {}

        MANDATORY_PARAMETERS = ["JobIDList"]
        for argument in MANDATORY_PARAMETERS:
            if argument not in kwargs:
                resultDict["Status"] = -1
                resultDict["Message"] = "No %s" % argument
                return resultDict

        jobIDList = kwargs["JobIDList"]
        if not jobIDList:
            resultDict["Status"] = -1
            resultDict["Message"] = "Empty job list"
            return resultDict

        successful = []
        failed = []
        errors = ""
        for job in jobIDList:
            sp = subprocess.Popen(
                shlex.split("condor_rm %s" % job),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            output, error = sp.communicate()
            status = sp.returncode
            if status != 0:
                failed.append(job)
                errors += error
            else:
                successful.append(job)

        resultDict["Status"] = 0
        if failed:
            resultDict["Status"] = 1
            resultDict["Message"] = errors
        resultDict["Successful"] = successful
        resultDict["Failed"] = failed
        return resultDict

    def getJobStatus(self, **kwargs):
        """Get status of the jobs in the given list"""

        resultDict = {}

        MANDATORY_PARAMETERS = ["JobIDList"]
        for argument in MANDATORY_PARAMETERS:
            if argument not in kwargs:
                resultDict["Status"] = -1
                resultDict["Message"] = "No %s" % argument
                return resultDict

        jobIDList = kwargs["JobIDList"]
        if not jobIDList:
            resultDict["Status"] = -1
            resultDict["Message"] = "Empty job list"
            return resultDict

        user = kwargs.get("User")
        if not user:
            user = os.environ.get("USER")
        if not user:
            resultDict["Status"] = -1
            resultDict["Message"] = "No user name"
            return resultDict

        cmd = "condor_q -submitter %s -af:j JobStatus HoldReasonCode HoldReasonSubCode HoldReason" % user
        sp = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, error = sp.communicate()
        status = sp.returncode

        if status != 0:
            resultDict["Status"] = status
            resultDict["Message"] = error
            return resultDict

        qList = output.strip().split("\n")

        condorHistCall = (
            "condor_history -af:j JobStatus HoldReasonCode HoldReasonSubCode HoldReason -submitter %s" % user
        )
        sp = subprocess.Popen(
            shlex.split(condorHistCall),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, _ = sp.communicate()
        status = sp.returncode
        if status == 0:
            for line in output.split("\n"):
                qList.append(line)

        statusDict = {}
        if len(qList):
            for job in jobIDList:
                job = str(job)
                statusDict[job], _ = parseCondorStatus(qList, job)

        # Final output
        status = 0
        resultDict["Status"] = 0
        resultDict["Jobs"] = statusDict
        return resultDict

    def getCEStatus(self, **kwargs):
        """Get the overall status of the CE"""
        resultDict = {}

        user = kwargs.get("User")
        if not user:
            user = os.environ.get("USER")
        if not user:
            resultDict["Status"] = -1
            resultDict["Message"] = "No user name"
            return resultDict

        waitingJobs = 0
        runningJobs = 0

        sp = subprocess.Popen(
            shlex.split("condor_q -submitter %s" % user),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, error = sp.communicate()
        status = sp.returncode

        if status != 0:
            if "no record" in output:
                resultDict["Status"] = 0
                resultDict["Waiting"] = waitingJobs
                resultDict["Running"] = runningJobs
                return resultDict
            resultDict["Status"] = status
            resultDict["Message"] = error
            return resultDict

        if "no record" in output:
            resultDict["Status"] = 0
            resultDict["Waiting"] = waitingJobs
            resultDict["Running"] = runningJobs
            return resultDict

        if output:
            lines = output.split("\n")
            for line in lines:
                if not line.strip():
                    continue
                if " I " in line:
                    waitingJobs += 1
                elif " R " in line:
                    runningJobs += 1

        # Final output
        resultDict["Status"] = 0
        resultDict["Waiting"] = waitingJobs
        resultDict["Running"] = runningJobs
        return resultDict
