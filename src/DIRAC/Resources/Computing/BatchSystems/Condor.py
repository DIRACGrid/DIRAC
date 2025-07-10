""" Condor.py is a DIRAC independent class representing Condor batch system.
    Condor objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import json
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

STATE_ATTRIBUTES = "ClusterId,ProcId,JobStatus,HoldReasonCode,HoldReasonSubCode,HoldReason"

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
requirements = NumJobStarts == 0

# Exit options
# ------------
# Specify the signal sent to the job when HTCondor needs to vacate the worker node
kill_sig=SIGTERM
# By default, HTCondor marked jobs as completed regardless of its status
# This option allows to mark jobs as Held if they don't finish successfully
on_exit_hold = ExitCode =!= 0
# A subcode of our choice to identify who put the job on hold
on_exit_hold_subcode = %(holdReasonSubcode)s
# Jobs are then deleted from the system after N days if they are not idle or running
periodic_remove = ((JobStatus == 1) && (NumJobStarts > 0)) || \
    ((JobStatus != 1) && (JobStatus != 2) && ((time() - EnteredCurrentStatus) > (%(daysToKeepRemoteLogs)s * 24 * 3600)))

# Specific options
# ----------------
# Local vs Remote schedd
%(scheddOptions)s
# CE-specific options
%(extraString)s


Queue stamp in %(pilotStampList)s
"""


def getCondorStatus(jobMetadata):
    """parse the condor_q or condor_history output for the job status

    :param jobMetadata: dict with job metadata
    :type jobMetadata: dict[str, str | int]
    :returns: Status as known by DIRAC, and a reason if the job is being held
    """
    if jobMetadata["JobStatus"] != 5:
        # If the job is not held, we can return the status directly
        return (STATES_MAP.get(jobMetadata["JobStatus"], "Unknown"), "")

    # A job can be held for various reasons,
    # we need to further investigate with the holdReasonCode & holdReasonSubCode
    # Details in:
    # https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html#HoldReasonCode

    # By default, a held (5) job is defined as Aborted in STATES_MAP, but there might be some exceptions
    status = 3

    # If holdReasonCode is 3 (The PERIODIC_HOLD expression evaluated to True. Or, ON_EXIT_HOLD was true)
    # And subcode is HOLD_REASON_SUBCODE, then it means the job failed by itself, it needs to be marked as Failed
    if jobMetadata["HoldReasonCode"] == 3 and jobMetadata["HoldReasonSubCode"] == HOLD_REASON_SUBCODE:
        status = 5
    # If holdReasonCode is 16 (Input files are being spooled), the job should be marked as Waiting
    elif jobMetadata["HoldReasonCode"] == 16:
        status = 1

    return (STATES_MAP.get(status, "Unknown"), jobMetadata["HoldReason"])


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
                extraString=submitOptions,
                pilotStampList=",".join(stamps),
            )
        )

        jdlFile.flush()

        cmd = "%s; " % preamble if preamble else ""
        cmd += "condor_submit -spool %s" % jdlFile.name
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

        # Prepare the command to get the status of the jobs
        cmdJobs = " ".join(str(jobID) for jobID in jobIDList)

        # Get the status of the jobs currently active
        cmd = "condor_q %s -attributes %s -json" % (cmdJobs, STATE_ATTRIBUTES)
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
        if not output:
            output = "[]"

        jobsMetadata = json.loads(output)

        # Get the status of the jobs in the history
        condorHistCall = "condor_history %s -attributes %s -json" % (cmdJobs, STATE_ATTRIBUTES)
        sp = subprocess.Popen(
            shlex.split(condorHistCall),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, _ = sp.communicate()
        status = sp.returncode

        if status != 0:
            resultDict["Status"] = status
            resultDict["Message"] = error
            return resultDict
        if not output:
            output = "[]"

        jobsMetadata += json.loads(output)

        statusDict = {}
        # Build a set of job IDs found in jobsMetadata
        foundJobIDs = set()
        for jobDict in jobsMetadata:
            jobID = "%s.%s" % (jobDict["ClusterId"], jobDict["ProcId"])
            statusDict[jobID], _ = getCondorStatus(jobDict)
            foundJobIDs.add(jobID)

        # For job IDs not found, set status to "Unknown"
        for jobID in jobIDList:
            if str(jobID) not in foundJobIDs:
                statusDict[str(jobID)] = "Unknown"

        # Final output
        status = 0
        resultDict["Status"] = 0
        resultDict["Jobs"] = statusDict
        return resultDict

    def getCEStatus(self, **kwargs):
        """Get the overall status of the CE"""
        resultDict = {}

        cmd = "condor_q -totals -json"
        sp = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, error = sp.communicate()
        status = sp.returncode

        if status != 0 or not output:
            resultDict["Status"] = -1
            resultDict["Message"] = error
            return resultDict

        jresult = json.loads(output)
        resultDict["Status"] = 0
        resultDict["Waiting"] = jresult[0]["Idle"]
        resultDict["Running"] = jresult[0]["Running"]

        # We also need to check the hold jobs, some of them are actually waiting (e.g. for input files)
        cmd = 'condor_q -json -constraint "JobStatus == 5" -attributes HoldReasonCode'
        sp = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, error = sp.communicate()
        status = sp.returncode

        if status != 0:
            resultDict["Status"] = -1
            resultDict["Message"] = error
            return resultDict

        # If there are no held jobs, we can return the result
        if not output:
            return resultDict

        jresult = json.loads(output)
        for job_metadata in jresult:
            if job_metadata["HoldReasonCode"] == 16:
                resultDict["Waiting"] += 1

        return resultDict

    def getJobOutputFiles(self, **kwargs):
        """Get output file names and templates for the specific CE"""
        resultDict = {}

        MANDATORY_PARAMETERS = ["JobIDList", "OutputDir", "ErrorDir"]
        for argument in MANDATORY_PARAMETERS:
            if argument not in kwargs:
                resultDict["Status"] = -1
                resultDict["Message"] = "No %s" % argument
                return resultDict

        outputDir = kwargs["OutputDir"]
        errorDir = kwargs["ErrorDir"]
        jobIDList = kwargs["JobIDList"]

        jobDict = {}
        for jobID in jobIDList:
            jobDict[jobID] = {}

            cmd = "condor_transfer_data %s" % jobID
            sp = subprocess.Popen(
                shlex.split(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            _, error = sp.communicate()
            status = sp.returncode
            if status != 0:
                resultDict["Status"] = -1
                resultDict["Message"] = error
                return resultDict

            jobDict[jobID]["Output"] = "%s/%s.out" % (outputDir, jobID)
            jobDict[jobID]["Error"] = "%s/%s.err" % (errorDir, jobID)

        resultDict["Status"] = 0
        resultDict["Jobs"] = jobDict
        return resultDict
