""" SLURM.py is a DIRAC independent class representing SLURM batch system.
    SLURM objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import os
import re
import subprocess
import shlex
import random


class SLURM(object):
    def submitJob(self, **kwargs):
        """Submit nJobs to the OAR batch system"""

        resultDict = {}

        MANDATORY_PARAMETERS = ["Executable", "OutputDir", "ErrorDir", "Queue", "SubmitOptions"]

        for argument in MANDATORY_PARAMETERS:
            if argument not in kwargs:
                resultDict["Status"] = -1
                resultDict["Message"] = "No %s" % argument
                return resultDict

        nJobs = kwargs.get("NJobs")
        if not nJobs:
            nJobs = 1

        stamps = kwargs["JobStamps"]
        outputDir = kwargs["OutputDir"]
        errorDir = kwargs["ErrorDir"]
        queue = kwargs["Queue"]
        submitOptions = kwargs["SubmitOptions"]
        executable = kwargs["Executable"]
        account = kwargs.get("Account", "")
        numberOfProcessors = kwargs.get("NumberOfProcessors", 1)
        wholeNode = kwargs.get("WholeNode", False)
        # numberOfNodes is treated as a string as it can contain values such as "2-4"
        # where 2 would represent the minimum number of nodes to allocate, and 4 the maximum
        numberOfNodes = kwargs.get("NumberOfNodes", "1")
        numberOfGPUs = kwargs.get("NumberOfGPUs")
        preamble = kwargs.get("Preamble")

        outFile = os.path.join(outputDir, "%jobid%")
        errFile = os.path.join(errorDir, "%jobid%")
        outFile = os.path.expandvars(outFile)
        errFile = os.path.expandvars(errFile)
        executable = os.path.expandvars(executable)

        # There are more than 1 node, we have to run the executable in parallel on different nodes using srun
        if numberOfNodes != "1":
            self._generateSrunWrapper(executable)

        jobIDs = []
        for iJob in range(nJobs):
            jid = ""
            cmd = "%s; " % preamble if preamble else ""
            # By default, all the environment variables of the submitter node are propagated to the workers
            # It can create conflicts during the installation of the pilots
            # --export restricts the propagation to the PATH variable to get a clean environment in the workers
            cmd += "sbatch --export=PATH,DIRAC_PILOT_STAMP=%s " % stamps[iJob]
            cmd += "-o %s/%%j.out " % outputDir
            cmd += "-e %s/%%j.err " % errorDir
            cmd += "--partition=%s " % queue
            if account:
                cmd += "--account=%s " % account
            # One pilot (task) per node, allocating a certain number of processors
            cmd += "--ntasks-per-node=1 "
            cmd += "--nodes=%s " % numberOfNodes
            if wholeNode:
                cmd += "--exclusive "
            else:
                cmd += "--cpus-per-task=%d " % numberOfProcessors
            if numberOfGPUs:
                cmd += "--gpus-per-task=%d " % int(numberOfGPUs)
            # Additional options
            cmd += "%s %s" % (submitOptions, executable)
            sp = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            output, error = sp.communicate()
            status = sp.returncode
            if status != 0 or not output:
                break

            lines = output.split("\n")
            for line in lines:
                result = re.search(r"Submitted batch job (\d*)", line)
                if result:
                    jid = result.groups()[0]
                    break

            if not jid:
                break

            jid = jid.strip()
            jobIDs.append(jid)

        if jobIDs:
            resultDict["Status"] = 0
            resultDict["Jobs"] = jobIDs
        else:
            resultDict["Status"] = status
            resultDict["Message"] = error
        return resultDict

    def _generateSrunWrapper(self, executableFile):
        """
        Associate the executable with srun, to execute the same command in parallel on multiple nodes.
        The wrapper overwrites the executable file

        :param str executableFile: name of the executable file to wrap
        :return str: name of the wrapper that runs the executable via srun
        """
        suffix = random.randrange(1, 99999)
        wrapper = os.path.join(os.path.dirname(executableFile), "srunExec_%s.sh" % suffix)

        with open(executableFile, "r") as f:
            content = f.read()

        # Need to escape environment variables of the executable file
        content = re.sub(r"\$", r"\\$", content)

        # Build the script to run the executable in parallel multiple times
        # - Embed the content of executableFile inside the parallel library wrapper script
        # - srun is the command to execute a task multiple time in parallel
        #   -l option: add the task ID to the output
        #   -k option: do not kill the slurm job if one of the nodes is broken
        cmd = """#!/bin/bash
cat > %(wrapper)s << EOFEXEC
%(content)s
EOFEXEC
chmod 755 %(wrapper)s
srun -l -k %(wrapper)s
""" % dict(
            wrapper=wrapper, content=content
        )

        with open(executableFile, "w") as f:
            f.write(cmd)

    def killJob(self, **kwargs):
        """Delete a job from SLURM batch scheduler. Input: list of jobs output: int"""

        resultDict = {}

        MANDATORY_PARAMETERS = ["JobIDList", "Queue"]
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

        queue = kwargs["Queue"]

        successful = []
        failed = []
        errors = ""
        for job in jobIDList:
            cmd = "scancel --partition=%s %s" % (queue, job)
            sp = subprocess.Popen(
                shlex.split(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            _, error = sp.communicate()
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

        if "JobIDList" not in kwargs or not kwargs["JobIDList"]:
            resultDict["Status"] = -1
            resultDict["Message"] = "Empty job list"
            return resultDict

        jobIDList = kwargs["JobIDList"]

        jobIDs = ""
        for jobID in jobIDList:
            jobIDs += jobID + ","

        # displays accounting data for all jobs in the Slurm job accounting log or Slurm database
        # -j is the given job
        # -o the information of interest
        # -X to get rid of intermediate steps
        # -n to remove the header
        # -P to make the output parseable (remove tabs, spaces, columns)
        # --delimiter to specify character that splits the fields
        cmd = "sacct -j %s -o JobID,STATE -X -n -P --delimiter=," % jobIDs
        sp = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, error = sp.communicate()
        status = sp.returncode
        if status != 0:
            resultDict["Status"] = 1
            resultDict["Message"] = error
            return resultDict

        statusDict = {}
        lines = output.strip().split("\n")
        jids = set()
        for line in lines:
            jid, status = line.split(",")
            jids.add(jid)
            if jid in jobIDList:
                if status in ["PENDING", "SUSPENDED", "CONFIGURING"]:
                    statusDict[jid] = "Waiting"
                elif status in ["RUNNING", "COMPLETING"]:
                    statusDict[jid] = "Running"
                elif status in ["CANCELLED", "PREEMPTED"]:
                    statusDict[jid] = "Aborted"
                elif status in ["COMPLETED"]:
                    statusDict[jid] = "Done"
                elif status in ["FAILED", "TIMEOUT", "NODE_FAIL"]:
                    statusDict[jid] = "Failed"
                else:
                    statusDict[jid] = "Unknown"

        leftJobs = set(jobIDList) - jids
        for jid in leftJobs:
            statusDict[jid] = "Unknown"

        # Final output
        resultDict["Status"] = 0
        resultDict["Jobs"] = statusDict
        return resultDict

    def getCEStatus(self, **kwargs):
        """Get the overall status of the CE"""

        resultDict = {}

        MANDATORY_PARAMETERS = ["Queue"]
        for argument in MANDATORY_PARAMETERS:
            if argument not in kwargs:
                resultDict["Status"] = -1
                resultDict["Message"] = "No %s" % argument
                return resultDict

        user = kwargs.get("User")
        if not user:
            user = os.environ.get("USER")
        if not user:
            resultDict["Status"] = -1
            resultDict["Message"] = "No user name"
            return resultDict

        queue = kwargs["Queue"]

        cmd = "squeue --partition=%s --user=%s --format='%%j %%T' " % (queue, user)
        sp = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        output, error = sp.communicate()
        status = sp.returncode
        if status != 0:
            resultDict["Status"] = 1
            resultDict["Message"] = error
            return resultDict

        waitingJobs = 0
        runningJobs = 0
        lines = output.split("\n")
        for line in lines[1:]:
            _jid, status = line.split()
            if status in ["PENDING", "SUSPENDED", "CONFIGURING"]:
                waitingJobs += 1
            elif status in ["RUNNING", "COMPLETING"]:
                runningJobs += 1

        # Final output
        resultDict["Status"] = 0
        resultDict["Waiting"] = waitingJobs
        resultDict["Running"] = runningJobs
        return resultDict

    def getJobOutputFiles(self, **kwargs):
        """Get output file names and templates for the specific CE

        Reorder the content of the output files according to the node identifier
        if multiple nodes were involved.

        From:
        >>> 1: line1
        >>> 2: line1
        >>> 1: line2
        To:
        >>> # On node 1
        >>>   line1
        >>>   line2
        >>> # On node 2
        >>>   line1
        """
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
        numberOfNodes = kwargs.get("NumberOfNodes", "1")

        jobDict = {}
        for jobID in jobIDList:
            output = "%s/%s.out" % (outputDir, jobID)
            error = "%s/%s.err" % (errorDir, jobID)

            if numberOfNodes != "1":
                self._openFileAndSortOutput(output)
                self._openFileAndSortOutput(error)

            jobDict[jobID] = {}
            jobDict[jobID]["Output"] = output
            jobDict[jobID]["Error"] = error

        resultDict["Status"] = 0
        resultDict["Jobs"] = jobDict
        return resultDict

    def _openFileAndSortOutput(self, outputFile):
        """
        Open a file, get its content and reorder it according to the node identifiers

        :param str outputFile: name of the file to sort
        """
        with open(outputFile, "r") as f:
            outputContent = f.read()

        sortedContent = self._sortOutput(outputContent)

        with open(outputFile, "w") as f:
            f.write(sortedContent)

    def _sortOutput(self, outputContent):
        """
        Reorder the content of the output file according to the node identifiers

        :param str outputContent: content to sort
        :return str: content sorted
        """
        outputLines = outputContent.strip().split("\n")
        nodes = {}
        for line in outputLines:
            node, line_content = line.split(":", 1)

            # if node is not a number, then it is a general information (e.g. timeout)
            # else, it is related to a specific node
            if not node.isdigit():
                key = "General Information"
                line_content = line
            else:
                key = "Node %s" % node

            if key not in nodes:
                nodes[key] = []
            nodes[key].append(line_content)
            print(nodes)

        content = ""
        for node, lines in nodes.items():
            content += "# %s\n\n" % node
            content += "\n".join(lines) + "\n"
        return content
