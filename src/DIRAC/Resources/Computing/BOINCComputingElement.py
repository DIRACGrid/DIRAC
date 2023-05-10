""" BOINC Computing Element
"""

import os
import bz2
import base64
import tempfile

from urllib.parse import urlparse

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.ComputingElement import ComputingElement

CE_NAME = "BOINC"


class BOINCComputingElement(ComputingElement):
    ###############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""

        super().__init__(ceUniqueID)

        self.ceType = CE_NAME
        self.mandatoryParameters = []
        self.wsdl = None
        self.BOINCClient = None
        # define a job prefix based on the wsdl url
        self.suffix = None

    # this is for standlone test
    #    self.ceParameters['projectURL'] = 'http://mardirac3.in2p3.fr:7788/?wsdl'
    #    self.ceParameters['Platform'] = 'Linux_x86_64_glibc-2.5'

    def createClient(self):
        """
        This method only can be called after the initialisation of this class. In this
        method, it will initial some variables and create a soap client for communication
        with BOINC server.
        """

        if not self.wsdl:
            self.wsdl = self.ceParameters["projectURL"]
        if not self.suffix:
            result = urlparse(self.wsdl)
            self.suffix = result.hostname
        if not self.BOINCClient:
            try:
                from suds.client import Client

                self.BOINCClient = Client(self.wsdl)
            except Exception as x:
                self.log.error("Creation of the soap client failed", f"{str(x)}")
                pass

    def submitJob(self, executableFile, proxy=None, numberOfJobs=1):
        """Method to submit job"""
        self.createClient()
        # Check if the client is ready
        if not self.BOINCClient:
            return S_ERROR("Soap client is not ready")

        self.log.verbose(f"Executable file path: {executableFile}")

        # if no proxy is supplied, the executable can be submitted directly
        # otherwise a wrapper script is needed to get the proxy to the execution node
        # The wrapper script makes debugging more complicated and thus it is
        # recommended to transfer a proxy inside the executable if possible.
        wrapperContent = ""
        if proxy:
            self.log.verbose("Setting up proxy for payload")

            compressedAndEncodedProxy = base64.b64encode(bz2.compress(proxy.dumpAllToString()["Value"])).decode()
            with open(executableFile, "rb") as fp:
                compressedAndEncodedExecutable = base64.b64encode(bz2.compress(fp.read(), 9)).decode()

            wrapperContent = """#!/bin/bash
/usr/bin/env python << EOF
# Wrapper script for executable and proxy
import os
import tempfile
import sys
import base64
import bz2
import shutil
import stat
try:
  workingDirectory = tempfile.mkdtemp( suffix = '_wrapper', prefix= 'TORQUE_' )
  os.chdir( workingDirectory )
  open( 'proxy', "w" ).write(bz2.decompress( base64.b64decode( "{compressedAndEncodedProxy}" ) ) )
  open( '{executable}', "w" ).write(bz2.decompress( base64.b64decode( "{compressedAndEncodedExecutable}" ) ) )
  os.chmod('proxy',stat.S_IRUSR | stat.S_IWUSR)
  os.chmod('{executable}',stat.S_IRWXU)
  os.environ["X509_USER_PROXY"]=os.path.join(workingDirectory, 'proxy')
except Exception as x:
  print >> sys.stderr, x
  sys.exit(-1)
cmd = "./{executable}"
print 'Executing: ', cmd
sys.stdout.flush()
os.system( cmd )

shutil.rmtree( workingDirectory )

EOF
""".format(
                compressedAndEncodedProxy=compressedAndEncodedProxy,
                compressedAndEncodedExecutable=compressedAndEncodedExecutable,
                executable=os.path.basename(executableFile),
            )

            fd, name = tempfile.mkstemp(suffix="_pilotwrapper.py", prefix="DIRAC_", dir=os.getcwd())
            os.close(fd)
            submitFile = name

        else:  # no proxy
            submitFile = executableFile
            wrapperContent = self._fromFileToStr(submitFile)

        if not wrapperContent:
            self.log.error("Executable file is empty.")
            return S_ERROR("Executable file is empty.")

        # Some special symbol can not be transported by xml,
        # such as less, greater, amp. So, base64 is used here.
        wrapperContent = base64.b64encode(wrapperContent).decode()

        prefix = os.path.splitext(os.path.basename(submitFile))[0].replace("_pilotwrapper", "").replace("DIRAC_", "")
        batchIDList = []
        stampDict = {}
        for i in range(0, numberOfJobs):
            jobID = "%s_%d@%s" % (prefix, i, self.suffix)
            try:
                #  print jobID + "\n" + wrapperContent
                #  print self.BOINCClient
                result = self.BOINCClient.service.submitJob(
                    jobID, wrapperContent, self.ceParameters["Platform"][0], self.ceParameters["MarketPlaceID"]
                )
            except Exception:
                self.log.error("Could not submit the pilot to the BOINC CE", f"Pilot {jobID}, BOINC CE {self.wsdl}")
                break

            if not result["ok"]:
                self.log.warn(
                    f"Didn't submit the pilot {jobID} to the BOINC CE {self.wsdl}, the value returned is false!"
                )
                break

            self.log.verbose(f"Submit the pilot {jobID} to the BOINC CE {self.wsdl}")
            diracStamp = "%s_%d" % (prefix, i)
            batchIDList.append(jobID)
            stampDict[jobID] = diracStamp

        if batchIDList:
            resultRe = S_OK(batchIDList)
            resultRe["PilotStampDict"] = stampDict
        else:
            resultRe = S_ERROR(f"Submit no pilot to BOINC CE {self.wsdl}")
        return resultRe

    #############################################################################
    def getCEStatus(self):
        """Method to get the BOINC CE dynamic jobs information."""
        self.createClient()
        # Check if the client is ready
        if not self.BOINCClient:
            self.log.error("Soap client is not ready.")
            return S_ERROR("Soap client is not ready.")

        try:
            result = self.BOINCClient.service.getDynamicInfo()
        except Exception:
            self.log.error("Could not get the BOINC CE dynamic jobs information", self.wsdl)
            return S_ERROR(f"Could not get the BOINC CE {self.wsdl} dynamic jobs information, communication failed!")

        if not result["ok"]:
            self.log.warn(
                f"Did not get the BOINC CE {self.wsdl} dynamic jobs information, the value returned is false!"
            )
            return S_ERROR(
                f"Did not get the BOINC CE {self.wsdl} dynamic jobs information, the value returned is false!"
            )

        self.log.verbose(f"Get the BOINC CE {self.wsdl} dynamic jobs info.")

        resultRe = S_OK()
        resultRe["WaitingJobs"] = result["values"][0][0]
        resultRe["RunningJobs"] = result["values"][0][1]
        resultRe["SubmittedJobs"] = 0
        self.log.verbose("Waiting Jobs: ", resultRe["WaitingJobs"])
        self.log.verbose("Running Jobs: ", resultRe["RunningJobs"])
        return resultRe

    #############################################################################
    def getJobStatus(self, jobIDList):
        """Get the status information about jobs in the given list"""
        self.createClient()
        # Check if the client is ready
        if not self.BOINCClient:
            self.log.error("Soap client is not ready.")
            return S_ERROR("Soap client is not ready.")

        wsdl_jobIDList = self.BOINCClient.factory.create("stringArray")
        for job in jobIDList:
            try:
                job = job.split("@")[0]
            except Exception:
                self.log.debug(f"The job id is {job}")
                pass
            wsdl_jobIDList[0].append(job)

        try:
            result = self.BOINCClient.service.getJobStatus(wsdl_jobIDList)
        except Exception:
            self.log.error("Could not get the status about jobs in the list from the BOINC CE", self.wsdl)
            return S_ERROR(
                f"Could not get the status about jobs in the list from the BOINC CE {self.wsdl}, commnication failed!"
            )

        if not result["ok"]:
            self.log.warn(
                "Did not get the status about jobs in the list from the BOINC CE %s, the value returned is false!"
                % self.wsdl
            )
            return S_ERROR(
                "Did not get the status about jobs in the list from the BOINC CE %s, the value returned is false!"
                % self.wsdl
            )
        self.log.debug(f"Got the status about jobs in list from the BOINC CE {self.wsdl}.")
        resultRe = {}
        for jobStatus in result["values"][0]:
            (jobID, status) = jobStatus.split(":")
            jobID = f"{jobID}@{self.suffix}"
            resultRe[jobID] = status

        return S_OK(resultRe)

    #############################################################################
    def getJobOutput(self, jobID, localDir=None):
        """Get the stdout and stderr outputs of the specified job . If the localDir is provided,
        the outputs are stored as files in this directory and the name of the files are returned.
        Otherwise, the outputs are returned as strings.
        """
        self.createClient()
        # Check if the client is ready
        if not self.BOINCClient:
            self.log.error("Soap client is not ready.")
            return S_ERROR("Soap client is not ready.")

        try:
            tempID = jobID.split("@")[0]
        except Exception:
            tempID = jobID

        try:
            result = self.BOINCClient.service.getJobOutput(tempID)
        except Exception:
            self.log.error("Could not get the outputs of job from the BOINC CE", f"Job {jobID}, BOINC CE {self.wsdl}")
            return S_ERROR(
                f"Could not get the outputs of job {jobID} from the BOINC CE {self.wsdl}, communication failed!"
            )
        if not result["ok"]:
            self.log.warn(
                "Did not get the outputs of job %s from the BOINC CE %s, the value returned is false!"
                % (jobID, self.wsdl)
            )
            return S_ERROR(
                "Did not get the outputs of job %s from the BOINC CE %s, the value returned is false!"
                % (jobID, self.wsdl)
            )

        self.log.debug(f"Got the outputs of job {jobID} from the BOINC CE {self.wsdl}.")

        strOutfile = base64.b64decode(result["values"][0][0])
        strErrorfile = base64.b64decode(result["values"][0][1])
        if localDir:
            outFile = os.path.join(localDir, f"BOINC_{jobID}.out")
            self._fromStrToFile(strOutfile, outFile)

            errorFile = os.path.join(localDir, f"BOINC_{jobID}.err")
            self._fromStrToFile(strErrorfile, errorFile)

            return S_OK((outFile, errorFile))
        else:
            # Return the outputs as a string
            return S_OK((strOutfile, strErrorfile))

    ##############################################################################
    def _fromFileToStr(self, fileName):
        """Read a file and return the file content as a string"""
        strFile = b""
        try:
            with open(fileName, "rb") as fileHander:
                strFile = fileHander.read()
        except Exception:
            self.log.verbose(f"To read file {fileName} failed!\n")
        return strFile

    #####################################################################
    def _fromStrToFile(self, strContent, fileName):
        """Write a string to a file"""
        try:
            with open(fileName, "wb") as fileHander:
                _ = fileHander.write(strContent)
        except Exception:
            self.log.verbose(f"To create {fileName} failed!")


# testing this
if __name__ == "__main__":
    test_boinc = BOINCComputingElement(12)
    test_submit = 1
    test_getStatus = 2
    test_getDynamic = 4
    test_getOutput = 8
    test_parameter = 4
    jobID = "zShvbK_0@mardirac3.in2p3.fr"
    if test_parameter & test_submit:
        fd, fname = tempfile.mkstemp(suffix="_pilotwrapper.py", prefix="DIRAC_", dir="/home/client/dirac/data/")
        os.close(fd)
        fd = open(fname, "w")
        fd.write('#!/usr/bin/env sh\necho "I am stadard out" &gt;&amp;1 \necho "I am stadard error" &gt;&amp;2 ')
        fd.close()
        result = test_boinc.submitJob(fname)

        if not result["OK"]:
            print(result["Message"])
        else:
            jobID = result["Value"][0]
            print(f"Successfully submit a job {jobID}")

    if test_parameter & test_getStatus:
        jobTestList = ["Uu0ghO_0@mardirac3.in2p3.fr", "1aDmIf_0@mardirac3.in2p3.fr", jobID]
        jobStatus = test_boinc.getJobStatus(jobTestList)
        if not jobStatus["OK"]:
            print(jobStatus["Message"])
        else:
            for _ in jobTestList:
                print(f"The status of the job {id} is {jobStatus['Value'][id]}")

    if test_parameter & test_getDynamic:
        serverState = test_boinc.getCEStatus()

        if not serverState["OK"]:
            print(serverState["Message"])
        else:
            print(f"The number of jobs waiting is {serverState['WaitingJobs']}")
            print(f"The number of jobs running is {serverState['RunningJobs']}")

    if test_parameter & test_getOutput:
        outstate = test_boinc.getJobOutput(jobID, "/tmp/")

        if not outstate["OK"]:
            print(outstate["Message"])
        else:
            print(f"Please check the directory /tmp for the output and error files of job {jobID}")
