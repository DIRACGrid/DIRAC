""" Collection of user jobs for testing purposes
"""

# pylint: disable=invalid-name

import os
import time
import errno

from DIRAC import rootPath
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

from DIRAC.tests.Utilities.utils import find_all


def getJob(jobClass=None):
    if not jobClass:
        jobClass = Job
    oJob = jobClass()
    return oJob


def getDIRAC(diracClass=None):
    if not diracClass:
        diracClass = Dirac
    oDirac = diracClass()
    return oDirac


def baseToAllJobs(jName, jobClass=None):
    print("**********************************************************************************************************")
    print("\n Submitting job ", jName)

    J = getJob(jobClass)
    J.setName(jName)
    J.setCPUTime(17800)
    return J


def endOfAllJobs(J):
    result = getDIRAC().submitJob(J)
    print("Job submission result:", result)
    if result["OK"]:
        print("Submitted with job ID:", result["Value"])

    return result


# List of jobs


def helloWorld():
    """simple hello world job"""

    J = baseToAllJobs("helloWorld")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setExecutable("exe-script.py", "", "helloWorld.log")
    return endOfAllJobs(J)


def helloWorldCERN():
    """simple hello world job to CERN"""

    J = baseToAllJobs("helloWorldCERN")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setExecutable("exe-script.py", "", "helloWorld.log")
    J.setDestination("LCG.CERN.cern")
    return endOfAllJobs(J)


def helloWorldNCBJ():
    """simple hello world job to NCBJ"""

    J = baseToAllJobs("helloWorldNCBJ")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setExecutable("exe-script.py", "", "helloWorld.log")
    J.setDestination("LCG.NCBJ.pl")
    return endOfAllJobs(J)


def helloWorldGRIDKA():
    """simple hello world job to GRIDKA"""

    J = baseToAllJobs("helloWorldGRIDKA")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setExecutable("exe-script.py", "", "helloWorld.log")
    J.setDestination("LCG.GRIDKA.de")
    return endOfAllJobs(J)


def helloWorldGRIF():
    """simple hello world job to GRIF"""

    J = baseToAllJobs("helloWorldGRIF")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setExecutable("exe-script.py", "", "helloWorld.log")
    J.setDestination("LCG.GRIF.fr")
    return endOfAllJobs(J)


def helloWorldSSHBatch():
    """simple hello world job to DIRAC.Jenkins_SSHBatch.ch"""

    J = baseToAllJobs("helloWorldSSHBatch")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setExecutable("exe-script.py", "", "helloWorld.log")
    J.setDestination("DIRAC.Jenkins_SSHBatch.ch")
    return endOfAllJobs(J)


def helloWorldCloudDirector():
    """simple hello world job to Cloud at Imperial College using VMDIRAC/CloudDirector"""

    J = baseToAllJobs("helloWorldCloudDirector")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setExecutable("exe-script.py", "", "helloWorldCloud.log")
    J.setDestination("CLOUD.UKI-LT2-IC-HEP.uk")
    return endOfAllJobs(J)


def helloWorldCloudCE():
    """simple hello world job to Cloud at Imperial College using SiteDirector"""

    J = baseToAllJobs("helloWorldCloudCE")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setExecutable("exe-script.py", "", "helloWorldCloud.log")
    J.setDestinationCE("stealthcloud.ic.ac.uk", "LCG.UKI-LT2-IC-HEP.uk")
    return endOfAllJobs(J)


def mpJob():
    """simple hello world job, with 4Processors and MultiProcessor tags"""

    J = baseToAllJobs("mpJob")
    try:
        J.setInputSandbox([find_all("mpTest.py", rootPath, "DIRAC/tests/Utilities")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("mpTest.py", ".", "DIRAC/tests/Utilities")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("mpTest.py", os.environ["WORKSPACE"], "DIRAC/tests/Utilities")[0]])

    J.setExecutable("mpTest.py")
    J.setTag(["4Processors", "MultiProcessor"])
    return endOfAllJobs(J)


def mp3Job():
    """simple hello world job, with 2 to 4 processors"""

    J = baseToAllJobs("min2max4Job")
    try:
        J.setInputSandbox([find_all("mpTest.py", rootPath, "DIRAC/tests/Utilities")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("mpTest.py", ".", "DIRAC/tests/Utilities")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("mpTest.py", os.environ["WORKSPACE"], "DIRAC/tests/Utilities")[0]])

    J.setExecutable("mpTest.py")
    J.setNumberOfProcessors(numberOfProcessors=3)
    return endOfAllJobs(J)


def min2max4Job():
    """simple hello world job, with 2 to 4 processors"""

    J = baseToAllJobs("min2max4Job")
    try:
        J.setInputSandbox([find_all("mpTest.py", rootPath, "DIRAC/tests/Utilities")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("mpTest.py", ".", "DIRAC/tests/Utilities")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("mpTest.py", os.environ["WORKSPACE"], "DIRAC/tests/Utilities")[0]])

    J.setExecutable("mpTest.py")
    J.setNumberOfProcessors(minNumberOfProcessors=2, maxNumberOfProcessors=4)
    return endOfAllJobs(J)


def wholeNodeJob():
    """simple hello world job, with WholeNode and MultiProcessor tags"""

    J = baseToAllJobs("wholeNodeJob")
    try:
        J.setInputSandbox([find_all("mpTest.py", rootPath, "DIRAC/tests/Utilities")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("mpTest.py", ".", "DIRAC/tests/Utilities")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("mpTest.py", os.environ["WORKSPACE"], "DIRAC/tests/Utilities")[0]])

    J.setExecutable("mpTest.py")
    J.setTag(["WholeNode", "MultiProcessor"])
    return endOfAllJobs(J)


def parametricJob():
    """Creates a parametric job with 3 subjobs which are simple hello world jobs"""

    J = baseToAllJobs("parametricJob")
    try:
        J.setInputSandbox([find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]])
    except IndexError:
        try:
            J.setInputSandbox([find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]])
        except IndexError:  # we are in Jenkins
            J.setInputSandbox([find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]])
    J.setParameterSequence("args", ["one", "two", "three"])
    J.setParameterSequence("iargs", [1, 2, 3])
    J.setExecutable("exe-script.py", arguments=": testing %(args)s %(iargs)s", logFile="helloWorld_%n.log")
    return endOfAllJobs(J)


def jobWithOutput():
    """Creates a job that uploads an output.
    The output SE is not set here, so it would use the default /Resources/StorageElementGroups/SE-USER
    And possibly use for failover /Resources/StorageElementGroups/Tier1-Failover
    """

    timenow = time.strftime("%s")
    inp1 = [os.path.join(os.getcwd(), timenow + "testFileUpload.txt")]
    with open(inp1[0], "w") as f:
        f.write(timenow)

    J = baseToAllJobs("jobWithOutput")
    try:
        inp2 = [find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]]
        J.setInputSandbox(inp1 + inp2)
    except IndexError:
        try:
            inp2 = [find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]]
        except IndexError:  # we are in Jenkins
            inp2 = [find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]]
        J.setInputSandbox(inp1 + inp2)
    J.setExecutable("exe-script.py", "", "helloWorld.log")
    J.setOutputData([timenow + "testFileUpload.txt"])
    res = endOfAllJobs(J)
    try:
        os.remove(os.path.join(os.getcwd(), timenow + "testFileUpload.txt"))
    except OSError as e:
        return e.errno == errno.ENOENT
    return res


def jobWithOutputs():
    """Creates a job that uploads an output to 2 SEs.
    The output SE is set here to ['RAL-SE', 'IN2P3-SE']
    """

    timenow = time.strftime("%s")
    inp1 = [os.path.join(os.getcwd(), timenow + "testFileUpload.txt")]
    with open(inp1[0], "w") as f:
        f.write(timenow)

    J = baseToAllJobs("jobWithOutputs")
    try:
        inp2 = [find_all("exe-script.py", rootPath, "DIRAC/tests/Workflow")[0]]
        J.setInputSandbox(inp1 + inp2)
    except IndexError:
        try:
            inp2 = [find_all("exe-script.py", ".", "DIRAC/tests/Workflow")[0]]
        except IndexError:  # we are in Jenkins
            inp2 = [find_all("exe-script.py", os.environ["WORKSPACE"], "DIRAC/tests/Workflow")[0]]
        J.setInputSandbox(inp1 + inp2)
    J.setExecutable("exe-script.py", "", "helloWorld.log")
    J.setOutputData([timenow + "testFileUpload.txt"], outputSE=["RAL-SE", "IN2P3-SE"])
    res = endOfAllJobs(J)
    try:
        os.remove(os.path.join(os.getcwd(), timenow + "testFileUpload.txt"))
    except OSError as e:
        return e.errno == errno.ENOENT
    return res
