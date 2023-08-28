# pylint: disable=protected-access
import tempfile

from DIRAC.Interfaces.API.Job import Job
from DIRAC.tests.Utilities.utils import find_all


def helloWorldJob():
    job = Job()
    job.setName("helloWorld")
    exeScriptLocation = find_all("exe-script.py", "..", "/DIRAC/tests/Integration")[0]
    job.setInputSandbox(exeScriptLocation)
    job.setExecutable(exeScriptLocation, "", "helloWorld.log")
    return job


def parametricJob():
    job = Job()
    job.setName("parametric_helloWorld_%n")
    exeScriptLocation = find_all("exe-script.py", "..", "/DIRAC/tests/Integration")[0]
    job.setInputSandbox(exeScriptLocation)
    job.setParameterSequence("args", ["one", "two", "three"])
    job.setParameterSequence("iargs", [1, 2, 3])
    job.setExecutable(exeScriptLocation, arguments=": testing %(args)s %(iargs)s", logFile="helloWorld_%n.log")
    return job


def createFile(job):
    tmpdir = tempfile.mkdtemp()
    jobDescription = tmpdir + "/jobDescription.xml"
    with open(jobDescription, "w") as fd:
        fd.write(job._toXML())
    return jobDescription
