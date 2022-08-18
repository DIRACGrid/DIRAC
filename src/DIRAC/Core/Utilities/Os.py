"""
   Collection of DIRAC useful operating system related modules
   by default on Error they return None
"""
import os
import shutil

import DIRAC
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.Subprocess import shellCall, systemCall
from DIRAC.Core.Utilities import List

DEBUG = 0


def uniquePath(path=None):
    """
    Utility to squeeze the string containing a PATH-like value to
    leave only unique elements preserving the original order
    """
    if not isinstance(path, str):
        return None

    try:
        elements = List.uniqueElements(List.fromChar(path, ":"))
        return ":".join(elements)
    except Exception:
        return None


def getDiskSpace(path=".", exclude=None):
    """Get the free disk space in the partition containing the path.
    The disk space is reported in MBytes. Returned 0 in case of any
    error, e.g. path does not exist
    """

    if not os.path.exists(path):
        return -1
    comm = "df -P -m %s " % path
    if exclude:
        comm += "-x %s " % exclude
    comm += "| tail -1"
    resultDF = shellCall(10, comm)
    if resultDF["OK"] and not resultDF["Value"][0]:
        output = resultDF["Value"][1]
        if output.find(" /afs") >= 0:  # AFS disk space
            comm = "fs lq | tail -1"
            resultAFS = shellCall(10, comm)
            if resultAFS["OK"] and not resultAFS["Value"][0]:
                output = resultAFS["Value"][1]
                fields = output.split()
                quota = int(fields[1])
                used = int(fields[2])
                space = (quota - used) / 1024
                return int(space)
            else:
                return -1
        else:
            fields = output.split()
            try:
                value = int(fields[3])
            except Exception as error:
                print("Exception during disk space evaluation:", str(error))
                value = -1
            return value
    else:
        return -1


def getDirectorySize(path):
    """Get the total size of the given directory in MB"""

    comm = "du -s -m %s" % path
    result = shellCall(10, comm)
    if not result["OK"] or result["Value"][0] != 0:
        return 0
    else:
        output = result["Value"][1]
        print(output)
        size = int(output.split()[0])
        return size


def sourceEnv(timeout, cmdTuple, inputEnv=None):
    """Function to source configuration files in a platform dependent way and get
    back the environment
    """

    # add appropriate extension to first element of the tuple (the command)
    envAsDict = '&& python -c "import os,sys ; print >> sys.stderr, os.environ"'

    # 1.- Choose the right version of the configuration file
    if DIRAC.getPlatformTuple()[0] == "Windows":
        cmdTuple[0] += ".bat"
    else:
        cmdTuple[0] += ".sh"

    # 2.- Check that it exists
    if not os.path.exists(cmdTuple[0]):
        result = DIRAC.S_ERROR("Missing script: %s" % cmdTuple[0])
        result["stdout"] = ""
        result["stderr"] = "Missing script: %s" % cmdTuple[0]
        return result

    # Source it in a platform dependent way:
    # On windows the execution makes the environment to be inherit
    # On Linux or Darwin use bash and source the file.
    if DIRAC.getPlatformTuple()[0] == "Windows":
        # this needs to be tested
        cmd = " ".join(cmdTuple) + envAsDict
        ret = shellCall(timeout, [cmd], env=inputEnv)
    else:
        cmdTuple.insert(0, "source")
        cmd = " ".join(cmdTuple) + envAsDict
        ret = systemCall(timeout, ["/bin/bash", "-c", cmd], env=inputEnv)

    # 3.- Now get back the result
    stdout = ""
    stderr = ""
    result = DIRAC.S_OK()
    if ret["OK"]:
        # The Command has not timeout, retrieve stdout and stderr
        stdout = ret["Value"][1]
        stderr = ret["Value"][2]
        if ret["Value"][0] == 0:
            # execution was OK
            try:
                result["outputEnv"] = eval(stderr.split("\n")[-2] + "\n")
                stderr = "\n".join(stderr.split("\n")[:-2])
            except Exception:
                stdout = cmd + "\n" + stdout
                result = DIRAC.S_ERROR("Could not parse Environment dictionary from stderr")
        else:
            # execution error
            stdout = cmd + "\n" + stdout
            result = DIRAC.S_ERROR("Execution returns %s" % ret["Value"][0])
    else:
        # Timeout
        stdout = cmd
        stderr = ret["Message"]
        result = DIRAC.S_ERROR(stderr)

    # 4.- Put stdout and stderr in result structure
    result["stdout"] = stdout
    result["stderr"] = stderr

    return result


@deprecated("Will be removed in DIRAC 8.1", onlyOnce=True)
def which(executable):
    return shutil.which(executable)
