"""
   Collection of DIRAC useful operating system related modules
   by default on Error they return None
"""
import os
import threading

import DIRAC
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.Subprocess import shellCall, systemCall

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
    comm = f"df -P -m {path} "
    if exclude:
        comm += f"-x {exclude} "
    comm += "| tail -1"
    resultDF = shellCall(10, comm)
    if not resultDF["OK"] or resultDF["Value"][0]:
        return -1
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
        return -1
    fields = output.split()
    try:
        value = int(fields[3])
    except Exception as error:
        print("Exception during disk space evaluation:", str(error))
        value = -1
    return value


def getDirectorySize(path):
    """Get the total size of the given directory in MB"""

    comm = f"du -s -m {path}"
    result = shellCall(10, comm)
    if not result["OK"] or result["Value"][0] != 0:
        return 0
    output = result["Value"][1]
    print(output)
    return int(output.split()[0])


def sourceEnv(timeout, cmdTuple, inputEnv=None):
    """Function to source configuration files in a platform dependent way and get
    back the environment
    """

    # add appropriate extension to first element of the tuple (the command)
    envAsDict = '&& python -c "import os,sys ; print >> sys.stderr, os.environ"'

    cmdTuple[0] += ".sh"

    # 2.- Check that it exists
    if not os.path.exists(cmdTuple[0]):
        result = DIRAC.S_ERROR(f"Missing script: {cmdTuple[0]}")
        result["stdout"] = ""
        result["stderr"] = f"Missing script: {cmdTuple[0]}"
        return result

    # Source it in a platform dependent way:
    # On Linux or Darwin use bash and source the file.
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
            result = DIRAC.S_ERROR(f"Execution returns {ret['Value'][0]}")
    else:
        # Timeout
        stdout = cmd
        stderr = ret["Message"]
        result = DIRAC.S_ERROR(stderr)

    # 4.- Put stdout and stderr in result structure
    result["stdout"] = stdout
    result["stderr"] = stderr

    return result


def safe_listdir(directory, timeout=60):
    """This is a "safe" list directory,
    for lazily-loaded File Systems like CVMFS.
    There's by default a 60 seconds timeout.

    .. warning::
        There is no distinction between an empty directory, and a non existent one.
        It will return `[]` in both cases.

    :param str directory: directory to list
    :param int timeout: optional timeout, in seconds. Defaults to 60.
    """

    def listdir(directory):
        try:
            return os.listdir(directory)
        except FileNotFoundError:
            print(f"{directory} not found")
            return []

    contents = []
    t = threading.Thread(target=lambda: contents.extend(listdir(directory)))
    t.daemon = True  # don't delay program's exit
    t.start()
    t.join(timeout)
    if t.is_alive():
        return None  # timeout
    return contents
