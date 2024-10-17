""" Starts a DIRAC command inside an apptainer container.
"""

import os
import shutil
import sys

import DIRAC
from DIRAC import S_ERROR, gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.Os import findImage
from DIRAC.Core.Utilities.Subprocess import systemCall

CONTAINER_WRAPPER = """#!/bin/bash

echo "Starting inner container wrapper scripts (no install) at `date`."
export DIRAC=%(dirac_env_var)s
export DIRACOS=%(diracos_env_var)s
# In any case we need to find a bashrc, and a cfg
source %(rc_script)s
%(command)s
echo "Finishing inner container wrapper scripts at `date`."
"""

CONTAINER_DEFROOT = "/cvmfs/dirac.egi.eu/container/apptainer/alma9/x86_64"


def getEnv():
    """Gets the environment for use within the container.
    We blank almost everything to prevent contamination from the host system.
    """

    payloadEnv = {k: v for k, v in os.environ.items()}
    payloadEnv["TMP"] = "/tmp"
    payloadEnv["TMPDIR"] = "/tmp"
    payloadEnv["X509_USER_PROXY"] = os.path.join("tmp", "proxy")
    payloadEnv["DIRACSYSCONFIG"] = os.path.join("tmp", "dirac.cfg")

    return payloadEnv


@Script()
def main():
    Script.registerArgument(" command: Command to execute inside the container")
    command = Script.getPositionalArgs(group=True)

    wrapSubs = {
        "dirac_env_var": os.environ.get("DIRAC", os.getcwd()),
        "diracos_env_var": os.environ.get("DIRACOS", os.getcwd()),
    }
    wrapSubs["rc_script"] = os.path.join(os.path.realpath(sys.base_prefix), "diracosrc")
    wrapSubs["command"] = command
    shutil.copyfile("dirac.cfg", os.path.join("tmp", "dirac.cfg"))

    wrapLoc = os.path.join("tmp", "dirac_container.sh")
    rawfd = os.open(wrapLoc, os.O_WRONLY | os.O_CREAT, 0o700)
    fd = os.fdopen(rawfd, "w")
    fd.write(CONTAINER_WRAPPER % wrapSubs)
    fd.close()

    innerCmd = os.path.join("tmp", "dirac_container.sh")
    cmd = ["apptainer", "exec"]
    cmd.extend(["--contain"])  # use minimal /dev and empty other directories (e.g. /tmp and $HOME)
    cmd.extend(["--ipc"])  # run container in a new IPC namespace
    cmd.extend(["--workdir", "/tmp"])  # working directory to be used for /tmp, /var/tmp and $HOME
    cmd.extend(["--home", "/tmp"])  # Avoid using small tmpfs for default $HOME and use scratch /tmp instead
    cmd.extend(["--bind", "{0}:{0}:ro".format(os.path.join(os.path.realpath(sys.base_prefix)))])

    rootImage = findImage() or CONTAINER_DEFROOT

    if os.path.isdir(rootImage) or os.path.isfile(rootImage):
        cmd.extend([rootImage, innerCmd])
    else:
        # if we are here is because there's no image, or it is not accessible (e.g. not on CVMFS)
        gLogger.error("Apptainer image to exec not found: ", rootImage)
        return S_ERROR("Failed to find Apptainer image to exec")

    gLogger.debug(f"Execute Apptainer command: {cmd}")
    result = systemCall(0, cmd, env=getEnv())
    if not result["OK"]:
        DIRAC.exit(1)


if __name__ == "__main__":
    main()
