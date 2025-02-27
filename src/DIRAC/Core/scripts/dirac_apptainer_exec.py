""" Starts a DIRAC command inside an apptainer container.
"""

import os
import sys

import DIRAC
from DIRAC import S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security.Locations import getCAsLocation, getProxyLocation, getVOMSLocation
from DIRAC.Core.Utilities.Subprocess import systemCall

CONTAINER_WRAPPER = """#!/bin/bash

export DIRAC=%(dirac_env_var)s
export DIRACOS=%(diracos_env_var)s
export X509_USER_PROXY=/etc/proxy
export X509_CERT_DIR=/etc/grid-security/certificates
export X509_VOMS_DIR=/etc/grid-security/vomsdir
export DIRACSYSCONFIG=%(etc_dir)s/dirac.cfg
source %(rc_script)s
%(command)s
"""

CONTAINER_DEFROOT = ""  # Should add something like "/cvmfs/dirac.egi.eu/container/apptainer/alma9/x86_64"


@Script()
def main():
    command = sys.argv[1]

    user_image = None
    Script.registerSwitch("i:", "image=", "   apptainer image to use")
    Script.parseCommandLine(ignoreErrors=False)
    for switch in Script.getUnprocessedSwitches():
        if switch[0].lower() == "i" or switch[0].lower() == "image":
            user_image = switch[1]

    etc_dir = os.path.join(DIRAC.rootPath, "etc")

    wrapSubs = {
        "dirac_env_var": os.environ.get("DIRAC", os.getcwd()),
        "diracos_env_var": os.environ.get("DIRACOS", os.getcwd()),
        "etc_dir": etc_dir,
    }
    wrapSubs["rc_script"] = os.path.join(os.path.realpath(sys.base_prefix), "diracosrc")
    wrapSubs["command"] = command

    rawfd = os.open("dirac_container.sh", os.O_WRONLY | os.O_CREAT, 0o700)
    fd = os.fdopen(rawfd, "w")
    fd.write(CONTAINER_WRAPPER % wrapSubs)
    fd.close()

    cmd = ["apptainer", "exec"]
    cmd.extend(["--contain"])  # use minimal /dev and empty other directories (e.g. /tmp and $HOME)
    cmd.extend(["--ipc"])  # run container in a new IPC namespace
    cmd.extend(["--bind", f"{os.getcwd()}:/mnt"])  # bind current directory for dirac_container.sh
    cmd.extend(["--bind", f"{getProxyLocation()}:/etc/proxy"])  # bind proxy file
    cmd.extend(["--bind", f"{getCAsLocation()}:/etc/grid-security/certificates"])  # X509_CERT_DIR
    cmd.extend(["--bind", f"{getVOMSLocation()}:/etc/grid-security/vomsdir"])  # X509_VOMS_DIR
    cmd.extend(["--bind", "{0}:{0}:ro".format(etc_dir)])  # etc dir for dirac.cfg
    cmd.extend(["--bind", "{0}:{0}:ro".format(os.path.join(os.path.realpath(sys.base_prefix)))])  # code dir

    rootImage = user_image or gConfig.getValue("/Resources/Computing/Singularity/ContainerRoot") or CONTAINER_DEFROOT

    if os.path.isdir(rootImage) or os.path.isfile(rootImage):
        cmd.extend([rootImage, "/mnt/dirac_container.sh"])
    else:
        # if we are here is because there's no image, or it is not accessible (e.g. not on CVMFS)
        gLogger.error("Apptainer image to exec not found: ", rootImage)
        return S_ERROR("Failed to find Apptainer image to exec")

    gLogger.debug(f"Execute Apptainer command: {' '.join(cmd)}")
    result = systemCall(0, cmd)
    if not result["OK"]:
        DIRAC.exit(1)
    gLogger.notice(result["Value"][1])


if __name__ == "__main__":
    main()
