#!/usr/bin/env python
"""Starts a DIRAC command inside an apptainer container."""

import os
import sys
from pathlib import Path

import DIRAC
from DIRAC import S_ERROR, gConfig, gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security.Locations import getCAsLocation, getProxyLocation, getVOMSLocation
from DIRAC.Core.Utilities.ContainerImageResolver import EXISTS_TIMEOUT, findMultiarchImage, resolveImagePath
from DIRAC.Core.Utilities.Os import safe_exists, safe_listdir
from DIRAC.Core.Utilities.Subprocess import systemCall


def generate_container_wrapper(dirac_env_var, diracos_env_var, etc_dir, rc_script, command, include_proxy=True):
    lines = [
        "#!/bin/bash",
        f"export DIRAC={dirac_env_var}",
        f"export DIRACOS={diracos_env_var}",
    ]

    if include_proxy:
        lines.append("export X509_USER_PROXY=/etc/proxy")

    lines.extend(
        [
            "export X509_CERT_DIR=/etc/grid-security/certificates",
            "export X509_VOMS_DIR=/etc/grid-security/vomsdir",
            "export X509_VOMSES=/etc/grid-security/vomses",
            f"export DIRACSYSCONFIG={etc_dir}/dirac.cfg",
            f"source {rc_script}",
            command,
        ]
    )

    return "\n".join(lines)


@Script()
def main():
    command = sys.argv[1]

    user_image = None
    Script.registerSwitch("i:", "image=", "   Container image: local path or OCI reference")
    Script.parseCommandLine(ignoreErrors=False)
    for switch in Script.getUnprocessedSwitches():
        if switch[0].lower() == "i" or switch[0].lower() == "image":
            user_image = switch[1]

    cwd = os.path.realpath(os.getcwd())
    dirac_env_var = os.environ.get("DIRAC", cwd)
    diracos_env_var = os.environ.get("DIRACOS", cwd)
    etc_dir = os.path.join(DIRAC.rootPath, "etc")
    rc_script = os.path.join(os.path.realpath(sys.base_prefix), "diracosrc")

    include_proxy = True
    proxy_location = getProxyLocation()
    if not proxy_location:
        include_proxy = False

    with open("dirac_container.sh", "w", encoding="utf-8") as fd:
        script = generate_container_wrapper(
            dirac_env_var, diracos_env_var, etc_dir, rc_script, command, include_proxy=include_proxy
        )
        fd.write(script)
    # Script may include credentials, make sure other users can't read it
    os.chmod("dirac_container.sh", 0o700)

    # Resolve the container image. As before the multiarch layout, a value given
    # with -i/--image is used on its own: an existing local path is taken as-is,
    # and the configured ContainerRoot is not considered. Looking the value up as
    # an OCI reference is new, and only happens where the command used to fail.
    if user_image:
        if safe_exists(user_image, timeout=EXISTS_TIMEOUT):
            image_path = Path(user_image)
        else:
            image_path = findMultiarchImage(user_image)
    else:
        # No built-in default here: an unconfigured node must fail loudly rather
        # than silently running the payload in whatever image happens to be on CVMFS
        image_path = resolveImagePath(defaultRoot=None)
    if not image_path:
        gLogger.error("Apptainer image to exec not found:", user_image or "(from the configuration)")
        return S_ERROR("Failed to find Apptainer image to exec")

    # Build the apptainer command
    cmd = ["apptainer", "exec"]
    cmd.extend(["--contain"])  # use minimal /dev and empty other directories (e.g. /tmp and $HOME)
    cmd.extend(["--ipc"])  # run container in a new IPC namespace
    cmd.extend(["--pid"])  # run container in a new PID namespace
    cmd.extend(["--bind", cwd])  # bind current directory for dirac_container.sh
    if proxy_location:
        cmd.extend(["--bind", f"{proxy_location}:/etc/proxy"])  # bind proxy file
    cmd.extend(["--bind", f"{getCAsLocation()}:/etc/grid-security/certificates"])  # X509_CERT_DIR
    voms_location = Path(getVOMSLocation())
    cmd.extend(["--bind", f"{voms_location}:/etc/grid-security/vomsdir"])  # X509_VOMS_DIR
    vomses_location = voms_location.parent / "vomses"
    cmd.extend(["--bind", f"{vomses_location}:/etc/grid-security/vomses"])  # X509_VOMSES
    cmd.extend(["--bind", "{0}:{0}:ro".format(etc_dir)])  # etc dir for dirac.cfg
    cmd.extend(["--bind", "{0}:{0}:ro".format(os.path.join(os.path.realpath(sys.base_prefix)))])  # code dir
    # here bind optional paths
    for bind_path in gConfig.getValue("/Resources/Computing/Singularity/BindPaths", []):
        if safe_listdir(bind_path):
            cmd.extend(["--bind", f"{bind_path}:{bind_path}"])
        else:
            gLogger.warn(f"Bind path {bind_path} does not exist, skipping")
    cmd.extend(["--cwd", cwd])  # set working directory

    cmd.extend([str(image_path), f"{cwd}/dirac_container.sh"])

    gLogger.debug(f"Execute Apptainer command: {' '.join(cmd)}")
    result = systemCall(0, cmd)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRAC.exit(1)
    if result["Value"][0] != 0:
        gLogger.error("Apptainer command failed with exit code", result["Value"][0])
        gLogger.error("Command output:", result["Value"])
        DIRAC.exit(2)
    gLogger.notice(result["Value"][1])


if __name__ == "__main__":
    main()
