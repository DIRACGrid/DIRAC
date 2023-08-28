#!/usr/bin/env python
""" update local cfg
"""
import os

from diraccfg import CFG

from DIRAC.Core.Base.Script import Script

Script.setUsageMessage("\n".join([__doc__.split("\n")[1], "Usage:", f"  {Script.scriptName} [options]"]))

Script.registerSwitch("F:", "file=", "set the cfg file to update.")
Script.registerSwitch("V:", "vo=", "set the VO.")
Script.registerSwitch("S:", "setup=", "set the software dist module to update.")
Script.registerSwitch("D:", "softwareDistModule=", "set the software dist module to update.")

Script.parseCommandLine()
args = Script.getPositionalArgs()

from DIRAC import gConfig

cFile = ""
sMod = ""
vo = ""
setup = ""

for unprocSw in Script.getUnprocessedSwitches():
    if unprocSw[0] in ("F", "file"):
        cFile = unprocSw[1]
    if unprocSw[0] in ("V", "vo"):
        vo = unprocSw[1]
    if unprocSw[0] in ("D", "softwareDistModule"):
        sMod = unprocSw[1]
    if unprocSw[0] in ("S", "setup"):
        setup = unprocSw[1]

localCfg = CFG()
if cFile:
    localConfigFile = cFile
else:
    print(f"WORKSPACE: {os.path.expandvars('$WORKSPACE')}")
    if os.path.isfile(os.path.expandvars("$WORKSPACE") + "/PilotInstallDIR/etc/dirac.cfg"):
        localConfigFile = os.path.expandvars("$WORKSPACE") + "/PilotInstallDIR/etc/dirac.cfg"
    elif os.path.isfile(os.path.expandvars("$WORKSPACE") + "/ServerInstallDIR/etc/dirac.cfg"):
        localConfigFile = os.path.expandvars("$WORKSPACE") + "/ServerInstallDIR/etc/dirac.cfg"
    elif os.path.isfile("./etc/dirac.cfg"):
        localConfigFile = "./etc/dirac.cfg"
    else:
        print("Local CFG file not found")
        exit(2)

localCfg.loadFromFile(localConfigFile)
if not localCfg.isSection("/LocalSite"):
    localCfg.createNewSection("/LocalSite")
localCfg.setOption("/LocalSite/CPUTimeLeft", 5000)
localCfg.setOption("/DIRAC/Security/UseServerCertificate", False)

if not sMod:
    if not setup:
        setup = gConfig.getValue("/DIRAC/Setup")
        if not setup:
            setup = "dirac-JenkinsSetup"

    if not localCfg.isSection("/Operations"):
        localCfg.createNewSection("/Operations")
    if not localCfg.isSection(f"/Operations/{setup}"):
        localCfg.createNewSection(f"/Operations/{setup}")
    localCfg.setOption(f"/Operations/{setup}/SoftwareDistModule", "")

localCfg.writeToFile(localConfigFile)
