#!/usr/bin/env python
# Test the possibility to use general workflow parameters as variables
# in the job description

import subprocess
import shlex
import sys
from pathlib import Path
from DIRAC.Interfaces.API.Job import Job

job = Job()

job.setExecutable("/bin/ls")
job.setExecutable("/bin/echo", arguments="@{InputData}")

file = Path("jobDescription.xml")
file.write_text(job.workflow.toXML())

status = subprocess.call(shlex.split("dirac-jobexec jobDescription.xml -p InputData='{input_file_1,input_file_2}'"))
if status:
    sys.exit(status)

file = Path("Script2_echo.log")
content = file.read_text()
if not "input_file_1;input_file_2" in content:
    print("Test failed !")
    sys.exit(-1)

print("Test successful !")
