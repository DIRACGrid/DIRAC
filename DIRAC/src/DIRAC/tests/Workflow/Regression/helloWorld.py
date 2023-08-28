""" simple hello world job
"""

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers

j = Job()

j.setName("helloWorld-test")

j.setExecutable("exe-script.py", "", "Executable.log")

# <-- user settings
j.setCPUTime(172800)
tier1s = DMSHelpers().getTiers(tier=(0, 1))
j.setBannedSites(tier1s)
# user settings -->


# print j.workflow

# submit the job to dirac
result = Dirac().submitJob(j)
print(result)
