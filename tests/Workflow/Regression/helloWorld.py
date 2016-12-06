# dirac job created by ganga
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac
j = Job()
dirac = Dirac()

# default commands added by ganga
j.setName( "helloWorld-test" )
j.setInputSandbox( ['/afs/cern.ch/user/f/fstagni/userJobs/_inputHello.tar.bz2', '/afs/cern.ch/user/f/fstagni/userJobs/hello-script.py'] )

j.setExecutable( "exe-script.py", "", "Ganga_Executable.log" )

# <-- user settings
j.setCPUTime( 172800 )
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
try:
  tier1s = DMSHelpers().getTiers( tier = ( 0, 1 ) )
except AttributeError:
  tier1s = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
            'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.RRCKI.ru', 'LCG.SARA.nl']
# user settings -->


# print j.workflow

# submit the job to dirac
result = dirac.submit( j )
print result
