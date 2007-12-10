#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/Attic/dirac-pilot-lcg.py,v 1.5 2007/12/10 14:28:45 paterson Exp $
# File :   dirac-pilot-lcg.py
# Author : Stuart Paterson
########################################################################

import os,sys,string,re

""" The DIRAC Pilot script for LCG performs initial checks on the sanity of the WN
    environment then installs and configures DIRAC.  The Pilot script then runs
    a DIRAC JobAgent that can make requests to the central WMS for pending jobs
    for the VO.
"""

__RCSID__ = "$Id: dirac-pilot-lcg.py,v 1.5 2007/12/10 14:28:45 paterson Exp $"


DEBUG = 1
DIRAC_URL = 'http://cern.ch/lhcbproject/dist/DIRAC3'
SW_DIR = 'VO_LHCB_SW_DIR'
SW_PATH ='lib'
DIRAC_PYTHON ='2.4'
INSTALL_RETRIES = 5
MIN_DISK_SPACE = 2560 #MB
CLEANUP = 0
LOCAL = 0
DISABLE_INSTALL = 0
JOB_AGENT_CE = 'InProcess'

start = os.getcwd()
os.system('chmod 750 . 1> /dev/null 2>&1')
os.system('chmod 750 .. 1> /dev/null 2>&1')

outputFile = '%s/pilotOutput.log' % start
pilotOutput = open(outputFile,'w')

#############################################################################
def printPilot(message,level='INFO'):
  """Simple function to format print statements.
  """
  line = 'DIRAC/PilotAgent %s: %s' %(level,message)
  if level=='DEBUG':
    if DEBUG:
      print line
  else:
    print line
  pilotOutput.write('%s\n' % line)

#############################################################################
def runCommand(cmd,strip=0):
  """Simple function to run a system command.
  """
  if DEBUG:
    printPilot('Executing: `%s`' %cmd,'DEBUG')
  stdout_handle = os.popen(cmd, "r")
  stdout = stdout_handle.read()
  if strip:
    return stdout
  else:
    return stdout.replace('\n','')

#############################################################################
def fixPythonEnvironment(sharedPython):
  """This method resets the LD_LIBRARY_PATH, PATH and PYTHONPATH
     when using a pre-installed python version.
  """
  pyPath = os.environ.get('PYTHONPATH')
  printPilot('Original PYTHONPATH is:\n%s' %pyPath,'DEBUG')
  ldPath = os.environ.get('LD_LIBRARY_PATH')
  printPilot('Original LD_LIBRARY_PATH is:\n%s' %ldPath,'DEBUG')
  path = os.environ.get('PATH')
  printPilot('Original PATH is:\n%s' %path,'DEBUG')

  removelib64 = pyPath.split(':')
  shared = '%s/bin' %sharedPython
  correctedPy = [shared]
  for path in removelib64:
    if not re.search('lib64',path):
      correctedPy.append(path)

  newPyPath =  string.join(correctedPy,':')
  os.putenv('PYTHONPATH',newPyPath)
  printPilot('Corrected PYTHONPATH is:\n%s' %newPyPath,'DEBUG')
  ldShared = '%s/lib' %sharedPython
  newLDPath = '%s:%s' %(ldShared,ldpath)
  os.putenv('LD_LIBRARY_PATH',newLDPath)
  printPilot('Corrected LD_LIBRARY_PATH is:\n%s' %newLDPath,'DEBUG')
  newPath = '%s:%s' %(shared,path)
  os.putenv('PATH',newPath)
  printPilot('Corrected PATH is:\n%s' %newPath,'DEBUG')

#############################################################################
def writeConfigFile(fname,section,optionsDict):
  """Wrapper function to write a .cfg file to control Agent behaviour.
     Assumes only key value pairs in the options dict.
  """
  printPilot('Attempting to create %s' %(fname))
  sections = string.split(section,'/')
  contents = []
  for s in sections: contents.append('%s{ ' %(s))

  for n,v in optionsDict.items(): contents.append('%s = %s ' %(n,v))

  for i in xrange(len(sections)): contents += '}'

  cfg = string.join(contents,'\n')
  fopen = open(fname,'w')
  fopen.write(cfg)
  fopen.close()

#############################################################################
def pilotExit(code):
  """This method resets the LD_LIBRARY_PATH, PATH and PYTHONPATH
     when using a pre-installed python version.
  """
  printPilot('Exiting with status code %s' %(code),'INFO')
  if DEBUG:
    toCheck = os.listdir('.')
    for directory in toCheck:
      if os.path.isdir(directory):
        printPilot('Files in %s are:' %(directory),'DEBUG')
        for i in os.listdir(directory): print i
      else:
        printPilot('File %s' %(directory),'DEBUG')

  printPilot('==================================EOF===================================')
  pilotOutput.close()
  sys.stdout.flush()
  sys.exit(int(code))

#############################################################################
if len(sys.argv)!=3:
  script = sys.argv[0]
  print 'Illegal number of arguments: %s' %(sys.argv)
  print 'Usage: %s <DIRAC Setup> <Job CPU Requirement>' % sys.argv[0]
  sys.exit(1)

#############################################################################
#Preamble
printPilot('========================================================================')
scriptName = sys.argv[0]
printPilot('Version %s' %(__RCSID__))
diracSetup = sys.argv[1]
jobCPUReqt = sys.argv[2]
CEUNIQUEID = 'InProcess' #This will be specified by the Agent Director eventually
printPilot('Running in %s setup on %s' %(diracSetup,runCommand('date')))
printPilot('WMS CPU Requirement is %s' %jobCPUReqt)

CMTCONFIG = runCommand('python guessPlatform')
printPilot('Setting CMTCONFIG for site to %s' %(CMTCONFIG))

if CMTCONFIG == 'Unknown':
  CMTCONFIG = 'slc3_ia32_gcc323'
  printPilot('Since Unknown, setting default CMTCONFIG value to %s' %CMTCONFIG,'WARN')

os.putenv('CMTCONFIG',CMTCONFIG)
printPilot('Current python is: %s' %(sys.executable))

DIRAC_PYTHON = ''

if os.environ.has_key(SW_DIR):
  sharedArea = os.environ.get(SW_DIR)
  printPilot('Found %s = %s' %(SW_DIR,sharedArea))
  sharedPython='%s/%s/%s/bin/python%s' %(sharedArea,SW_PATH,CMTCONFIG,DIRAC_PYTHON)
  printPilot('Searching for pre-installed DIRAC python:','INFO')
  printPilot(sharedPython,'INFO')
  if os.path.exists(sharedPython):
    printPilot('Found python in %s' %(sharedArea))
    fixPythonEnvironment(sharedPython)
    DIRAC_PYTHON = sharedPython
  else:
    printPilot('python not found in %s, currently using %s' %(SW_DIR,sys.executable),'WARN')
else:
  printPilot('Using standard python since %s not defined' %SW_DIR,'WARN')

if not CLEANUP:
  printPilot('Setting DO_NOT_DO_JOB_CLEANUP=1','DEBUG')

printPilot('========================================================================')

#############################################################################
#Initial checks in Grid environment, print debugging information

printPilot('Proxy information:')
os.system('grid-proxy-info')
sys.stdout.flush()
printPilot('========================================================================')

if not LOCAL:
  glite = runCommand('command -v glite-brokerinfo')
  lcg = runCommand('command -v edg-brokerinfo')
  if glite:
    printPilot('Running with gLite middleware')
    CE = runCommand('glite-brokerinfo getCE')
    LCG_SITE_CE = runCommand('glite-brokerinfo getCE | cut -d ":" -f 1')
    printPilot('CE = %s && LCG_SITE_CE = %s' %(CE,LCG_SITE_CE))
  elif lcg:
    printPilot('Running with LCG middleware')
    CE = runCommand('edg-brokerinfo getCE')
    LCG_SITE_CE = runCommand('edg-brokerinfo getCE | cut -d ":" -f 1')
    printPilot('CE = %s && LCG_SITE_CE = %s' %(CE,LCG_SITE_CE))
  else:
    printPilot('WN has no access to glite or edg brokerinfo commands, exiting','ERROR')
    pilotExit(1)
else:
  LCG_SITE_CE = 'Local'
  printPilot('Running locally')

printPilot('Hostname = %s' %(runCommand('hostname')))
whoami = runCommand('whoami')
printPilot('LocalAccount = %s' %(whoami))
printPilot('ID = %s' %(runCommand('id')))
printPilot('CurrentDir = %s' %(start))
if os.path.exists('/etc/redhat-release'):
  printPilot('RedHat Release = %s' %(runCommand('cat /etc/redhat-release')))
printPilot('uname = %s' %(runCommand('uname -a')))
printPilot(runCommand("""cat /proc/cpuinfo | grep -i "cpu MHz" | awk '{n+=1;CPU=$4}END{print "CPU (MHz)  = ",CPU, " x ",n  }'"""))
printPilot(runCommand('cat /proc/meminfo | grep -i MemTotal'))
diskSpace = runCommand("""df -P -m . |tail -1 | awk '{print $4}'""")
printPilot('Available Space (MB) = %s' %(diskSpace))

if diskSpace < MIN_DISK_SPACE:
  printPilot('%s MB < %s MB, not enough local disk space available, exiting' %(diskSpace,MIN_DISK_SPACE),'ERROR')
  pilotExit(1)

if not LCG_SITE_CE:
  printPilot('LCG_SITE_CE is not defined, exiting','ERROR')
  pilotExit(1)

printPilot('========================================================================')

#############################################################################
#Installation and configuration of DIRAC

TARFILE = 'DIRAC-%s.tar.gz' % diracSetup
diracDist = '%s/%s' %(DIRAC_URL,TARFILE)
printPilot('DIRAC Tar File to be downloaded is: %s' %(diracDist))
installDIRAC = './dirac-install -f -p %s ' %(diracDist) #perform full DIRAC installation for now

if DISABLE_INSTALL:
  installDIRAC = 'echo disabled DIRAC installation'

runCommand('chmod a+x dirac-install')
printPilot(runCommand('ls -al dirac-install'),'DEBUG')
DIRAC_INSTALLED = 0
for attempt in xrange(INSTALL_RETRIES):
  if not DIRAC_INSTALLED:
    installation = runCommand(installDIRAC,1)
    printPilot('>>>>>>>>>>Start Installation Attempt %s: DIRAC Installation Log' %(attempt+1),'DEBUG')
    if DEBUG:
      print installation
      sys.stdout.flush()
    printPilot('<<<<<<<<<<End Installation Attempt %s: DIRAC Installation Log' %(attempt+1),'DEBUG')

    if os.path.exists('DIRAC'):
      DIRAC_INSTALLED = 1

if not DIRAC_INSTALLED:
  printPilot('Could not install DIRAC from %s, exiting' %(diracDist),'ERROR')
  pilotExit(1)

#Temporarily add a link to the correct CMTCONFIG (to be updated when all binaries available)
printPilot('Creating link to %s from slc4_amd64_gcc34' %(CMTCONFIG),'DEBUG')
runCommand('ln -s slc4_amd64_gcc34 %s' %(CMTCONFIG))

#Get site name from CS and repeat setup
if not DIRAC_PYTHON:
  diracPython='%s/%s/bin/python%s' %(start,CMTCONFIG,DIRAC_PYTHON)
  printPilot('Using locally installed DIRAC python: %s' %(diracPython))
else:
  diracPython=DIRAC_PYTHON
  printPilot('Using DIRAC python from shared area: %s' %(diracPython))

if not os.path.exists(diracPython):
  printPilot('DIRAC Python does not exist','ERROR')
  pilotExit(1)

printPilot(runCommand('chmod a+x %s' %(diracPython),1),'DEBUG')
printPilot(runCommand('ls -al %s' %(diracPython),1),'DEBUG')

#Insert DIRAC ROOT to sys.path
#printPilot('Adding %s to sys.path' %(start),'DEBUG')
#sys.path.insert(0,start)
#sys.path.insert(0,start+'/scripts')

#Initial setup of DIRAC to enable CS settings
initialDIRACSetup = '%s scripts/dirac-setup -s LCG.Unknown.ch -m %s' %(diracPython,diracSetup)
printPilot('>>>>>>>>>>Start: Initial DIRAC Setup Log','DEBUG')
if DEBUG:
  print initialDIRACSetup
printPilot('Setting PYTHONPATH to null for dirac-setup','DEBUG')
os.putenv('PYTHONPATH','')
sys.stdout.flush()
os.system(initialDIRACSetup)
sys.stdout.flush()
printPilot('<<<<<<<<<<End: Initial DIRAC Setup Log','DEBUG')
if DEBUG:
  printPilot('Checking local configuration file:','DEBUG')
  if os.path.exists('etc/dirac.cfg'):
    cfg = runCommand('cat etc/dirac.cfg',1)
    sys.stdout.flush()
    print cfg
  else:
    printPilot('etc/dirac.cfg file does not exist','WARN')

getSite = """ "from DIRAC.Core.Base import Script; Script.parseCommandLine(); from DIRAC import gConfig; result = gConfig.getOptionsDict('/Resources/GridSites/LCG'); print result" """
siteDictStr = runCommand('%s -c %s' %(diracPython,getSite),1)
printPilot('CS query returned: \n%s' %(siteDictStr),'DEBUG')
try:
  res = string.split(siteDictStr,'\n')
  siteCSDict = None
  for i in res:
    if re.search('^{',i):
      printPilot(i,'DEBUG')
      siteCSDict = i
  if siteCSDict:
    siteDict = eval(siteCSDict)
except Exception,x:
  printPilot('Could not obtain LCG site list from CS with exception:','ERROR')
  printPilot(str(x),'ERROR')
  pilotExit(1)

if not siteDict:
  printPilot('Null object returned from CS','ERROR')
  pilotExit(1)

if not siteDict['OK']:
  printPilot('Returned LCG site dictionary not OK','ERROR')
  printPilot(siteDict['Message'],'ERROR')
  pilotExit(1)

sites = siteDict['Value']
DIRAC_SITE_NAME = ''
for ce,siteName in siteDict.items():
  if LCG_SITE_CE == ce:
    printPilot('Found DIRAC site name: %s' %(ce))
    DIRAC_SITE_NAME = siteName

if LOCAL:
  DIRAC_SITE_NAME = 'LCG.CERN.ch'
  printPilot('Found DIRAC site name: %s' %(DIRAC_SITE_NAME))

if not DIRAC_SITE_NAME:
  printPilot('No DIRAC site names were found for CE %s' %(LCG_SITE_CE),'ERROR')
  pilotExit(1)

#Full setup of DIRAC with LCG site name
#temporarily append LHCb to dev string from AD
fullDIRACSetup = '%s scripts/dirac-setup -m %s -s %s -a %s -p %s ' %(diracPython,'LHCb-'+diracSetup,DIRAC_SITE_NAME,CMTCONFIG,'LCG')
printPilot('>>>>>>>>>>Start: Full DIRAC Setup Log','DEBUG')
if DEBUG:
  print fullDIRACSetup
sys.stdout.flush()
os.system(fullDIRACSetup)
sys.stdout.flush()
printPilot('<<<<<<<<<<End: Full DIRAC Setup Log','DEBUG')
if DEBUG:
  printPilot('Checking local configuration file:','DEBUG')
  if os.path.exists('etc/dirac.cfg'):
    cfg = runCommand('cat etc/dirac.cfg',1)
    sys.stdout.flush()
    print cfg
  else:
    printPilot('etc/dirac.cfg file does not exist','ERROR')

if not os.path.exists('etc/dirac.cfg'):
  pilotExit(1)

#Add default extra CS values to cfg file

cfg = open('etc/dirac.cfg','a')
cfg.close()

#############################################################################
#Start DIRAC Job Agent

#runJobAgent = '%s %s/DIRAC/Core/scripts/dirac-agent WorkloadManagement/JobAgent -o LogLevel=debug ' %(diracPython,start)
runJobAgent = '%s scripts/dirac-agent WorkloadManagement/JobAgent -o LogLevel=debug ' %(diracPython)

#write any necessary configuration files
inProcessSection = 'Resources/Computing/InProcess'
inProcessDict = {'WorkingDirectory':start,'LocalAccountString':whoami,'TotalCPUs':1,'MaxCPUTime':int(jobCPUReqt)+1,'MaxRunningJobs':1}
inProcessDict['CPUScalingFactor']=1
inProcessDict['MaxTotalJobs']=1
writeConfigFile('InProcess.cfg',inProcessSection,inProcessDict)

#below is because LHCb-Development differs from Development, this is fine for initial tests
#but will be replaced...  also 'Development' below will be replaced by local sites / agents section
writeConfigFile('setup.cfg','DIRAC',{'Setup':'LHCb-Development'})

jobAgentSection = 'Systems/WorkloadManagement/Development/Agents/JobAgent'
writeConfigFile('JobAgent.cfg',jobAgentSection,{'CEUniqueID':'InProcess','MaxCycles':1})

writeConfigFile('security.cfg','DIRAC/Security',{'UseServerCertificate':'no'})
#need to define watchdog control directory
watchdogSection = 'Systems/WorkloadManagement/Development/Agents/Watchdog'
writeConfigFile('Watchdog.cfg',watchdogSection,{'PollingTime':20,'ControlDirectory':start})


#find any .cfg files and append to script to run job agent, all files created in '.'
for i in os.listdir(start):
  if re.search('.cfg$',i):
    runJobAgent += i+' '

printPilot('Running DIRAC Job Agent:\n%s' %(runJobAgent),'DEBUG')

sys.stdout.flush()
#printPilot('Setting PYTHONPATH to null')
#os.putenv('PYTHONPATH',' ')

os.system(runJobAgent)
sys.stdout.flush()

#############################################################################
#Perform any post-execution tasks / debugging and exit gracefully
printPilot('Post-execution proxy information:')
os.system('grid-proxy-info')
sys.stdout.flush()
printPilot('Execution of %s complete.' %(scriptName))
printPilot('========================================================================')
pilotExit(0)
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#