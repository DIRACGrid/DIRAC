#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/Attic/dirac-pilot-lcg.py,v 1.16 2008/02/19 17:48:24 paterson Exp $
# File :   dirac-pilot-lcg.py
# Author : Stuart Paterson
########################################################################

import os,sys,string,re

""" The DIRAC Pilot script for LCG performs initial checks on the sanity of the WN
    environment then installs and configures DIRAC.  The Pilot script then runs
    a DIRAC JobAgent that can make requests to the central WMS for pending jobs
    for the VO.
"""

__RCSID__ = "$Id: dirac-pilot-lcg.py,v 1.16 2008/02/19 17:48:24 paterson Exp $"


DEBUG = 1
DIRAC_URL = 'http://cern.ch/lhcbproject/dist/DIRAC3'
SW_PATH ='lib'
DIRAC_PYTHON_VERSION ='2.4'
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
def getDictFromCS(diracPython,csSection):
  """This function executes a query for CS Dictionaries using the installed
     DIRAC python distribution before running the Agent to retrieve jobs.
     If a query fails, the PilotAgent will terminate gracefully since the
     information is always vital to the execution of the Agent.
  """
  csQuery = """ "from DIRAC.Core.Base import Script; Script.parseCommandLine(); from DIRAC import gConfig; result = gConfig.getOptionsDict('%s'); print result" """ % (csSection)
  csDictStr = runCommand('%s -c %s' %(diracPython,csQuery),1)
  printPilot('CS query returned: \n%s' %(csDictStr),'DEBUG')
  try:
    result = None
    res = string.split(csDictStr,'\n')
    resCSDict = None
    for i in res:
      if re.search('^{',i):
        printPilot(i,'DEBUG')
        resCSDict = i
    if resCSDict:
      result = eval(resCSDict)
  except Exception,x:
    printPilot('Could not obtain section %s from CS with exception:' %(csSection),'ERROR')
    printPilot(str(x),'ERROR')
    pilotExit(1)

  if not result:
    printPilot('Null object returned from CS for section %s' %(csSection),'ERROR')
    pilotExit(1)

  if not result['OK']:
    printPilot('CS returned S_ERROR() for section %s' %(csSection),'ERROR')
    printPilot(result['Message'],'ERROR')
    pilotExit(1)

  if not result['Value']:
    printPilot('Empty dictionary returned from CS for section %s' %(csSection),'ERROR')
    pilotExit(1)

  return result['Value']

#############################################################################
def installDIRACDist(architecture,diracDistribution):
  """Simple wrapper to install DIRAC (since done more than once now).
  """
  if not os.path.exists('dirac-install'):
    printPilot('dirac-install not present in local directory','ERROR')

  installDIRAC = './dirac-install -f %s -p %s ' %(architecture,diracDistribution)
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

      if os.path.exists('%s/DIRAC' %(start)):
        DIRAC_INSTALLED = 1

  if not DIRAC_INSTALLED:
    printPilot('Could not install DIRAC from %s, exiting' %(diracDistribution),'ERROR')
    pilotExit(1)

#############################################################################
def setupDIRAC(cmd):
  """Simple wrapper to setup DIRAC (done more than once).
  """
  printPilot('>>>>>>>>>>Start: DIRAC Setup Log','DEBUG')
  if DEBUG:
    print cmd
  sys.stdout.flush()
  os.system(cmd)
  sys.stdout.flush()
  printPilot('<<<<<<<<<<End: DIRAC Setup Log','DEBUG')

  if DEBUG:
    printPilot('Checking local configuration file:','DEBUG')
    if os.path.exists('etc/dirac.cfg'):
      cfg = runCommand('cat etc/dirac.cfg',1)
      sys.stdout.flush()
      print cfg
    else:
      printPilot('etc/dirac.cfg file does not exist','ERROR')
      pilotExit(1)

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
        if not directory=='lib':
          printPilot('Files in %s are:' %(directory),'DEBUG')
          for i in os.listdir(directory): print i
      else:
        printPilot('File %s' %(directory),'DEBUG')

  printPilot('==================================EOF===================================')
  pilotOutput.close()
  sys.stdout.flush()
  sys.exit(int(code))

#############################################################################
if len(sys.argv)!=6:
  script = sys.argv[0]
  print 'Illegal number of arguments: %s' %(sys.argv)
  print 'Usage: %s <DIRAC Setup> <Job CPU Requirement> <VO_SW_DIR_Variable> <ProxyRole>' % sys.argv[0]
  sys.exit(1)

#############################################################################
#Preamble
printPilot('========================================================================')
scriptName = sys.argv[0]
printPilot('Version %s' %(__RCSID__))
diracSetup = sys.argv[1]
jobCPUReqt = sys.argv[2]
SW_DIR = sys.argv[3]
PROXY_ROLE = sys.argv[4]
PILOT_TYPE = sys.argv[5]
printPilot('Running in %s setup on %s' %(diracSetup,runCommand('date')))
printPilot('WMS CPU Requirement is %s' %jobCPUReqt)
printPilot('VO SW directory environment variable is %s' %(SW_DIR),'DEBUG')
CMTCONFIG = runCommand('python dirac-architecture')
#printPilot('Temporarily hardcoding CMTCONFIG to slc4_ia32_gcc34')
#CMTCONFIG = 'slc4_ia32_gcc34'
printPilot('Setting CMTCONFIG for site to %s' %(CMTCONFIG))

if CMTCONFIG == 'Unknown':
  CMTCONFIG = 'slc4_ia32_gcc34'
  printPilot('Since Unknown, setting default CMTCONFIG value to %s' %(CMTCONFIG),'WARN')

os.putenv('CMTCONFIG',CMTCONFIG)
printPilot('Current python is: %s' %(sys.executable))

DIRAC_PYTHON = ''

if os.environ.has_key(SW_DIR):
  sharedArea = os.environ.get(SW_DIR)
  printPilot('Found %s = %s' %(SW_DIR,sharedArea))
  sharedPython='%s/%s/%s/bin/python%s' %(sharedArea,SW_PATH,CMTCONFIG,DIRAC_PYTHON_VERSION)
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
printPilot('Performing initial DIRAC installation for %s' %(CMTCONFIG))
installDIRACDist(CMTCONFIG,diracDist)

#Locate DIRAC python
if not DIRAC_PYTHON:
  diracPython='%s/%s/bin/python%s' %(start,CMTCONFIG,DIRAC_PYTHON_VERSION)
  printPilot('Using locally installed DIRAC python: %s' %(diracPython))
else:
  diracPython=DIRAC_PYTHON
  printPilot('Using DIRAC python from shared area: %s' %(diracPython))

if not os.path.exists(diracPython):
  printPilot('DIRAC Python does not exist:','ERROR')
  printPilot(diracPython,'ERROR')
  pilotExit(1)

printPilot(runCommand('chmod a+x %s' %(diracPython),1),'DEBUG')
printPilot(runCommand('ls -al %s' %(diracPython),1),'DEBUG')

#Initial setup of DIRAC to enable CS settings
initial = '%s scripts/dirac-setup -s LCG.Unknown.ch -m %s' %(diracPython,diracSetup)
printPilot('Performing initial DIRAC setup to enable CS')
setupDIRAC(initial)

#Retrieve current site name and local SEs from CS
siteDict = getDictFromCS(diracPython,'/Resources/GridSites/LCG')
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

LOCALSE = ''
siteLocalSEMapping = getDictFromCS(diracPython,'/Resources/SiteLocalSEMapping')
for site,ses in siteLocalSEMapping.items():
  if site == DIRAC_SITE_NAME:
    LOCALSE = ses
    printPilot('Found LocalSE = %s' %(LOCALSE))

if not LOCALSE:
  printPilot('No LocalSE found in SiteLocalSEMapping for %s setting to None' %(DIRAC_SITE_NAME),'WARN')
  LOCALSE = 'None'

#Now that CS can be accessed, other possible system configurations can now be resolved
extraArchitectures = getDictFromCS(diracPython,'/Resources/Computing/OSCompatibility')
newArch = []
if extraArchitectures.has_key(CMTCONFIG):
  newArch = extraArchitectures[CMTCONFIG]
  printPilot('Compatible OS Architectures are: %s' %(newArch))
else:
  printPilot('Compatible OS Architectures for %s undefined in /Resources/Computing/OSCompatibility' %(CMTCONFIG),'ERROR')
  pilotExit(1)

newArch = string.split(newArch,',')
#Trigger installations of additional tags but retain original CMTCONFIG for final dirac-setup
for archToInstall in newArch:
  if not archToInstall==CMTCONFIG: #already installed this one
    printPilot('Installing DIRAC for %s' %(archToInstall))
    installDIRACDist(archToInstall,diracDist)
    newPython = '%s/%s/bin/python%s' %(start,archToInstall,DIRAC_PYTHON_VERSION)
    printPilot(runCommand('chmod a+x %s' %(newPython),1),'DEBUG')
    printPilot(runCommand('ls -al %s' %(newPython),1),'DEBUG')

#Full setup of DIRAC with LCG site name
fullSetup = '%s scripts/dirac-setup -m %s -s %s -a %s -p %s ' %(diracPython,diracSetup,DIRAC_SITE_NAME,CMTCONFIG,PILOT_TYPE)
setupDIRAC(fullSetup)

#############################################################################
#Start DIRAC Job Agent after creating some cfg files

runJobAgent = '%s scripts/dirac-agent WorkloadManagement/JobAgent -o LogLevel=debug ' %(diracPython)

inProcessSection = 'Resources/Computing/InProcess'
inProcessDict = {'WorkingDirectory':start,'LocalAccountString':whoami,'TotalCPUs':1,'MaxCPUTime':int(jobCPUReqt)+1,'MaxRunningJobs':1}
inProcessDict['CPUScalingFactor']=1
inProcessDict['MaxTotalJobs']=1
writeConfigFile('InProcess.cfg',inProcessSection,inProcessDict)
#writeConfigFile('Setup.cfg','DIRAC',{'Setup':'LHCb-Development'})

#Must get 'Development' string from CS to set certain parameters
setupDict = getDictFromCS(diracPython,'DIRAC/Setups/%s' %(diracSetup))
if not setupDict.has_key('WorkloadManagement'):
  printPilot('Could not find setup for %s/WorkloadManagement' %(diracSetup))
  pilotExit(1)

wmsSetup = setupDict['WorkloadManagement']
jobAgentSection = 'Systems/WorkloadManagement/%s/Agents/JobAgent' %(wmsSetup)
writeConfigFile('JobAgent.cfg',jobAgentSection,{'CEUniqueID':JOB_AGENT_CE,'MaxCycles':1})
writeConfigFile('Security.cfg','DIRAC/Security',{'UseServerCertificate':'no'})

#need to define watchdog control directory
watchdogSection = 'Systems/WorkloadManagement/%s/Agents/Watchdog' %(wmsSetup)
#writeConfigFile('Watchdog.cfg',watchdogSection,{'PollingTime':20,'ControlDirectory':start})
writeConfigFile('Watchdog.cfg',watchdogSection,{'ControlDirectory':start})

#setup local site SE to be automatically picked up in Job Wrapper arguments
localSESection = 'LocalSite'
writeConfigFile('LocalSE.cfg',localSESection,{'LocalSE':LOCALSE})

#find any .cfg files and append to script to run job agent, all files created in '.'
for i in os.listdir(start):
  if re.search('.cfg$',i):
    runJobAgent += i+' '

#Add DIRAC group to proxy
diracGroup = """ "from DIRAC.Core.Base import Script; Script.parseCommandLine(); from DIRAC.Core.Utilities.GridCredentials import setDIRACGroup; result = setDIRACGroup('%s'); print result" """ % (PROXY_ROLE)
diracGroupResult = runCommand('%s -c %s' %(diracPython,diracGroup),1)
printPilot('Setting DIRAC Group Result: \n%s' %(diracGroupResult),'DEBUG')

printPilot('Running DIRAC Job Agent:\n%s' %(runJobAgent),'DEBUG')
sys.stdout.flush()
os.system(runJobAgent)
sys.stdout.flush()

#############################################################################
#Perform any post-execution tasks / debugging and exit gracefully

printPilot('Post-execution proxy information:')
os.system('grid-proxy-info')
sys.stdout.flush()
if os.path.exists('%s/job/Wrapper' %(start)):
  printPilot('Saving all job wrappers to wrappers.tar.gz')
  os.system('tar cfz wrappers.tar.gz job/Wrapper')
else:
  printPilot('job/Wrapper directory does not exist','WARN')

printPilot('Execution of %s complete.' %(scriptName))
printPilot('========================================================================')
pilotExit(0)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
