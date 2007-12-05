#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/PilotAgent/Attic/dirac-pilot-lcg.py,v 1.2 2007/12/05 16:53:52 paterson Exp $
# File :   dirac-pilot-lcg.py
# Author : Stuart Paterson
########################################################################

import os,sys,string,re

""" The DIRAC Pilot script for LCG performs initial checks on the sanity of the WN
    environment then installs and configures DIRAC.  The Pilot script then runs
    a DIRAC JobAgent that can make requests to the central WMS for pending jobs
    for the VO.
"""

__RCSID__ = "$Id: dirac-pilot-lcg.py,v 1.2 2007/12/05 16:53:52 paterson Exp $"


DEBUG = 1
DIRAC_URL = 'http://cern.ch/lhcbproject/dist/DIRAC3'
SW_DIR = 'VO_LHCB_SW_DIR'
SW_PATH ='lib'
DIRAC_PYTHON ='2.4'
INSTALL_RETRIES = 5
MIN_DISK_SPACE = 2560 #MB
CLEANUP = 0

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
  pilotOutput.close()
  sys.exit(1)

printPilot('Hostname = %s' %(runCommand('hostname')))
printPilot('LocalAccount = %s' %(runCommand('whoami')))
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
  pilotOutput.close()
  sys.exit(1)

if not LCG_SITE_CE:
  printPilot('LCG_SITE_CE is not defined, exiting','ERROR')
  pilotOutput.close()
  sys.exit(1)

printPilot('========================================================================')

#############################################################################
#Installation and configuration of DIRAC

TARFILE = 'DIRAC-%s.tar.gz' % diracSetup
diracDist = '%s/%s' %(DIRAC_URL,TARFILE)
printPilot('DIRAC Tar File to be downloaded is: %s' %(diracDist))
installDIRAC = './dirac-install -f -p %s ' %() #perform full DIRAC installation for now

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
  printPilot('Could not install DIRAC from $s, exiting' %(diracDist),'ERROR')
  pilotOutput.close()
  sys.stdout.flush()
  sys.exit(1)

#Initial setup of DIRAC to enable CS settings
initialDIRACsetup = './scripts/dirac-setup -s LCG.Unknown.ch'
printPilot('>>>>>>>>>>Start: Initial DIRAC Setup Log','DEBUG')
if DEBUG:
  sys.stdout.flush()
  print initialDIRACsetup
printPilot('<<<<<<<<<<End: Initial DIRAC Setup Log','DEBUG')
if DEBUG:
  printPilot('Checking local configuration file:','DEBUG')
  if os.path.exists('etc/dirac.cfg'):
    cfg = runCommand('cat etc/dirac.cfg',1)
    sys.stdout.flush()
    print cfg
  else:
    printPilot('etc/dirac.cfg file does not exist','WARN')

#Get site name from CS and repeat setup
if not DIRAC_PYTHON:
  diracPython='%s/%s/bin/python%s' %(start,CMTCONFIG,DIRAC_PYTHON)
  printPilot('Using locally installed DIRAC python: %s' %(diracPython))
else:
  diracPython=DIRAC_PYTHON
  printPilot('Using DIRAC python from shared area: %s' %(diracPython))

getSite = """ "from DIRAC import gConfig; result = gConfig.getOptionsDict('/Resources/GridSites/LCG'); print result" """
siteDictStr = runCommand('%s -c %s' %(diracPython,getSite))
try:
  siteDict = dict(siteDictStr)
except Exception,x:
  printPilot('Could not obtain LCG site list from CS with exception:','ERROR')
  printPilot(str(x),'ERROR')
  pilotOutput.close()
  sys.exit(1)

if not siteDict['OK']:
  printPilot('Returned LCG site dictionary not OK','ERROR')
  printPilot(siteDict['Message'],'ERROR')
  pilotOutput.close()
  sys.stdout.flush()
  sys.exit(1)

sites = siteDict['Value']
DIRAC_SITE_NAME = ''
for ce,siteName in siteDict.items():
  if LCG_SITE_CE == ce:
    printPilot('Found DIRAC site name: %s' %(ce))
    DIRAC_SITE_NAME = siteName

if not DIRAC_SITE_NAME:
  printPilot('No DIRAC site names were found for CE %s' %(LCG_SITE_CE),'ERROR')
  pilotOutput.close()
  sys.stdout.flush()
  sys.exit(1)
#-s $LCG_NEW_SITE_NAME -m $DIRACInstance -f $FACTOR -q $LCG_SITE_CE -t $time -a $CMTCONFIG -p $SITETYPE LCG.ini
#Full setup of DIRAC with LCG site name
fullDIRACsetup = './scripts/dirac-setup -m %s -q %s -a %s -p %s ' %(diracSetup,LCG_SITE_CE,CMTCONFIG,'LCG')
printPilot('>>>>>>>>>>Start: Full DIRAC Setup Log','DEBUG')
if DEBUG:
  sys.stdout.flush()
  print fullDIRACsetup
printPilot('<<<<<<<<<<End: Full DIRAC Setup Log','DEBUG')
if DEBUG:
  printPilot('Checking local configuration file:','DEBUG')
  if os.path.exists('etc/dirac.cfg'):
    cfg = runCommand('cat etc/dirac.cfg',1)
    sys.stdout.flush()
    print cfg
  else:
    printPilot('etc/dirac.cfg file does not exist','WARN')

#############################################################################
#Start DIRAC Job Agent

runJobAgent = '%s DIRAC/Core/scripts/dirac-agent WorkloadManagement/JobAgent -o LogLevel=debug' %(diracPython)
printPilot('Running DIRAC Job Agent:\n%s' %(runJobAgent),'DEBUG')
os.system(runJobAgent)
sys.stdout.flush()

#############################################################################
#Perform any post-execution tasks / debugging and exit gracefully

printPilot('Files in current directory are:')
for i in os.listdir('.'): print i

printPilot('Post-execution proxy information:')
os.system('grid-proxy-info')
sys.stdout.flush()
printPilot('Execution of %s complete.' %(scriptName))
printPilot('========================================================================')
pilotOutput.close()
sys.stdout.flush()
sys.exit(0)
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
