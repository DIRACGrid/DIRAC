#!/usr/bin/env python
'''
  dirac-production-runjoblocal
 
  Module created to run failed jobs locally on a CVMFS-configured machine.
  It creates the necessary environment, downloads the necessary files, modifies the necessary 
  files and runs the job
  
  Usage:
    dirac-production-diracrunjoblocal (job ID)  -  No parenthesis
    
'''

__RCSID__ = "$transID: dirac-production-runjoblocal.py 61232 2015-09-22 16:20:00 msoares $"


import DIRAC
import os
import sys
import errno
import shutil
from DIRAC.Core.Base      import Script
from DIRAC                import S_OK, S_ERROR

Script.registerSwitch( 'D:', 'Download='    , 'Defines data acquisition as DownloadInputData'   )
Script.registerSwitch( 'P:', 'Protocol='    , 'Defines data acquisition as InputDataByProtocol' )
Script.parseCommandLine( ignoreErrors = False )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                      '\nUsage:',
                                      'dirac-production-runjoblocal [Data imput mode] [job ID]'
                                      '\nArguments:',
                                      '  Download (Job ID): Defines data aquisition as DownloadInputData',
                                      '  Protocol (Job ID): Defines data acquisition as InputDataByProtocol\n'] ) )

_downloadinputdata = False
_jobID = None

for switch in Script.getUnprocessedSwitches():
  if switch [ 0 ] in ( 'D', 'Download' ):
    _downloadinputdata = True
    _jobID = switch[1]
  if switch [ 0 ] in ( 'I', 'Protocol' ):
    _downloadinputdata = False
    _jobID = switch[1]

def __runSystemDefaults(jobID, vo):
  """
  Creates the environment for running the job and returns
  the path for the other functions.
  
  """

  
  tempdir = str(vo) + "job" + str(jobID) + "temp"

  try:
    os.mkdir(tempdir)
    if not sys.exc_info()[1][0]:
      S_OK("Temporary directory created.")    
    pass
  except:    
    if sys.exc_info()[1][0] == 17:
      S_OK("Temporary directory already exists.")
    elif sys.exc_info()[1][0] == 30:
      print sys.exc_info()[1], "Unable to create temporary directory"
    
  basepath = os.getcwd()
  return basepath + os.path.sep + tempdir + os.path.sep

def __downloadJobDescriptionXML(jobID, basepath):
  """
  Downloads the jobDescription.xml file into the temporary directory
  created.
  
  """
  from DIRAC.Interfaces.API.Dirac import Dirac
  jdXML = Dirac()
  jdXML.getInputSandbox(jobID, basepath)

def __modifyJobDescription(jobID, basepath, downloadinputdata):
  """
  Modifies the jobDescription.xml to, instead of DownloadInputData, it 
  uses InputDataByProtocol
  
  """
  if not downloadinputdata:
    from xml.etree import ElementTree as et
    archive = et.parse(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")
    for element in archive.getiterator():
      if element.text == "DIRAC.WorkloadManagementSystem.Client.DownloadInputData":
        element.text = "DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol"
        archive.write(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")
        S_OK("Job parameter changed from DownloadInputData to InputDataByProtocol.")

  
def __downloadPilotScripts(basepath, diracpath):
  """
  Downloads the scripts necessary to configure the pilot
  
  """
  shutil.copyfile(str(diracpath) + os.path.sep + "WorkloadManagementSystem/PilotAgent/dirac-pilot.py"   , basepath + "dirac-pilot.py")
  shutil.copyfile(str(diracpath) + os.path.sep + "WorkloadManagementSystem/PilotAgent/pilotCommands.py" , basepath + "pilotCommands.py")
  shutil.copyfile(str(diracpath) + os.path.sep + "WorkloadManagementSystem/PilotAgent/pilotTools.py"    , basepath + "pilotTools.py")
      
def __configurePilot(basepath, vo):
  """
  Configures the pilot.
  This method was created specifically for LHCb pilots, more info
  about othe VOs is needed to make it more general.
  """

  from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals    import getVO, getSetup
  from DIRAC.ConfigurationSystem.Client.ConfigurationData    import gConfigurationData
  

  
  vo = getVO()
  currentSetup = getSetup()
  masterCS = gConfigurationData.getMasterServer()

  os.system("python " + basepath + "dirac-pilot.py -S %s -l %s -C %s -N ce.debug.ch -Q default -n DIRAC.JobDebugger.ch -dd" %(currentSetup, vo, masterCS))
  
  dir = os.path.expanduser('~') + os.path.sep
  try:
    os.rename(dir + '.dirac.cfg', dir + '.dirac.cfg.old')
  except OSError:
    pass
  shutil.copyfile(dir + 'pilot.cfg', dir + '.dirac.cfg')

def __runJobLocally(jobID, basepath, vo):
  """
  Runs the job!
  
  """
  ipr = __import__(str(vo) + 'DIRAC.Interfaces.API.' + str(vo) + 'Job', globals(), locals(), [str(vo) + 'Job'], -1)
  voJob = getattr(ipr, str(vo) + 'Job')
  localJob = voJob(basepath + "InputSandbox" + str(jobID) + os.path.sep + "jobDescription.xml")
  localJob.setInputSandbox(os.getcwd()+ os.path.sep+"pilot.cfg")
  localJob.setConfigArgs(os.getcwd()+ os.path.sep+"pilot.cfg")
  os.chdir(basepath)
  localJob.runLocal()
  

if __name__ == "__main__":
  from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import Extensions
  ext = Extensions()
  _vo = ext.getCSExtensions()[0]
  _diracPath = Extensions().getExtensionPath('DIRAC')
  _dir = os.path.expanduser('~') + os.path.sep
  try:
    _path = __runSystemDefaults(_jobID, _vo)
      
    __downloadJobDescriptionXML(_jobID, _path)
      
    __modifyJobDescription(_jobID, _path, _downloadinputdata)
    
    __downloadPilotScripts(_path, _diracPath)
    
    __configurePilot(_path, _vo)
    
    __runJobLocally(_jobID, _path, _vo)
    
  finally:
    os.chdir(_dir)
    os.rename(_dir + '.dirac.cfg.old', _dir + '.dirac.cfg')