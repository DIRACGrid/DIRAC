#!/usr/bin/env python
'''buildScriptsDocs

  Build scripts documentation from the scripts docstrings. The scripts are not
  very uniform

'''

import glob
import os
import sys
import subprocess

from DIRAC import rootPath

# Scripts that either do not have -h, are obsolete or cause havoc when called
BAD_SCRIPTS = ['dirac-deploy-scripts', 'dirac-install', 'dirac-compile-externals',
               'dirac-framework-self-ping', 'dirac-dms-add-files',
               ]

MARKERS_SECTIONS_SCRIPTS = [(['dms'], 'Data Management', [], []),
                            (['wms'], 'Workload Management', [], []),
                            (['dirac-proxy', 'dirac-info', 'dirac-version', 'myproxy'],
                             'Others', [], ['dirac-cert-convert.sh']),
                            # (['rss'],'Resource Status Management', [], []),
                            #  (['rms'],'Request Management', [], []),
                            # (['stager'],'Storage Management', [], []),
                            # (['transformation'], 'Transformation Management', [], []),
                            # (['admin', 'accounting', 'FrameworkSystem',
                            # 'ConfigurationSystem', 'Core',], 'Admin', [], []),
                            # ([''], 'CatchAll', [], []),
                            ]


def mkdir(path):
  """ save mkdir, ignores exceptions """
  try:
    os.makedirs(path)
  except OSError:
    pass


def runCommand(command):
  """ execute shell command, return output, catch exceptions """
  command = command.strip().split(" ")
  try:
    return subprocess.check_output(command, stderr=subprocess.STDOUT)
  except (OSError, subprocess.CalledProcessError) as e:
    print "Error when runnning", command, "\n", repr(e)
    return ''


def getScripts():
  """ get all scripts in the Dirac System, split by type admin/wms/rms/other """

  diracPath = os.path.join(rootPath, 'DIRAC')
  if not os.path.exists(diracPath):
    sys.exit('%s does not exist' % diracPath)

  # Get all scripts
  scriptsPath = os.path.join(diracPath, '*', 'scripts', '*.py')

  # Get all scripts on scriptsPath and sorts them, this will make our life easier
  # afterwards
  scripts = glob.glob(scriptsPath)
  scripts.sort()
  for scriptPath in scripts:
    # Few modules still have __init__.py on the scripts directory
    if '__init__' in scriptPath or 'build' in scriptPath:
      print "ignoring", scriptPath
      continue
    # if os.path.basename(scriptPath) in BAD_SCRIPTS:
    #   print "ignoring", scriptPath
    #   continue

    for mT in MARKERS_SECTIONS_SCRIPTS:
      if any(pattern in scriptPath for pattern in mT[0]):
        mT[2].append(scriptPath)
        break

  return


def createFoldersAndIndices():
  """ creates the index files and folders where the RST files will go

  e.g.:
  source/UserGuide/CommandReference
  """

  # create the main UserGuide Index file
  userIndexRST = """
========================================
Commands Reference (|release|)
========================================

  This page is the work in progress. See more material here soon !

.. toctree::
   :maxdepth: 1

"""

  for mT in MARKERS_SECTIONS_SCRIPTS:
    system = mT[1]
    systemString = system.replace(" ", "")
    userIndexRST += "   %s/index\n" % systemString

    print userIndexRST
    sectionPath = os.path.join(rootPath, 'DIRAC/docs/source/UserGuide/CommandReference/', systemString)
    mkdir(sectionPath)
    createSectionIndex(mT, sectionPath)

  userIndexPath = os.path.join(rootPath, 'DIRAC/docs/source/UserGuide/CommandReference/index.rst')
  with open(userIndexPath, 'w') as userIndexFile:
    userIndexFile.write(userIndexRST)


def createSectionIndex(mT, sectionPath):
  """ create the index """

  systemName = mT[1]
  systemHeader = systemName + " Command Reference"
  systemHeader = "%s\n%s\n%s\n" % ("=" * len(systemHeader), systemHeader, "=" * len(systemHeader))
  sectionIndexRST = systemHeader + """
In this subsection the %s commands are collected

.. toctree::
   :maxdepth: 2

""" % systemName

  # these scripts use pre-existing rst files, cannot re-create them automatically
  for script in mT[3]:
    scriptName = os.path.basename(script)
    sectionIndexRST += "   %s\n" % scriptName

  for script in mT[2]:
    scriptName = os.path.basename(script)
    if scriptName.endswith('.py'):
      scriptName = scriptName[:-3]
    if createScriptDocFiles(script, sectionPath, scriptName):
      sectionIndexRST += "   %s\n" % scriptName

  sectionIndexPath = os.path.join(sectionPath, 'index.rst')
  with open(sectionIndexPath, 'w') as sectionIndexFile:
    sectionIndexFile.write(sectionIndexRST)


def createScriptDocFiles(script, sectionPath, scriptName):
  """ create the RST files for all the scripts

  folders and indices already exist, just call the scripts and get the help messages...
  """
  if scriptName in BAD_SCRIPTS:
    return False

  print "Creating Doc for", scriptName
  helpMessage = runCommand("%s -h" % script)
  if not helpMessage:
    print "NO DOC For", scriptName
    return False

  scriptRSTPath = os.path.join(sectionPath, scriptName + '.rst')
  with open(scriptRSTPath, 'w') as rstFile:
    rstFile.write('=' * len(scriptName))
    rstFile.write('\n%s\n' % scriptName)
    rstFile.write('=' * len(scriptName))
    rstFile.write('\n')
    for line in helpMessage.splitlines():
      line = line.rstrip()
      newLine = line + ":\n\n" if line.endswith(":") else line + "\n"
      rstFile.write(newLine)

  return True


def run():
  ''' creates the rst files right in the source tree of the docs
  '''

  getScripts()
  createFoldersAndIndices()

  print 'Done'


if __name__ == "__main__":
  run()
