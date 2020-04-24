#!/usr/bin/env python
"""
Deploy all scripts and extensions
Options:
 * --symlink: this will create symlinks instead of wrappers
 * <python path>: you can specify the folder where your python installation should be fetched from
                  to replace the shebang
"""
from __future__ import print_function
__RCSID__ = "$Id$"

import getopt
import os
import shutil
import stat
import re
import sys

DEBUG = False

moduleSuffix = "DIRAC"
gDefaultPerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
excludeMask = ['__init__.py']
simpleCopyMask = [os.path.basename(__file__),
                  'dirac-compile-externals.py',
                  'dirac-install.py',
                  'dirac-platform.py',
                  'dirac_compile_externals.py',
                  'dirac_install.py',
                  'dirac_platform.py']

wrapperTemplate = """#!/bin/bash
source "$DIRAC/bashrc"
export DCOMMANDS_PPID=$PPID
exec $PYTHONLOCATION$ $DIRAC/$SCRIPTLOCATION$ "$@"
"""

# Python interpreter location can be specified as an argument
pythonLocation = "/usr/bin/env python"
# if True, do not use the script wrapper but just use symlinks
useSymlinks = False


try:
  opts, args = getopt.getopt(sys.argv[1:], "", ["symlink"])
except getopt.GetoptError as err:
  # print help information and exit:
  print(str(err))  # will print something like "option -a not recognized"
  sys.exit(2)
for o, a in opts:
  if o == "--symlink":
    useSymlinks = True
  else:
    assert False, "unhandled options %s" % o


if args:
  pythonLocation = os.path.join(args[0], 'bin', 'python')

wrapperTemplate = wrapperTemplate.replace('$PYTHONLOCATION$', pythonLocation)


def lookForScriptsInPath(basePath, rootModule):
  isScriptsDir = os.path.split(rootModule)[1] == "scripts"
  scriptFiles = []
  for entry in os.listdir(basePath):
    absEntry = os.path.join(basePath, entry)
    if os.path.isdir(absEntry):
      scriptFiles.extend(lookForScriptsInPath(absEntry, os.path.join(rootModule, entry)))
    elif isScriptsDir and os.path.isfile(absEntry):
      scriptFiles.append((os.path.join(rootModule, entry), entry))
  return scriptFiles


def findDIRACRoot(path):
  dirContents = os.listdir(path)
  if 'DIRAC' in dirContents and os.path.isdir(os.path.join(path, 'DIRAC')):
    return path
  parentPath = os.path.dirname(path)
  if parentPath == path or len(parentPath) == 1:
    return False
  return findDIRACRoot(os.path.dirname(path))


rootPath = findDIRACRoot(os.path.dirname(os.path.realpath(__file__)))
if not rootPath:
  print("Error: Cannot find DIRAC root!")
  sys.exit(1)

targetScriptsPath = os.path.join(rootPath, "scripts")
pythonScriptRE = re.compile("(.*/)*([a-z]+-[a-zA-Z0-9-]+|[a-z]+_[a-zA-Z0-9_]+|d[a-zA-Z0-9-]+).py$")
print("Scripts will be deployed at %s" % targetScriptsPath)

if not os.path.isdir(targetScriptsPath):
  os.mkdir(targetScriptsPath)


# DIRAC scripts need to be treated first, so that its scripts
# can be overwritten by the extensions
listDir = os.listdir(rootPath)
if 'DIRAC' in listDir:  # should always be true...
  listDir.remove('DIRAC')
  listDir.insert(0, 'DIRAC')

for rootModule in listDir:
  modulePath = os.path.join(rootPath, rootModule)
  if not os.path.isdir(modulePath):
    continue
  extSuffixPos = rootModule.find(moduleSuffix)
  if extSuffixPos == -1 or extSuffixPos != len(rootModule) - len(moduleSuffix):
    continue
  print(("Inspecting %s module" % rootModule))
  scripts = lookForScriptsInPath(modulePath, rootModule)
  for script in scripts:
    scriptPath = script[0]
    scriptName = script[1]
    if scriptName in excludeMask:
      continue
    scriptLen = len(scriptName)
    if scriptName not in simpleCopyMask and pythonScriptRE.match(scriptName):
      newScriptName = scriptName[:-3].replace('_', '-')
      if DEBUG:
        print((" Wrapping %s as %s" % (scriptName, newScriptName)))
      fakeScriptPath = os.path.join(targetScriptsPath, newScriptName)

      # Either create the symlink or write the wrapper script
      if useSymlinks:
        # We may overwrite already existing links (in extension for example)
        # os.symlink will not allow that, so remove the existing first
        if os.path.exists(fakeScriptPath):
          os.remove(fakeScriptPath)
        # Create the symlink
        os.symlink(os.path.join(rootPath, scriptPath), fakeScriptPath)
      else:
        with open(fakeScriptPath, "w") as fd:
          fd.write(wrapperTemplate.replace('$SCRIPTLOCATION$', scriptPath))

      os.chmod(fakeScriptPath, gDefaultPerms)
    else:
      if DEBUG:
        print((" Copying %s" % scriptName))
      shutil.copy(os.path.join(rootPath, scriptPath), targetScriptsPath)
      copyPath = os.path.join(targetScriptsPath, scriptName)
      os.chmod(copyPath, gDefaultPerms)
      cLen = len(copyPath)
      reFound = pythonScriptRE.match(copyPath)
      if reFound:
        pathList = list(reFound.groups())
        pathList[-1] = pathList[-1].replace('_', '-')
        destPath = "".join(pathList)
        if DEBUG:
          print((" Renaming %s as %s" % (copyPath, destPath)))
        os.rename(copyPath, destPath)
