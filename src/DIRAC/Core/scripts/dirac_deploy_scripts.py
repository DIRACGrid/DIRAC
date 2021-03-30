#!/usr/bin/env python
"""
Deploy all scripts and extensions for python2 installations.
Useless in python3.

This script is not meant to be called by users (it's automatically called by dirac-install).

Usage:
  dirac-deploy-scripts [options] ... ... <python path>

Arguments:
  python path:    you can specify the folder where your python installation should be fetched from
                  to replace the shebang

Example:
  $ dirac-deploy-scripts
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import os
import re
import sys
import stat
import getopt
import shutil

cmdOpts = (('', 'symlink', 'this will create symlinks instead of wrappers'),
           ('', 'module=', 'module in which to look for the scripts'),
           ('h', 'help', 'help doc string'))


def usage(err=''):
  """ Usage printout

      :param str err: will print something like "option -a not recognized"
  """
  if err:
    print(err)
  print(__doc__)
  print('Options::\n\n')
  for cmdOpt in cmdOpts:
    print("  %s %s : %s" % (cmdOpt[0].ljust(3), cmdOpt[1].ljust(20), cmdOpt[2]))

  sys.exit(2 if err else 0)


DEBUG = False

moduleSuffix = "DIRAC"
gDefaultPerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
excludeMask = ['__init__.py']
simpleCopyMask = [os.path.basename(__file__),
                  'dirac-compile-externals.py',
                  'dirac-install.py',
                  'dirac-install-extension.py',
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
# Module in which to look for the scripts
module = None

try:
  opts, args = getopt.getopt(sys.argv[1:],
                             "".join([opt[0] for opt in cmdOpts]),
                             [opt[1] for opt in cmdOpts])
except getopt.GetoptError as err:
  # print help information and exit
  usage(str(err))
for o, a in opts:
  if o in ('-h', '--help'):
    usage()
  if o == "--symlink":
    useSymlinks = True
  elif o == "--module":
    module = a
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

if module:
  listDir = module.split(',')
else:
  listDir = os.listdir(rootPath)
  # DIRAC scripts need to be treated first, so that its scripts
  # can be overwritten by the extensions
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
