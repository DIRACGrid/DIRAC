#!/usr/bin/env python
"""
Allows to add a specified extension to an already existing DIRAC installation.
The extension can come from another project than the one installed.
No new version directory is created. The command is based on the main DIRAC installer dirac-install.py.

Usage:
  dirac-install-extension [options] ...

"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import os
import sys
import six
import getopt
import importlib

cmdOpts = (('r:', 'release=', 'Release version to install'),
           ('l:', 'project=', 'Project to install'),
           ('e:', 'extensions=', 'Extensions to install (comma separated). Several -e options can be given'),
           ('M:', 'defaultsURL=', 'Where to retrieve the global defaults from'),
           ('h', 'help', 'help doc string'))


def usage():
  """ Usage printout
  """
  print(__doc__)
  print('Options::\n\n')
  for cmdOpt in cmdOpts:
    print("  %s %s : %s" % (cmdOpt[0].ljust(3), cmdOpt[1].ljust(20), cmdOpt[2]))

  sys.exit(0)


# Import dirac-install.py as a module
installFile = ''
for basePath in ('pro', '.'):
  if os.path.exists(os.path.join(basePath, 'DIRAC/Core/scripts/dirac-install.py')):
    installFile = os.path.join(basePath, 'DIRAC/Core/scripts/dirac-install.py')
    break

if installFile:
  sys.path.append(os.path.dirname(installFile))
  diracInstall = importlib.import_module("dirac-install")
else:
  usage()
  sys.exit(-1)


def loadConfiguration():
  """
  It loads the configuration file
  """
  optList, args = getopt.getopt(sys.argv[1:],
                                "".join([opt[0] for opt in cmdOpts]),
                                [opt[1] for opt in cmdOpts])

  # First check if the name is defined
  for opt, value in optList:
    if opt in ('-h', '--help'):
      usage()
    elif opt in ("-M", "--defaultsURL"):
      diracInstall.cliParams.globalDefaults = value

  rConfig = diracInstall.ReleaseConfig(
      instName=diracInstall.cliParams.installation,
      globalDefaultsURL=diracInstall.cliParams.globalDefaults)
  if diracInstall.cliParams.debug:
    rConfig.debugCB = diracInstall.logDEBUG

  res = rConfig.loadInstallationDefaults()
  if not res['OK']:
    diracInstall.logERROR("Could not load defaults: %s" % res['Message'])

  rConfig.loadInstallationLocalDefaults(args)

  for opName in ('release', 'globalDefaults', 'useVersionsDir'):
    try:
      opVal = rConfig.getInstallationConfig(
          "LocalInstallation/%s" % (opName[0].upper() + opName[1:]))
    except KeyError:
      continue

    if isinstance(getattr(diracInstall.cliParams, opName), six.string_types):
      setattr(diracInstall.cliParams, opName, opVal)
    elif isinstance(getattr(diracInstall.cliParams, opName), bool):
      setattr(diracInstall.cliParams, opName, opVal.lower() in ("y", "yes", "true", "1"))

  # Now parse the ops
  for opt, value in optList:
    if opt in ('-r', '--release'):
      diracInstall.cliParams.release = value
    elif opt in ('-l', '--project'):
      diracInstall.cliParams.project = value
    elif opt in ('-e', '--extensions'):
      for pkg in [p.strip() for p in value.split(",") if p.strip()]:
        if pkg not in diracInstall.cliParams.extensions:
          diracInstall.cliParams.extensions.append(pkg)

  if not diracInstall.cliParams.release and not diracInstall.cliParams.modules:
    diracInstall.logERROR("Missing release to install")
    usage()

  diracInstall.cliParams.basePath = diracInstall.cliParams.targetPath
  if diracInstall.cliParams.useVersionsDir:
    # install under the pro directory
    diracInstall.cliParams.targetPath = os.path.join(diracInstall.cliParams.targetPath, 'pro')

  diracInstall.logNOTICE("Destination path for installation is %s" % diracInstall.cliParams.targetPath)
  rConfig.projectName = diracInstall.cliParams.project

  res = rConfig.loadProjectRelease(diracInstall.cliParams.release,
                                   project=diracInstall.cliParams.project,
                                   sourceURL=diracInstall.cliParams.installSource)
  if not res['OK']:
    return res

  # Reload the local configuration to ensure it takes prescience
  rConfig.loadInstallationLocalDefaults(args)

  return diracInstall.S_OK(rConfig)


if __name__ == "__main__":

  result = loadConfiguration()
  if result['OK']:
    releaseConfig = result['Value']
  else:
    diracInstall.logERROR('Can not load the configuration: %s' % result['Message'])
    sys.exit(-1)

  result = releaseConfig.getModulesToInstall(diracInstall.cliParams.release, diracInstall.cliParams.extensions)

  for extension in diracInstall.cliParams.extensions:
    if ":" in extension:
      extension = extension.split(":")[0]
    extUrl = result['Value'][1][extension][0]
    extVersion = result['Value'][1][extension][1]

    diracInstall.logNOTICE('Installing extension %s:%s' % (extension, extVersion))

    if not diracInstall.downloadAndExtractTarball(extUrl, extension, extVersion):
      diracInstall.logERROR('Failed to install %s' % extension)
      sys.exit(-1)

  # (Re)deploy scripts now taking into account the newly installed extensions
  ddeLocation = os.path.join(diracInstall.cliParams.targetPath, "DIRAC", "Core",
                             "scripts", "dirac-deploy-scripts.py")
  if not os.path.isfile(ddeLocation):
    ddeLocation = os.path.join(diracInstall.cliParams.targetPath, "DIRAC", "Core",
                               "scripts", "dirac_deploy_scripts.py")
  if os.path.isfile(ddeLocation):
    cmd = ddeLocation

  # if specified, create symlink instead of wrapper.
  if diracInstall.cliParams.scriptSymlink:
    cmd += ' --symlink'

  extensionList = []
  for extension in diracInstall.cliParams.extensions:
    if ":" in extension:
      extension = extension.split(":")[0]
    extensionList.append(extension.split(":")[0])
  cmd += " --module " + ','.join(extensionList)
  os.system(cmd)
