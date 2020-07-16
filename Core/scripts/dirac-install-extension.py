#!/usr/bin/env python
""" dirac-install-extension command allows to add a specified extension
to an already existing DIRAC installation. No new version directory is created.
The command is based on the main DIRAC installer dirac-install.py.

The valid options are:

  -l <project> - the project in which the extension is developed
  -r <release> - the project release version
  -e <extension> - the extension name. Several -e options can be given
"""

from __future__ import unicode_literals, absolute_import, division, print_function

import os
import sys
import importlib
import getopt
import time
import six

# Get dirac-install.py source as a module in the current directory
installFile = ''
for basePath in ('pro', '.'):
  if os.path.exists(os.path.join(basePath, 'DIRAC/Core/scripts/dirac-install.py')):
    installFile = os.path.join(basePath, 'DIRAC/Core/scripts/dirac-install.py')
    break

if installFile:
  sys.path.append(os.path.dirname(installFile))
  diracInstall = importlib.import_module("dirac-install")
else:
  sys.exit(-1)

cmdOpts = (('r:', 'release=', 'Release version to install'),
           ('l:', 'project=', 'Project to install'),
           ('e:', 'extensions=', 'Extensions to install (comma separated)'),
           ('h', 'help', 'help doc string'))


def usage():
  """ Usage printout
  """
  print("\nThe command allows to add a specified extension to an already existing DIRAC installation.\n"
        "The extension can come from another project than the one installed.")
  print("\nUsage:\n\n  %s <opts> <cfgFile>" % os.path.basename(sys.argv[0]))
  print("\nOptions:")
  for cmdOpt in cmdOpts:
    print("  %s %s : %s" % (cmdOpt[0].ljust(3), cmdOpt[1].ljust(20), cmdOpt[2]))

  sys.exit(0)

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
      instName = diracInstall.cliParams.installation,
      globalDefaultsURL = diracInstall.cliParams.globalDefaults)
  if diracInstall.cliParams.debug:
    rConfig.debugCB = diracInstall.logDEBUG

  res = rConfig.loadInstallationDefaults()
  if not res['OK']:
    diracInstall.logERROR("Could not load defaults: %s" % res['Message'])

  rConfig.loadInstallationLocalDefaults(args)

  for opName in ('release', 'globalDefaults', 'extensions'):
    try:
      opVal = rConfig.getInstallationConfig(
          "LocalInstallation/%s" % (opName[0].upper() + opName[1:]))
    except KeyError:
      continue

    if isinstance(getattr(diracInstall.cliParams, opName), six.string_types):
      setattr(diracInstall.cliParams, opName, opVal)
    elif isinstance(getattr(diracInstall.cliParams, opName), bool):
      setattr(diracInstall.cliParams, opName, opVal.lower() in ("y", "yes", "true", "1"))
    elif isinstance(getattr(diracInstall.cliParams, opName), list):
      setattr(diracInstall.cliParams, opName, [opV.strip() for opV in opVal.split(",") if opV])

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
    # install under <installPath>/versions/<version>_<timestamp>
    diracInstall.cliParams.targetPath = os.path.join(
        diracInstall.cliParams.targetPath, 'versions', '%s_%s' % (diracInstall.cliParams.release, int(time.time())))
    try:
      os.makedirs(diracInstall.cliParams.targetPath)
    except BaseException:
      pass

  diracInstall.logNOTICE("Destination path for installation is %s" % diracInstall.cliParams.targetPath)
  rConfig.projectName = diracInstall.cliParams.project

  res = rConfig.loadProjectRelease(diracInstall.cliParams.release,
                                   project=diracInstall.cliParams.project,
                                   sourceURL=diracInstall.cliParams.installSource)
  if not res['OK']:
    return res

  if not rConfig.isProjectLoaded("DIRAC"):
    return diracInstall.S_ERROR("DIRAC is not depended by this installation. Aborting")

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
