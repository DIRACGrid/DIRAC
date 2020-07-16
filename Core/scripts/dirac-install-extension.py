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
import shutil

if __name__ == "__main__":

  # Get dirac-install.py source as a module in the current directory
  installFile = ''
  for basePath in ('pro', '.'):
    if os.path.exists(os.path.join(basePath, 'DIRAC/Core/scripts/dirac-install.py')):
      installFile = os.path.join(basePath, 'DIRAC/Core/scripts/dirac-install.py')
      break

  if installFile:
    shutil.copy(installFile, 'dirac_install.py')
    sys.path.append('.')
  else:
    sys.exit(-1)

  # Import dirac-install goodies
  from dirac_install import cliParams, loadConfiguration, downloadAndExtractTarball, logERROR, logNOTICE # pylint: disable=import-error


  result = loadConfiguration()
  if result['OK']:
    releaseConfig = result['Value']
  else:
    logERROR('Can not load the configuration: %s' % result['Message'])
    sys.exit(-1)

  result = releaseConfig.getModulesToInstall(cliParams.release, cliParams.extensions)

  for extension in cliParams.extensions:
    if ":" in extension:
      extension = extension.split(":")[0]
    extUrl = result['Value'][1][extension][0]
    extVersion = result['Value'][1][extension][1]

    logNOTICE('Installing extension %s:%s' % (extension, extVersion))

    if not downloadAndExtractTarball(extUrl, extension, extVersion):
      logERROR('Failed to install %s' % extension)
      sys.exit(-1)

  # (Re)deploy scripts now taking into account the newly installed extensions
  ddeLocation = os.path.join(cliParams.targetPath, "DIRAC", "Core",
                             "scripts", "dirac-deploy-scripts.py")
  if not os.path.isfile(ddeLocation):
    ddeLocation = os.path.join(cliParams.targetPath, "DIRAC", "Core",
                               "scripts", "dirac_deploy_scripts.py")
  if os.path.isfile(ddeLocation):
    cmd = ddeLocation

  # if specified, create symlink instead of wrapper.
  if cliParams.scriptSymlink:
    cmd += ' --symlink'

  extensionList = []
  for extension in cliParams.extensions:
    if ":" in extension:
      extension = extension.split(":")[0]
    extensionList.append(extension.split(":")[0])
  cmd += " --module " + ','.join(extensionList)
  os.system(cmd)

  # Clean up
  os.unlink('dirac_install.py')
  if os.path.exists('dirac_install.pyc'):
    os.unlink('dirac_install.pyc')
  if os.path.exists('dirac_install.pyo'):
    os.unlink('dirac_install.pyo')
