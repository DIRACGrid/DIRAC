""" The Script class provides a simple way for users to specify an executable
    or file to run (and is also a simple example of a workflow module).
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import re
import stat
import shlex
import distutils.spawn  # pylint: disable=no-name-in-module,no-member,import-error

from DIRAC import gLogger
from DIRAC.Core.Utilities.Subprocess import systemCall

from DIRAC.Workflow.Modules.ModuleBase import ModuleBase


class Script(ModuleBase):
  """ Module for running executable
  """

  #############################################################################
  def __init__(self, log=None):
    """ c'tor
    """
    if log is not None:
      self.log = log
    else:
      self.log = gLogger.getSubLogger('Script')
    super(Script, self).__init__(self.log)

    # Set defaults for all workflow parameters here
    self.executable = ''
    self.applicationName = ''
    self.applicationVersion = ''
    self.applicationLog = ''
    self.arguments = ''
    self.workflow_commons = None
    self.step_commons = None

    self.environment = None
    self.callbackFunction = None
    self.bufferLimit = 52428800

  #############################################################################

  def _resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    super(Script, self)._resolveInputVariables()
    super(Script, self)._resolveInputStep()

    self.arguments = self.step_commons.get('arguments', self.arguments)
    if not self.arguments.strip():
      self.arguments = self.workflow_commons.get('arguments', self.arguments)

  #############################################################################

  def _initialize(self):
    """ simple checks
    """
    if not self.executable:
      raise RuntimeError('No executable defined')

  def _setCommand(self):
    """ set the command that will be executed
    """
    self.command = self.executable
    if os.path.exists(os.path.basename(self.executable)):
      self.executable = os.path.basename(self.executable)
      if not os.access('%s/%s' % (os.getcwd(), self.executable), 5):
        # doc in https://docs.python.org/2/library/stat.html#stat.S_IRWXU
        os.chmod('%s/%s' % (os.getcwd(), self.executable),
                 stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
      self.command = '%s/%s' % (os.getcwd(), self.executable)
    elif re.search('.py$', self.executable):
      self.command = '%s %s' % (sys.executable, self.executable)
    elif distutils.spawn.find_executable(self.executable):  # pylint: disable=no-member
      self.command = self.executable

    if self.arguments:
      self.command = '%s %s' % (self.command, self.arguments)

    self.log.info('Command is: %s' % self.command)

  def _executeCommand(self):
    """ execute the self.command (uses systemCall)
    """
    failed = False

    outputDict = systemCall(timeout=0,
                            cmdSeq=shlex.split(self.command),
                            env=self.environment,
                            callbackFunction=self.callbackFunction,
                            bufferLimit=self.bufferLimit)
    if not outputDict['OK']:
      failed = True
      self.log.error('System call execution failed:', '\n' + str(outputDict['Message']))
    status, stdout, stderr = outputDict['Value'][0:3]
    if status:
      failed = True
      self.log.error("Non-zero status while executing", "%s exited with status %s" % (self.command, status))
    else:
      self.log.info("%s execution completed with status %s" % (self.command, status))

    self.log.verbose(stdout)
    self.log.verbose(stderr)
    if os.path.exists(self.applicationLog):
      self.log.verbose('Removing existing %s' % self.applicationLog)
      os.remove(self.applicationLog)
    with open('%s/%s' % (os.getcwd(), self.applicationLog), 'w') as fopen:
      fopen.write("<<<<<<<<<< %s Standard Output >>>>>>>>>>\n\n%s " % (self.executable, stdout))
      if stderr:
        fopen.write("<<<<<<<<<< %s Standard Error >>>>>>>>>>\n\n%s " % (self.executable, stderr))
    self.log.info("Output written to %s, execution complete." % (self.applicationLog))

    if failed:
      self._exitWithError(status)

  def _exitWithError(self, status):
    """ Here because of possible extensions.

        :param str status: the status of the application becomes the status of the workflow,
                           and may be interpreted by JobWrapper (e.g. for rescheduling cases)
    """
    raise RuntimeError("'%s' Exited With Status %s" % (os.path.basename(self.executable).split('_')[0], status),
                       status)

  def _finalize(self):
    """ simply finalize
    """
    applicationString = os.path.basename(self.executable).split('_')[0]
    if self.applicationName and self.applicationName.lower() != 'unknown':
      applicationString += ' (%s %s)' % (self.applicationName, self.applicationVersion)
    status = "%s successful" % applicationString

    super(Script, self)._finalize(status)
