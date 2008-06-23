########################################################################
# $Id: Script.py,v 1.1 2008/06/23 14:36:30 paterson Exp $
# File :   Script.py
# Author : Stuart Paterson
########################################################################

""" The Script class provides a simple way for users to specify an executable
    or file to run (and is also a simple example of a workflow module).
"""

__RCSID__ = "$Id: Script.py,v 1.1 2008/06/23 14:36:30 paterson Exp $"


from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC                           import gLogger, S_OK, S_ERROR

import os,sys,re

class Script(object):

  #############################################################################
  def __init__(self):
    self.log = gLogger.getSubLogger( "Script" )
    #Set defaults for all workflow parameters here
    self.name = ''
    self.executable = ''
    self.logFile = ''
    self.arguments = ''

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    result = S_OK()
    if self.step_commons.has_key('name'):
      self.name = self.step_commons['name']
    else:
      result = S_ERROR('No module instance name defined')
      self.log.warn('No module instance name defined')

    if self.step_commons.has_key('executable'):
      self.executable = self.step_commons['executable']
    else:
      result = S_ERROR('No executable defined')
      self.log.warn('No executable defined')

    if self.step_commons.has_key('logFile'):
      self.logFile = self.step_commons['logFile']
    else:
      result = S_ERROR('No logFile defined')
      self.log.warn('No logFile defined')

    if self.step_commons.has_key('arguments'):
      self.arguments = self.step_commons['arguments']

    return result

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    result = self.resolveInputVariables()
    if not result['OK']:
      return result
    self.log.info('Script Module Instance Name: %s' %(self.name))
    if os.path.exists(os.path.basename(self.executable)):
      self.executable = os.path.basename(self.executable)
      if not os.access('%s/%s' %(os.getcwd(),self.executable), 5):
        os.chmod('%s/%s' %(os.getcwd(),self.executable),0755)
    cmd = self.executable
    if re.search('.py$',self.executable):
      cmd = '%s %s' %(sys.executable,self.executable)
    if re.search('.sh$',self.executable) or re.search('.csh$',self.executable):
      cmd = './%s' %(self.executable)
    if self.arguments:
      cmd = '%s %s' %(cmd,self.arguments)

    self.log.info('Command is: %s' %cmd)
    outputDict = shellCall(0,cmd)
    if not outputDict:
      self.log.info('Could not execute script %s' %(self.executable))
    if not outputDict['OK']:
      self.log.info('Shell call execution failed:')
      self.log.info(outputDict['Message'])
    resTuple = outputDict['Value']
    status = resTuple[0]
    stdout = resTuple[1]
    stderr = resTuple[2]
    self.log.info('%s execution completed with status %s' %(self.executable,status))
    self.log.verbose(stdout)
    self.log.verbose(stderr)
    if os.path.exists(self.logFile):
      self.log.verbose('Removing existing %s' % self.logFile)
      os.remove(self.logFile)
    fopen = open('%s/%s' %(os.getcwd(),self.logFile),'w')
    fopen.write('<<<<<<<<<< %s Standard Output >>>>>>>>>>\n\n%s ' %(self.executable,stdout))
    if stderr:
      fopen.write('<<<<<<<<<< %s Standard Error >>>>>>>>>>\n\n%s ' %(self.executable,stderr))
    fopen.close()
    self.log.info('Output written to %s, execution complete.' % (self.logFile))
    return S_OK()

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#