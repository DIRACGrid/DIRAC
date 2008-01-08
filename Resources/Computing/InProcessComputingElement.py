########################################################################
# $Id: InProcessComputingElement.py,v 1.4 2008/01/08 22:47:08 paterson Exp $
# File :   InProcessComputingElement.py
# Author : Stuart Paterson
########################################################################

""" The simplest Computing Element instance that submits jobs locally.
"""

__RCSID__ = "$Id: InProcessComputingElement.py,v 1.4 2008/01/08 22:47:08 paterson Exp $"

from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC                                               import S_OK, S_ERROR

import os,sys

CE_NAME = 'InProcess'

class InProcessComputingElement(ComputingElement):

  #############################################################################
  def __init__(self):
    """ Standard constructor.
    """
    ComputingElement.__init__(self,CE_NAME)
    self.submittedJobs = 0

  #############################################################################
  def submitJob(self,executableFile,jdl,localID):
    """ Method to submit job, should be overridden in sub-class.
    """
    if not os.access(executableFile, 5):
      os.chmod(executableFile,0755)
    cmd = os.path.abspath(executableFile)
    self.log.verbose('CE submission command: %s' %(cmd))
    result = shellCall(0,cmd,callbackFunction = self.sendOutput)
    if not result['OK']:
      self.log.warn('===========>In process CE result NOT OK')
      self.log.debug(result)
    else:
      self.log.debug('InProcess CE result OK')

    self.submittedJobs += 1
    return S_OK(localID)

  #############################################################################
  def getDynamicInfo(self):
    """ Method to return information on running and pending jobs.
    """
    result = {}
    result['SubmittedJobs'] = 0
    result['RunningJobs'] = 0
    result['WaitingJobs'] = 0
    return S_OK(result)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#