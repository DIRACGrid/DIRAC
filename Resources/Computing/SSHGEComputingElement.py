########################################################################
# $HeadURL$
# File :   SSHGEComputingElement.py
# Author : A.T. V.H.
########################################################################

""" Grid Engine Computing Element with remote job submission via ssh/scp and using site
    shared area for the job proxy placement
"""

__RCSID__ = "092c1d9 (2011-06-02 15:20:46 +0200) atsareg <atsareg@in2p3.fr>"

from DIRAC.Resources.Computing.SSHComputingElement  import SSH, SSHComputingElement 
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.Utilities.List                           import breakListIntoChunks
from DIRAC                                               import S_OK, S_ERROR
from DIRAC                                               import systemCall, rootPath
from DIRAC                                               import gConfig
from DIRAC.Core.Security.ProxyInfo                       import getProxyInfo
from DIRAC.Resources.Computing.SSHComputingElement       import SSH 

import os, sys, time, re, socket, stat, shutil
import string, shutil, bz2, base64, tempfile

CE_NAME = 'SSHGE'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHGEComputingElement( SSHComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'sgece'
    self.mandatoryParameters = MANDATORY_PARAMETERS
    
  def _getJobOutputFiles( self, jobID ):
    """ Get output file names for the specific CE 
    """
    result = pfnparse( jobID )
    if not result['OK']:
      return result
    jobStamp = result['Value']['FileName']
    host = result['Value']['Host']

    output = '%s/DIRACPilot.o%s' % ( self.batchOutput, jobStamp )
    error = '%s/DIRACPilot.e%s' % ( self.batchError, jobStamp )
  
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
