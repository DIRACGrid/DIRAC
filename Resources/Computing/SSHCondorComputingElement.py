########################################################################
# $HeadURL$
# File : CondorComputingElement.py
# Author : A.Tsaregorodtsev
########################################################################

import os, stat, tempfile, shutil

from DIRAC.Resources.Computing.SSHComputingElement  import SSH, SSHComputingElement 
from DIRAC.Resources.Computing.PilotBundle          import bundleProxy 
from DIRAC import rootPath, S_OK, S_ERROR

CE_NAME = 'SSHCondor'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHCondorComputingElement( SSHComputingElement ):
       
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'condorce'
    self.mandatoryParameters = MANDATORY_PARAMETERS         
       