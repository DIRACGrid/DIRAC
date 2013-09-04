########################################################################
# $HeadURL$
# File : OARComputingElement.py
# Author : M. Sapunov
########################################################################

import os, stat, tempfile, shutil

from DIRAC.Resources.Computing.SSHComputingElement  import SSH, SSHComputingElement
from DIRAC.Resources.Computing.PilotBundle          import bundleProxy
from DIRAC import rootPath, S_OK, S_ERROR

CE_NAME = 'SSHOAR'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHOARComputingElement( SSHComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'oarce'
    self.mandatoryParameters = MANDATORY_PARAMETERS