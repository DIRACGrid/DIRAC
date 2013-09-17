########################################################################
# $HeadURL$
# File : SSHOSGComputingElement.py
# Author : S.Poss
########################################################################

from DIRAC.Resources.Computing.SSHComputingElement  import SSH, SSHComputingElement 
from DIRAC.Resources.Computing.PilotBundle          import bundleProxy 
from DIRAC import rootPath, S_OK, S_ERROR

CE_NAME = 'SSHOSG'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHOSGComputingElement( SSHComputingElement ):
       
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'condorgce'
    self.mandatoryParameters = MANDATORY_PARAMETERS         
    