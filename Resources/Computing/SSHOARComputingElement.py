########################################################################
# $HeadURL$
# File : OARComputingElement.py
# Author : M. Sapunov
########################################################################
""" OAR CE interface, via SSH
"""

from DIRAC.Resources.Computing.SSHComputingElement  import SSHComputingElement

__RCSID__ = "$Id$"

CE_NAME = 'SSHOAR'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHOARComputingElement( SSHComputingElement ):
  """ For OAR submission via SSH
  """
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'oarce'
    self.mandatoryParameters = MANDATORY_PARAMETERS
