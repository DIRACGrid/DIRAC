########################################################################
# $HeadURL$
# File : CondorComputingElement.py
# Author : A.Tsaregorodtsev
########################################################################

"""
The condor CE with submission via SSH
"""

from DIRAC.Resources.Computing.SSHComputingElement  import SSHComputingElement 

__RCSID__ = "$Id$"

CE_NAME = 'SSHCondor'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHCondorComputingElement( SSHComputingElement ):
  """ The Condor CE via SSH
  """
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'condorce'
    self.mandatoryParameters = MANDATORY_PARAMETERS         
