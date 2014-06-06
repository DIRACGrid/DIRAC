########################################################################
# $HeadURL$
# File : SSHOSGComputingElement.py
# Author : S.Poss
########################################################################
""" For OSG, it's required to go through condor_g (grid universe), via SSH
"""

from DIRAC.Resources.Computing.SSHComputingElement  import SSHComputingElement 

__RCSID__ = "$Id$"

CE_NAME = 'SSHOSG'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHOSGComputingElement( SSHComputingElement ):
  """ The condor_g CE, via SSH
  """
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'condorgce'
    self.mandatoryParameters = MANDATORY_PARAMETERS         
    