########################################################################
# $HeadURL$
# File : CondorComputingElement.py
# Author : A.Tsaregorodtsev
########################################################################


from DIRAC.Resources.Computing.SSHComputingElement  import SSHComputingElement 

__RCSID__ = "$Id$"

CE_NAME = 'SSHCondor'
MANDATORY_PARAMETERS = [ 'Queue' ]

class SSHCondorComputingElement( SSHComputingElement ):
  """ To use Condor CEs through SSH
  """
  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    SSHComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'condorce'
    self.mandatoryParameters = MANDATORY_PARAMETERS         
