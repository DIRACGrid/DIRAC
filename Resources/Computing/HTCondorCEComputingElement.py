"""
Module to submit jobs to HTCondorCE

Author : A.Sailer

"""
__RCSID__ = "$Id$"

from DIRAC.Resources.Computing.LocalComputingElement  import LocalComputingElement

CE_NAME = 'HTCondorCE'
MANDATORY_PARAMETERS = [ 'Queue' ]

class HTCondorCEComputingElement( LocalComputingElement ):
  """Class to submit jobs to HTCondorCE"""
  #############################################################################
  def __init__( self, ceUniqueID ):

    LocalComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'htcondorce'
    self.mandatoryParameters = MANDATORY_PARAMETERS
