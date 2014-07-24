########################################################################
# $HeadURL$
# File : CondorComputingElement.py
# Author : R.Graciani
########################################################################

import os, stat, tempfile, shutil

from DIRAC.Resources.Computing.LocalComputingElement  import LocalComputingElement

CE_NAME = 'Condor'
MANDATORY_PARAMETERS = [ 'Queue' ]

class CondorComputingElement( LocalComputingElement ):

  #############################################################################
  def __init__( self, ceUniqueID ):
    """ Standard constructor.
    """
    LocalComputingElement.__init__( self, ceUniqueID )

    self.ceType = CE_NAME
    self.controlScript = 'condorce'
    self.mandatoryParameters = MANDATORY_PARAMETERS
