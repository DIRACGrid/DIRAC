########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/WorkloadManagementSystem/Client/IgnoreMissingInputData.py $
# File :    IgnoreMissingInputData.py
# Author :  Ricardo Graciani
########################################################################
""" Remove from the list any InputData file that has not yet been handled
"""

__RCSID__ = "$Id: DownloadInputData.py 36511 2011-03-24 06:37:07Z rgracian $"

from DIRAC                                                          import S_OK, gLogger


COMPONENT_NAME = 'IgnoreMissingInputData'

class IgnoreMissingInputData:
  """ Remove from the list any InputData file that has not yet been handled
  """

  #############################################################################
  def __init__( self, argumentsDict ):
    """ Standard constructor
    """
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )

  #############################################################################
  def execute( self, dataToResolve = None ):
    """Simply return a empty structure
    """
    self.log.verbose( 'Skipping missing Input Data' )
    if dataToResolve:
      self.log.debug( '\n'.join( dataToResolve ) )
    result = S_OK()
    result['Successful'] = {}
    result['Failed'] = {}
    return result
