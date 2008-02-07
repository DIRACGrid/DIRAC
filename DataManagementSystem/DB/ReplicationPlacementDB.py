"""
    DIRAC ReplicationPlacementDB class is a front-end to the transformation metadata and associated files.
"""

from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Transformation.TransformationDB import TransformationDB

class ReplicationPlacementDB(TransformationDB):

  def __init__( self, maxQueueSize=4 ):
    """ Constructor
    """
    TransformationDB.__init__(self,'ReplicationPlacementDB', 'DataManagement/ReplicationPlacementDB', maxQueueSize)
