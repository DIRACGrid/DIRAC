""" Class that contains client access to the StorageManagerDB handler. """
########################################################################
# $Id: StorageManagerClient.py .py 22438 2010-03-08 08:33:14Z acsmith $
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/TransformationSystem/Client/TransformationDBClient.py $
########################################################################
__RCSID__ = "$Id: StorageManagerClient.py 22438 2010-03-08 08:33:14Z acsmith $"

from DIRAC                                          import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Client                         import Client
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.Core.Utilities.List                      import breakListIntoChunks
import types

class StorageManagerClient(Client):

  def __init__(self):
    self.setServer('StorageManagement/StorageManagerHandler')
