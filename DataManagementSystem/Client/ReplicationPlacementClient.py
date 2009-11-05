""" Client class to access the replication placement service
"""
########################################################################
# $Id$
########################################################################

from DIRAC.Core.Transformation.TransformationDBClient import TransformationDBClient

class ReplicationPlacementClient(TransformationDBClient):

  def __init__(self):
    self.server = 'DataManagement/PlacementDB'
