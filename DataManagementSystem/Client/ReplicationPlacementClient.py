""" Client class to access the replication placement service
"""
########################################################################
# $Id: ReplicationPlacementClient.py,v 1.1 2009/09/18 13:59:14 acsmith Exp $
########################################################################

from DIRAC.Core.Transformation.TransformationDBClient import TransformationDBClient

class ReplicationPlacementClient(TransformationDBClient):

  def __init__(self):
    self.server = 'DataManagement/PlacementDB'
