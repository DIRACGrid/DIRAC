""" Soft implementation of a Direct Acyclic Graph (DAG)

    Nota Bene: It is NOT fully checked if valid (i.e. some cycle can be introduced)!
"""

from DIRAC import gLogger

__RCSID__ = "$Id $"

class DAG( object ):
  """ a Direct Acyclic Graph (DAG)

      Represented as a dictionary whose keys are nodes, and values are sets holiding their dependencies
  """

  def __init__( self ):
    """ Defines graph variable holding the dag representation
    """
    self.graph = {}

  def addNode( self, nodeName ):
    """ add a node to graph
    """
    if nodeName not in self.graph:
      self.graph[nodeName] = set()

  def addEdge( self, fromNode, toNode ):
    """ add an edge (checks if both nodes exist)
    """
    if fromNode not in self.graph:
      gLogger.error( "Missing node from where the edge start" )
      return
    if toNode not in self.graph:
      gLogger.error( "Missing node to where the edge lands" )
      return

    for node, toNodes in self.graph.iteritems():
      #This is clearly not enough to assure that it's really acyclic...
      if toNode == node and fromNode in toNodes:
	gLogger.error( "Can't insert this edge" )
	return
    self.graph[fromNode].add( toNode )

  def getIndexNodes( self ):
    """ Return a list of index nodes
    """
    notIndexNodes = set()
    for depNodes in self.graph.itervalues():
      [notIndexNodes.add(depNode) for depNode in depNodes]
    return list(set(self.graph.keys()) - notIndexNodes)
