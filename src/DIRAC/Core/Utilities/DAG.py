""" Soft implementation of a Direct Acyclic Graph (DAG)

    Nota Bene: It is NOT fully checked if valid (i.e. some cycle can be introduced)!
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
from DIRAC import gLogger

__RCSID__ = "$Id $"


class DAG(object):
  """ a Direct Acyclic Graph (DAG)

      Represented as a dictionary whose keys are nodes, and values are sets holiding their dependencies
  """

  def __init__(self):
    """ Defines graph variable holding the dag representation
    """
    self.graph = {}

  def addNode(self, node):
    """ add a node to graph

        Args:
          node (object): Any type of object - if not hashable, it will be converted to a frozenset
    """
    node = checkNode(node)
    if node not in self.graph:
      self.graph[node] = set()

  def addEdge(self, fromNode, toNode):
    """ add an edge (checks if both nodes exist)

        Args:
          fromNode (object)
          toNode (object)
    """
    fromNode = checkNode(fromNode)
    toNode = checkNode(toNode)

    if fromNode not in self.graph:
      gLogger.error("Missing node from where the edge start")
      return
    if toNode not in self.graph:
      gLogger.error("Missing node to where the edge lands")
      return

    for node, toNodes in self.graph.items():
      # This is clearly not enough to assure that it's really acyclic...
      if toNode == node and fromNode in toNodes:
        gLogger.error("Can't insert this edge")
        return
    self.graph[fromNode].add(toNode)

  def getIndexNodes(self):
    """ Return a list of index nodes
    """
    notIndexNodes = set()
    for depNodes in self.graph.values():
      [notIndexNodes.add(depNode) for depNode in depNodes]
    indexNodes = list(set(self.graph.keys()) - notIndexNodes)
    return [unHashNode(inu) for inu in indexNodes]

  def getList(self):
    """ Returns a list out of the DAG, if possible
    """
    cDAG = copy.deepcopy(self)

    l = []
    while True:
      try:
        indexNodes = cDAG.getIndexNodes()
        if len(indexNodes) != 1:
          gLogger.warn("The DAG is not sequential")
          break
        ind = checkNode(indexNodes[0])
        del cDAG.graph[ind]
        l.append(unHashNode(ind))
      except KeyError:
        break
      except IndexError:
        break

    return l


def checkNode(node):
  """ Returns a hashable version of node
  """
  try:
    node.__hash__()
    return node
  except TypeError:  # nodeName is not hashable, so it can't be a key in the graph (which is a dictionary)
    return makeFrozenSet(node)


def unHashNode(node):
  """ Returns a dict or list, if node is frozenset
  """
  if isinstance(node, frozenset):
    try:
      return dict(node)  # Is it a dictionary?
    except TypeError:
      return list(node)
    except ValueError:
      return list(node)
  else:
    return node


def makeFrozenSet(ob):
  """
  Makes a hash from a dictionary, list, tuple or set to any level, that contains
  only other hashable types (including any lists, tuples, sets, and dictionaries).
  """
  if isinstance(ob, (set, tuple, list)):
    return frozenset([makeFrozenSet(e) for e in ob])

  elif not isinstance(ob, dict):
    return ob

  new_ob = copy.deepcopy(ob)
  for obk, obv in new_ob.items():
    new_ob[obk] = makeFrozenSet(obv)

  return frozenset(sorted(new_ob.items()))
