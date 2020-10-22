########################################################################
# File: Graph.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/27 07:22:15
########################################################################
"""
:mod: Graph

.. module: Graph

:synopsis: graph

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

graph
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"
import six
# #
# @file Graph.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/27 07:22:23
# @brief Definition of Graph class.
# pylint: disable=no-member


class DynamicProps(type):
  """

  metaclass allowing to create properties on the fly
  """
  def __new__(cls, name, bases, classdict):
    """
    new operator
    """

    def makeProperty(self, name, value, readOnly=False):
      """
      Add property :name: to class

      This also creates a private :_name: attribute
      If you want to make read only property, set :readOnly: flag to True
      :warn: could raise AttributeError if :name: of :_name: is already
      defined as an attribute
      """
      if hasattr(self, "_" + name) or hasattr(self, name):
        raise AttributeError("_%s or %s is already defined as a member" % (name, name))

      def fget(self):
        return self._getProperty(name)

      fset = None if readOnly else lambda self, value: self._setProperty(name, value)
      setattr(self, '_' + name, value)
      setattr(self.__class__, name, property(fget=fget, fset=fset))

    def _setProperty(self, name, value):
      """
      property setter
      """
      setattr(self, '_' + name, value)

    def _getProperty(self, name):
      """
      property getter
      """
      return getattr(self, '_' + name)

    classdict["makeProperty"] = makeProperty
    classdict["_setProperty"] = _setProperty
    classdict["_getProperty"] = _getProperty
    return type.__new__(cls, name, bases, classdict)


@six.add_metaclass(DynamicProps)
class Node(object):
  """
  graph node
  """
  def __init__(self, name, rwAttrs=None, roAttrs=None):
    """
    c'tor

    :param str name: node name
    :param dict rwAttrs: read/write properties dict
    :param dict roAttrs: read-only properties dict
    """
    self.makeProperty("name", name, True)
    self.makeProperty("visited", False)
    self.__edges = list()
    rwAttrs = rwAttrs if isinstance(rwAttrs, dict) else {}
    for attr, value in rwAttrs.items():
      self.makeProperty(attr, value, False)
    roAttrs = roAttrs if isinstance(roAttrs, dict) else {}
    for attr, value in roAttrs.items():
      self.makeProperty(attr, value, True)

  def __contains__(self, edge):
    """
    in operator for edges
    """
    if not isinstance(edge, Edge):
      raise TypeError("edge should be an instance or subclass of Edge")
    return edge in self.__edges

  def __iter__(self):
    """
    edges iterator
    """
    return self.__edges.__iter__()

  def edges(self):
    """
    get edges
    """
    return self.__edges

  def addEdge(self, edge):
    """
    Add edge to the node
    """
    if not isinstance(edge, Edge):
      raise TypeError("supplied edge argument should be an Edge instance or subclass")
    if edge not in self:
      self.__edges.append(edge)

  def connect(self, other, rwAttrs=None, roAttrs=None):
    """
    Connect self to Node :other: with edge attibutes rw :rwAttrs: and
    ro :roAttrs:
    """
    if not isinstance(other, Node):
      raise TypeError("argument other should be a Node instance!")
    edge = Edge(self, other, rwAttrs, roAttrs)
    if edge not in self:
      self.__edges.append(edge)
    return edge


@six.add_metaclass(DynamicProps)
class Edge(object):
  """
  Directed link between two nodes
  """
  def __init__(self, fromNode, toNode, rwAttrs=None, roAttrs=None):
    """
    c'tor

    :param Node fromNode: edge start
    :param Node toNode: edge end
    :param dict rwAttrs: read/write properties dict
    :param dict roAttrs: read only properties dict
    """
    if not isinstance(fromNode, Node):
      raise TypeError("supplied argument fromNode should be a Node instance")
    if not isinstance(toNode, Node):
      raise TypeError("supplied argument toNode should be a Node instance")
    self.makeProperty("fromNode", fromNode, True)
    self.makeProperty("toNode", toNode, True)
    self.makeProperty("name", "%s-%s" % (self.fromNode.name, self.toNode.name), True)
    self.makeProperty("visited", False)
    rwAttrs = rwAttrs if isinstance(rwAttrs, dict) else {}
    for attr, value in rwAttrs.items():
      self.makeProperty(attr, value, False)
    roAttrs = roAttrs if isinstance(roAttrs, dict) else {}
    for attr, value in roAttrs.items():
      self.makeProperty(attr, value, True)
    if self not in self.fromNode:
      self.fromNode.addEdge(self)

  def __str__(self):
    """
    str representation of an object
    """
    return self.name

  def __repr__(self):
    """
    repr operator for dot format
    """
    return "'%s' -> '%s';" % (self.fromNode.name, self.toNode.name)

########################################################################


@six.add_metaclass(DynamicProps)
class Graph(object):
  """
  A generic directed graph with attributes attached to its nodes and edges
  """
  # # flag to indicate PREORDER traversal
  __PREORDER = False
  # # flag to indicate POSTORDER traversal
  __POSTORDER = False

  def __init__(self, name, nodes=None, edges=None):
    """
    c'tor

    :param self: self reference
    :param str name: graph name
    :param list nodes: initial node list
    :param list edges: initial edge list
    """
    self.makeProperty("name", name, True)
    nodes = nodes if nodes else list()
    edges = edges if edges else list()
    self.__nodes = []
    self.__edges = []
    for edge in edges:
      if edge not in self:
        self.addEdge(edge)
    for node in nodes:
      if node not in self:
        self.addNode(node)

  def __contains__(self, obj):
    """
    In operator for edges and nodes
    """
    return bool(obj in self.__nodes or obj in self.__edges)

  def nodes(self):
    """
    Get nodes dict
    """
    return self.__nodes

  def getNode(self, nodeName):
    """
    Get node :nodeName:
    """
    for node in self.__nodes:
      if node.name == nodeName:
        return node

  def edges(self):
    """
    Get edges dict
    """
    return self.__edges

  def getEdge(self, edgeName):
    """
    Get edge :edgeName:
    """
    for edge in self.__edges:
      if edge.name == edgeName:
        return edge

  @property
  def PREORDER(self):
    """
    PREORDER getter
    """
    return self.__PREORDER

  @PREORDER.setter
  def PREORDER(self, flag=True):
    """
    PREORDER setter
    """
    self.__PREORDER = bool(flag)
    self.__POSTORDER = not self.__PREORDER

  @property
  def POSTORDER(self):
    """
    POSTORDER getter
    """
    return self.__POSTORDER

  @POSTORDER.setter
  def POSTORDER(self, flag=True):
    """
    POSTORDER setter
    """
    self.__POSTORDER = bool(flag)
    self.__PREORDER = not self.__POSTORDER

  def connect(self, fromNode, toNode, rwAttrs=None, roAttrs=None):
    """
    Connect :fromNode: to :toNode: with edge of attributes
    """
    edge = fromNode.connect(toNode, rwAttrs, roAttrs)
    self.addEdge(edge)
    self.addNode(fromNode)
    self.addNode(toNode)
    return edge

  def addNode(self, node):
    """
    Add Node :node: to graph
    """
    if not isinstance(node, Node):
      raise TypeError("Supplied argument should be a Node instance")
    if node not in self:
      self.__nodes.append(node)
      if not hasattr(node, "graph"):
        node.makeProperty("graph", self)
      else:
        node.graph = self
    for edge in node:
      if edge not in self:
        self.addEdge(edge)
        if edge.toNode not in self:
          self.addNode(edge.toNode)

  def addEdge(self, edge):
    """
    Add edge :edge: to the graph
    """
    if not isinstance(edge, Edge):
      raise TypeError("Supplied edge argument should be an Edge instance")
    if edge.fromNode not in self:
      self.addNode(edge.fromNode)
    if edge.toNode not in self:
      self.addNode(edge.toNode)
    if edge not in self:
      self.__edges.append(edge)
    if not hasattr(edge, "graph"):
      edge.makeProperty("graph", self)
    else:
      edge.graph = self

  def reset(self):
    """
    Set visited for all nodes to False
    """
    for node in self.__nodes:
      node.visited = False
    for edge in self.__edges:
      edge.visited = False

  def walkAll(self, nodeFcn=None, edgeFcn=None, res=None):
    """
    Wall all nodes excuting :nodeFcn: on each node and :edgeFcn: on each
    edge result is a dict { Node.name : result from :nodeFcn:,
    Edge.name : result from edgeFcn }
    """
    if not any((self.PREORDER, self.POSTORDER)):
      self.PREORDER = True
    res = res if res else {}
    self.reset()
    for node in self.nodes():
      if not node.visited:
        res.update(self.walkNode(node, nodeFcn, edgeFcn, res))
    return res

  def walkNode(self, node, nodeFcn=None, edgeFcn=None, res=None):
    """
    Walk through the graph calling nodeFcn on nodes and edgeFcn on edges
    """
    res = res if res else {}
    # # already visited, return
    if node.visited:
      return res

    if self.PREORDER:
      node.visited = True
      if callable(nodeFcn):
        res.update({node.name: nodeFcn(node)})
      for edge in node:
        # # execute edge fcn
        if callable(edgeFcn):
          res[edge.name] = edgeFcn(edge)
        # # mark edge visited
        edge.visited = True
        res.update(self.walkNode(edge.toNode, nodeFcn, edgeFcn, res))
      return res

    if self.POSTORDER:
      node.visited = True
      for edge in node:
        # # execute edge fcn
        if callable(edgeFcn):
          res[edge.name] = edgeFcn(edge)
        # # mark edge visited
        edge.visited = True
        res.update(self.walkNode(edge.toNode, nodeFcn, edgeFcn, res))
      if callable(nodeFcn):
        res.update({node.name: nodeFcn(node)})
      return res

  def __repr__(self):
    """
    Repr operator creating dot string
    """
    out = ["digraph '%s' {" % self.name]
    for node in self.nodes():
      out.append("%s;" % node.name)
    for edge in self.edges():
      out.append(repr(edge))
    out.append("}")
    return "\n".join(out)

  def explore(self, node, preVisit=None, postVisit=None):
    """
    Explore node
    """
    node.visited = True
    if callable(preVisit):
      preVisit(node)
    for edge in node.edges():
      if not edge.toNode.visited:
        self.explore(edge.toNode, preVisit, postVisit)
    if callable(postVisit):
      postVisit(node)

  def dfs(self, preVisit=None, postVisit=None):
    """
    dfs recursive walk
    """
    self.reset()
    nodes = list(self.nodes())
    nodes.sort(key=lambda node: len(node.edges()), reverse=True)
    for node in nodes:
      if not node.visited:
        self.explore(node, preVisit, postVisit)

  def bfs(self, preVisit=None, postVisit=None):
    """
    bfs walk
    """
    self.reset()
    nodes = list(self.nodes())
    nodes.sort(key=lambda node: len(node.edges()), reverse=True)
    queue = [nodes[0]]
    while queue:
      node = queue.pop(0)
      if callable(preVisit):
        preVisit(node)
      node.visited = True
      for edge in node.edges():
        if not edge.toNode.visited:
          queue.append(edge.toNode)
      if callable(postVisit):
        postVisit(node)
    return nodes

  def dfsIter(self, preVisit=None, postVisit=None):
    """
    Iterative dfs - no recursion
    """
    nodes = list(self.nodes())
    nodes.sort(key=lambda node: len(node.edges()), reverse=True)
    for node in nodes:
      if not hasattr(node, "explored"):
        node.makeProperty("explored", False, False)
      if not hasattr(node, "discovered "):
        node.makeProperty("discovered", False, False)
    stack = []
    for node in nodes:
      if not node.discovered:
        node.discovered = True
        stack.append(node)
      while stack:
        node = stack[-1]
        if callable(preVisit):
          preVisit(node)
        for edge in node.edges():
          if not edge.toNode.discovered:
            edge.toNode.discovered = True
            stack.append(edge.toNode)
        if not node.explored:
          node.explored = True
        if callable(postVisit):
          postVisit(node)
          stack.pop()
