########################################################################
# $HeadURL $
# File: Graph.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/27 07:22:15
########################################################################

""" :mod: Graph 
    =======================
 
    .. module: Graph
    :synopsis: graph 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    graph 
"""

__RCSID__ = "$Id $"

##
# @file Graph.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/27 07:22:23
# @brief Definition of Graph class.

class DynamicProps( type ):
  """
  .. class:: DynamicProps

  """
  def __new__( mcs, name, bases, classdict ):
    """ new operator """

    def makeProperty( self, name, value, readOnly=False ):
      """ add property :name: to class 
      
      this also creates a private :_name: attribute
      if you want to make read only property, set :readOnly: flag to True
      :warn: could raise AttributeError if :name: of :_name: is already defined as an attribute  
      """
      if hasattr( self, "_"+name) or hasattr( self, name ):
       raise AttributeError( "_%s or %s is already defined as a member" % (name, name) )    
      fget = lambda self: self._getProperty( name )
      fset = None if readOnly else lambda self, value: self._setProperty( name, value )
      setattr( self, '_' + name, value )         
      setattr( self.__class__, name, property( fget = fget, fset = fset ) ) 
    
    def _setProperty( self, name, value ):
      """ property setter """
      setattr( self, '_' + name, value )    

    def _getProperty( self, name ):
      """ propery getter """
      return getattr( self, '_' + name )
    
    classdict["makeProperty"] = makeProperty
    classdict["_setProperty"] = _setProperty
    classdict["_getProperty"] = _getProperty
    return type.__new__( mcs, name, bases, classdict )

class Node( object ):
  """ 
  .. class:: Node
  
  graph node
  """
  __metaclass__ = DynamicProps

  def __init__( self, name, rwAttrs=None, roAttrs=None ):
    """ c'tor """
    self.makeProperty( "name", name, True ) 
    self.makeProperty( "visited", False )
    self.__edges = list()
    rwAttrs = rwAttrs if type(rwAttrs) == dict else {}
    for attr, value in rwAttrs.items():
      self.makeProperty( attr, value, False )
    roAttrs = roAttrs if type(roAttrs) == dict else {}
    for attr, value in roAttrs.items():
      self.makeProperty( attr, value, True  )

  def __contains__( self, edge ):
    """ in operator for edges """
    if not isinstance( edge, Edge ):
      raise TypeError( "edge should be an instance or subclass of Edge" )
    return edge in self.__edges  

  def __iter__( self ):
    """ edges iterator """
    return self.__edges.__iter__()

  def edges( self ):
    return self.__edges

  def addEdge( self, edge ):
    """ add edge to the node """
    if not isinstance( edge, Edge ):
      raise TypeError( "supplied edge argument should be an Edge instance or subclass" )
    if edge not in self:
      self.__edges.append( edge )
  
  def connect( self, other, rwAttrs=None, roAttrs=None ):
    """ connect self to Node :other: with edge attibutes rw :rwAttrs: and ro :roAttrs:"""
    if not isinstance( other, Node ):
      raise TypeError( "argument other should be a Node instance!" )
    edge = Edge( self, other, rwAttrs, roAttrs )
    if edge not in self:
      self.__edges.append( edge )
    return edge 

class Edge( object ):
  """
  .. class:: Edge

  directed link between two nodes
  """
  __metaclass__ = DynamicProps

  def __init__( self, fromNode, toNode, rwAttrs=None, roAttrs=None ):
    """ c'tor """
    if not isinstance( fromNode, Node ):
      raise TypeError("supplied argument fromNode should be a Node instance" )
    if not isinstance( toNode, Node ):
      raise TypeError("supplied argument toNode should be a Node instance" )
    self.makeProperty( "fromNode", fromNode, True )
    self.makeProperty( "toNode", toNode, True )    
    self.makeProperty( "name", "%s-%s" % ( self.fromNode.name, self.toNode.name ), True )
    self.makeProperty( "visited", False )
    rwAttrs = rwAttrs if type(rwAttrs) == dict else {}
    for attr, value in rwAttrs.items():
      self.makeProperty( attr, value, False )
    roAttrs = roAttrs if type(roAttrs) == dict else {}
    for attr, value in roAttrs.items():
      self.makeProperty( attr, value, True )

    if self not in self.fromNode:
      self.fromNode.addEdge( self )

  def __str__( self ):
    """ str representation of an object """
    return self.name 

########################################################################
class Graph(object):
  """
  .. class:: Graph
  
  a generic graph with attributes attached to its nodes and edges
  """
  __metaclass__ = DynamicProps 

  def __init__( self, name, nodes=None, edges=None ):
    """c'tor

    :param self: self reference
    """
    self.makeProperty( "name", name, True )
    nodes = nodes if nodes else list()
    edges = edges if edges else list()
    self.__nodes = []
    self.__edges = []
    for edge in edges:
      if edge not in self:
        self.addEdge( edge )
    for node in nodes:
      if node not in self:
        self.addNode( node ) 
     
  def __contains__( self, obj ):
    """ in operator for edges and nodes """
    return bool( obj in self.__nodes or obj in self.__edges ) 
    
  def nodes( self ):
    """ get nodes dict """
    return self.__nodes

  def getNode(self, nodeName ):
    """ get node :nodeName: """    
    for node in self.__nodes:
      if node.name == nodeName:
        return node

  def edges( self ):
    """ get edges dict """
    return self.__edges

  def getEdge(self, edgeName):
    """ get edge :edgeName: """
    for edge in self.__edges:
      if egde.name == edgeName:
        return edge

  def connect( self, fromNode, toNode, rwAttrs=None, roAttrs=None ):
    """ connect :fromNode: to :toNode: with edge of attributes """
    edge = fromNode.connect( toNode, rwAttrs, roAttrs )
    self.addEdge( edge )
    self.addNode( fromNode )
    self.addNode( toNode )

  def subNode( self, node ):
    """ TODO: remove node and edges that points from (to) it """
    if node in self:
      pass
    
  def addNode( self, node ):
    """ add Node :node: to graph """
    if not isinstance( node, Node ):
      raise TypeError( "supplied argument should be a Node instance" )
    if node not in self:
      self.__nodes.append( node )
      node.makeProperty( "graph", self, True )
    for edge in node:
      if edge not in self:
        self.addEdge( edge )
        if edge.toNode not in self:
          self.addNode( edge.toNode )
        
  def addEdge( self, edge ):
    """ add edge to the graph """
    if not isinstance( edge, Edge ):
      raise TypeError( "supplied edge argument should be an Edge instance" )
    if edge.fromNode not in self: 
      self.addNode( edge.fromNode )
    if edge.toNode not in self:
      self.addNode( edge.toNode )
    self.__edges.append( edge )
    edge.makeProperty( "graph", self, True )
      
  def reset( self ):
    """ set visited for all nodes to False """
    for node in self.__nodes:
      node.visited = False 
    for edge in self.__edges:
      edge.visited = False

  def walkAll( self, nodeFcn=None, edgeFcn=None, res=None ):
    res = res if res else {}
    self.reset()
    for node in self.nodes():
      if not node.visited:
        res.update( self.walkNode( node, nodeFcn, edgeFcn, res ) )
    return res     
  
  def walkNode( self, node, nodeFcn=None, edgeFcn=None, res=None ):
    """ walk through the graph calling nodeFcn on nodes and edgeFcn on edges """
    res = res if res else {}
    ## already visited, return 
    if node.visited:
      return res
    ## mark node visited
    node.visited = True 
    ## execute node fcn 
    if callable(nodeFcn):
      res.update( { node.name : nodeFcn( node ) } )
    for edge in node:
      ## execute edge fcn
      if callable(edgeFcn):
        res[edge.name] = edgeFcn( edge )
      ## mark edge visited
      edge.visited = True 
      res.update( self.walkNode( edge.toNode, nodeFcn, edgeFcn, res ) )  
    return res

if __name__ == "__main__":
  
  class A( object ):
    __metaclass__ = DynamicProps 
    
  a = A()  
  a.makeProperty( "myname", "a" )
  print a._myname

  print a.myname
  a.myname = 10
  print a.myname
  a.makeProperty( "ro", 10, True )
  print a.ro
  try:
    a.ro = 14
  except AttributeError, err:
    print err
    
  b = Node( "node1", { "foo" :1, "bar": False } ) 
  print b.foo
  print b.bar
  b.foo = 2
  print b.foo
  print b.bar
  c = Node( "node2", { "foo" : -1 } )
  d = Node( "node3" )

  b.connect( c )
  c.connect( d )
  d.connect( b )
  
  g = Graph( "sample" )
  g.addNode(b)

  g.addNode( Node( "alone") )

  print g.nodes()
  print g.edges()

  g.walkNode( b )

  def getName( node ):
    return node.name
  g.reset()
  ret = g.walkAll( getName, getName )
  print ret 

  g.reset()
  def getFoo( node ):
    return node.foo if hasattr( node, "foo" ) else None 
  
  ret = g.walkAll( getFoo, getFoo )
  print ret 
