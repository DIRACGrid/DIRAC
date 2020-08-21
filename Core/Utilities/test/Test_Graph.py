########################################################################
# $HeadURL $
# File: GraphTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/28 09:02:23
########################################################################
""" :mod: GraphTests
    =======================

    .. module: GraphTests
    :synopsis: tests for Graph module classes
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"
# #
# @file GraphTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/28 09:02:24
# @brief Definition of GraphTests class.
# # imports
import unittest
import six
# # SUT
from DIRAC.Core.Utilities.Graph import Node, Edge, Graph, DynamicProps  # , topologicalSort, topoSort

class DynamicPropTests( unittest.TestCase ):
  """
  ..  class:: DynamicPropTests
  """
  def testDynamicProps( self ):
    """ test dynamic props """
    @six.add_metaclass(DynamicProps)
    class TestClass( object ):
      """
      .. class:: TestClass

      dummy class
      """

    # # dummy instance
    testObj = TestClass()
    # # makeProperty in
    self.assertEqual( hasattr( testObj, "makeProperty" ), True )
    self.assertEqual( callable( getattr( testObj, "makeProperty" ) ), True )
    # # .. and works  for rw properties
    testObj.makeProperty( "rwTestProp", 10 ) #pylint: disable=no-member
    self.assertEqual( hasattr( testObj, "rwTestProp" ), True )
    self.assertEqual( getattr( testObj, "rwTestProp" ), 10 )
    testObj.rwTestProp += 1 #pylint: disable=no-member
    self.assertEqual( getattr( testObj, "rwTestProp" ), 11 )
    # # .. and ro as well
    testObj.makeProperty( "roTestProp", "I'm read only", True ) #pylint: disable=no-member
    self.assertEqual( hasattr( testObj, "roTestProp" ), True )
    self.assertEqual( getattr( testObj, "roTestProp" ), "I'm read only" )
    # # AttributeError for read only property setattr
    try:
      testObj.roTestProp = 11
    except AttributeError as error:
      self.assertEqual( str( error ), "can't set attribute" )

class NodeTests( unittest.TestCase ):
  """
  .. class:: NodeTests
  """
  def setUp( self ):
    """ test setup """
    self.roAttrs = { "ro1" : True, "ro2" : "I'm read only" }
    self.rwAttrs = { "rw1" : 0, "rw2" : ( 1, 2, 3 ) }
    self.name = "BrightStart"
    self.node = Node( self.name, self.rwAttrs, self.roAttrs )

  def tearDown( self ):
    """ clean up """
    del self.roAttrs
    del self.rwAttrs
    del self.name
    del self.node

  def testNode( self ):
    """ node rwAttrs roAttrs connect """

    # # node name - th eon,y one prop you can't overwrite
    self.assertEqual( self.node.name, self.name )
    try:
      self.node.name = "can't do this"
    except AttributeError as error:
      self.assertEqual( str( error ), "can't set attribute" )
    try:
      self.node.makeProperty( "name", "impossible" )
    except AttributeError as error:
      self.assertEqual( str( error ), "_name or name is already defined as a member" )

    # # visited attr for walking
    self.assertEqual( hasattr( self.node, "visited" ), True )
    self.assertEqual( self.node.visited, False )  #pylint: disable=no-member

    # # ro attrs
    for k, v in self.roAttrs.items():
      self.assertEqual( hasattr( self.node, k ), True )
      self.assertEqual( getattr( self.node, k ), v )
      try:
        setattr( self.node, k, "new value" )
      except AttributeError as error:
        self.assertEqual( str( error ), "can't set attribute" )

    # # rw attrs
    for k, v in self.rwAttrs.items():
      self.assertEqual( hasattr( self.node, k ), True )
      self.assertEqual( getattr( self.node, k ), v )
      setattr( self.node, k, "new value" )
      self.assertEqual( getattr( self.node, k ), "new value" )

    # # connect
    toNode = Node( "DeadEnd" )
    edge = self.node.connect( toNode, { "foo" : "boo" }, { "ro3" : True } )
    self.assertEqual( isinstance( edge, Edge ), True )
    self.assertEqual( edge.name, self.name + "-DeadEnd" )
    self.assertEqual( self.node, edge.fromNode ) #pylint: disable=no-member
    self.assertEqual( toNode, edge.toNode ) #pylint: disable=no-member

class EdgeTests( unittest.TestCase ):
  """
  .. class:: EdgeTests
  """
  def setUp( self ):
    """ test setup """
    self.fromNode = Node( "Start" )
    self.toNode = Node( "End" )
    self.roAttrs = { "ro1" : True, "ro2" : "I'm read only" }
    self.rwAttrs = { "rw1" : 0, "rw2" : ( 1, 2, 3 ) }

  def tearDown( self ):
    """ clean up """
    del self.fromNode
    del self.toNode
    del self.roAttrs
    del self.rwAttrs

  def testEdge( self ):
    """ c'tor connect attrs """
    edge = Edge( self.fromNode, self.toNode, self.rwAttrs, self.roAttrs )

    # # name
    self.assertEqual( edge.name, "%s-%s" % ( self.fromNode.name, self.toNode.name ) )
    try:
      edge.name = "can't do this"
    except AttributeError as error:
      self.assertEqual( str( error ), "can't set attribute" )
    try:
      edge.makeProperty( "name", "impossible" )
    except AttributeError as error:
      self.assertEqual( str( error ), "_name or name is already defined as a member" )

    # # visited attr
    self.assertEqual( hasattr( edge, "visited" ), True )
    self.assertEqual( edge.visited, False ) #pylint: disable=no-member

    # # ro attrs
    for k, v in self.roAttrs.items():
      self.assertEqual( hasattr( edge, k ), True )
      self.assertEqual( getattr( edge, k ), v )
      try:
        setattr( edge, k, "new value" )
      except AttributeError as error:
        self.assertEqual( str( error ), "can't set attribute" )

    # # rw attrs
    for k, v in self.rwAttrs.items():
      self.assertEqual( hasattr( edge, k ), True )
      self.assertEqual( getattr( edge, k ), v )
      setattr( edge, k, "new value" )
      self.assertEqual( getattr( edge, k ), "new value" )

    # # start and end
    self.assertEqual( edge.fromNode, self.fromNode ) #pylint: disable=no-member
    self.assertEqual( edge.toNode, self.toNode ) #pylint: disable=no-member
    # # in fromNode, not in toNode
    self.assertEqual( edge in self.fromNode, True )
    self.assertEqual( edge not in self.toNode, True )


clock = 0
########################################################################
class GraphTests( unittest.TestCase ):
  """
  .. class:: GraphTests
  """
  def setUp( self ):
    """ setup test case """
    self.nodes = [ Node( "1" ), Node( "2" ), Node( "3" ) ]
    self.edges = [ self.nodes[0].connect( self.nodes[1] ),
                   self.nodes[0].connect( self.nodes[2] ) ]
    self.aloneNode = Node( "4" )

  def tearDown( self ):
    """ clean up """
    del self.nodes
    del self.edges
    del self.aloneNode

  def testGraph( self ):
    """ ctor nodes edges connect walk """

    # # create graph
    gr = Graph( "testGraph", self.nodes, self.edges )

    # # nodes and edges
    for node in self.nodes:
      self.assertEqual(node in gr, True)
    for edge in self.edges:
      self.assertEqual(edge in gr, True)
    self.assertEqual(sorted(self.nodes, key=lambda x: x.name), sorted(gr.nodes(), key=lambda x: x.name))
    self.assertEqual(sorted(self.edges, key=lambda x: x.name), sorted(gr.edges(), key=lambda x: x.name))

    # # getNode
    for node in self.nodes:
      self.assertEqual( gr.getNode( node.name ), node )

    # # connect
    aloneEdge = gr.connect( self.nodes[0], self.aloneNode )
    self.assertEqual( self.aloneNode in gr, True )
    self.assertEqual( aloneEdge in gr, True )

    # # addNode
    anotherNode = Node( "5" )
    anotherEdge = anotherNode.connect( self.aloneNode )
    gr.addNode( anotherNode )
    self.assertEqual( anotherNode in gr, True )
    self.assertEqual( anotherEdge in gr, True )


    # # walk no nodeFcn
    ret = gr.walkAll()
    self.assertEqual( ret, {} )

    for node in gr.nodes():
      self.assertEqual( node.visited, True )

    gr.reset()
    for node in gr.nodes():
      self.assertEqual( node.visited, False )
    # # walk with nodeFcn
    def nbEdges( node ):
      """ dummy node fcn """
      return len( node.edges() )
    ret = gr.walkAll( nodeFcn = nbEdges )
    self.assertEqual( ret, { '1': 3, '2' : 0, '3': 0, '4' : 0, '5': 1 } )


  def testDFS( self ):
    """ dfs """

    global clock

    def topoA( graph ):
      """ topological sort """
      global clock
      nodes = graph.nodes()
      for node in nodes:
        node.makeProperty( "clockA", 0 )
      def postVisit( node ):
        global clock
        node.clockA = clock
        clock += 1
      graph.dfs( postVisit = postVisit )
      nodes = graph.nodes()
      nodes.sort( key = lambda node: node.clockA )
      return nodes

    def topoB( graph ):
      """ topological sort """
      global clock
      nodes = graph.nodes()
      for node in nodes:
        node.makeProperty( "clockB", 0 )

      def postVisit( node ):
        global clock
        node.clockB = clock
        clock += 1
      graph.dfsIter( postVisit = postVisit )
      nodes = graph.nodes()
      nodes.sort( key = lambda node: node.clockB )
      return nodes

    clock = 0
    gr = Graph( "testGraph", self.nodes, self.edges )
    gr.addNode( self.aloneNode )
    nodesSorted = topoA( gr )
    nodes = gr.nodes()
    nodes.sort( key = lambda node: node.clockA, reverse = True )
    self.assertEqual( nodes, nodesSorted, "topoA sort failed" )

    clock = 0
    gr = Graph( "testGraph", self.nodes, self.edges )
    gr.addNode( self.aloneNode )
    gr.reset()
    nodesSorted = topoB( gr )
    nodes = gr.nodes()
    nodes.sort( key = lambda node: node.clockB, reverse = True )
    self.assertEqual( nodes, nodesSorted, "topoB sort failed" )

  def testBFS( self ):
    """ bfs walk """

    global clock
    def walk( graph ):
      """ bfs walk  """
      global clock
      nodes = graph.nodes()
      for node in nodes:
        node.makeProperty( "clockC", 0 )

      def postVisit( node ):
        global clock
        node.clockC = clock
        clock += 1
      nodes = graph.bfs( postVisit = postVisit )
      nodes.sort( key = lambda node: node.clockC )
      return nodes

    clock = 0
    gr = Graph( "testGraph", self.nodes, self.edges )
    gr.addNode( self.aloneNode )
    gr.reset()
    nodesSorted = walk( gr )
    nodes = gr.nodes()
    nodes.sort( key = lambda node: node.clockC )
    self.assertEqual( nodesSorted, nodes, "bfs failed" )




# # test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  tests = ( testLoader.loadTestsFromTestCase( testCase ) for testCase in ( DynamicPropTests,
                                                                           NodeTests,
                                                                           EdgeTests,
                                                                           GraphTests ) )
  testSuite = unittest.TestSuite( tests )
  unittest.TextTestRunner( verbosity = 3 ).run( testSuite )




