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
__RCSID__ = "$Id$"
##
# @file GraphTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/28 09:02:24
# @brief Definition of GraphTests class.
## imports 
import unittest
## SUT
from DIRAC.Core.Utilities.Graph import Node, Edge, Graph, DynamicProps

class DynamicPropTests( unittest.TestCase ):
  """
  ..  class:: DynamicPropTests
  """  
  def testDynamicProps( self ):
    """ test dynamic props """
    class TestClass( object ):
      """ 
      .. class:: TestClass
      
      dummy class
      """
      __metaclass__ = DynamicProps 
    ## dummy instance
    testObj = TestClass()
    ## makeProperty in
    self.assertEqual( hasattr( testObj, "makeProperty" ), True )
    self.assertEqual( callable( getattr( testObj, "makeProperty" ) ), True )
    ## .. and works  for rw properties
    testObj.makeProperty( "rwTestProp", 10 )
    self.assertEqual( hasattr(testObj, "rwTestProp"), True )
    self.assertEqual( getattr(testObj, "rwTestProp"), 10 )
    testObj.rwTestProp += 1
    self.assertEqual( getattr(testObj, "rwTestProp"), 11 )
    ## .. and ro as well
    testObj.makeProperty( "roTestProp", "I'm read only", True )
    self.assertEqual( hasattr(testObj, "roTestProp"), True )
    self.assertEqual( getattr(testObj, "roTestProp"), "I'm read only" )
    ## AttributeError for read only property setattr 
    try:
      testObj.roTestProp = 11
    except AttributeError, error:
      self.assertEqual( str(error), "can't set attribute" )

class NodeTests( unittest.TestCase ):
  """
  .. class:: NodeTests
  """
  def setUp( self ):
    """ test setup """
    self.roAttrs = { "ro1" : True, "ro2" : "I'm read only" }
    self.rwAttrs = { "rw1" : 0, "rw2" : (1,2,3) }
    self.name = "BrightStart"    
    self.node = Node( self.name, self.rwAttrs, self.roAttrs )
    
  def tearDown(self):
    """ clean up """
    del self.roAttrs
    del self.rwAttrs
    del self.name
    del self.node

  def testNode( self ):
    """ node rwAttrs roAttrs connect """
    
    ## node name - th eon,y one prop you can't overwrite 
    self.assertEqual( self.node.name, self.name )
    try:
      self.node.name = "can't do this"
    except AttributeError, error:
      self.assertEqual( str(error), "can't set attribute" )
    try:
      self.node.makeProperty( "name", "impossible" )
    except AttributeError, error:
      self.assertEqual( str(error), "_name or name is already defined as a member" )

    ## visited attr for walking
    self.assertEqual( hasattr( self.node, "visited" ), True )
    self.assertEqual( self.node.visited, False )
 
    ## ro attrs
    for k, v in self.roAttrs.items():
      self.assertEqual( hasattr( self.node, k ), True )
      self.assertEqual( getattr( self.node, k ), v )
      try:
        setattr( self.node, k, "new value" )
      except AttributeError, error:
        self.assertEqual( str(error), "can't set attribute" )

    ## rw attrs
    for k, v in self.rwAttrs.items():
      self.assertEqual( hasattr( self.node, k ), True )
      self.assertEqual( getattr( self.node, k ), v )
      setattr( self.node, k, "new value" )
      self.assertEqual( getattr( self.node, k ), "new value" )

    ## connect 
    toNode = Node( "DeadEnd" )
    edge = self.node.connect( toNode, { "foo" : "boo" }, { "ro3" : True } )
    self.assertEqual( isinstance(edge, Edge), True )
    self.assertEqual( edge.name, self.name + "-DeadEnd" )
    self.assertEqual( self.node, edge.fromNode )
    self.assertEqual( toNode, edge.toNode )
    
class EdgeTests( unittest.TestCase ):
  """
  .. class:: EdgeTests
  """
  def setUp( self ):
    """ test setup """
    self.fromNode = Node( "Start" )
    self.toNode = Node( "End" )
    self.roAttrs = { "ro1" : True, "ro2" : "I'm read only" }
    self.rwAttrs = { "rw1" : 0, "rw2" : (1,2,3) }
    
  def tearDown(self):
    """ clean up """
    del self.fromNode
    del self.toNode
    del self.roAttrs
    del self.rwAttrs

  def testEdge( self ):
    """ c'tor connect attrs """
    edge = Edge( self.fromNode, self.toNode, self.rwAttrs, self.roAttrs )

    ## name 
    self.assertEqual( edge.name, "%s-%s" % ( self.fromNode.name, self.toNode.name ) )
    try:
      edge.name = "can't do this"
    except AttributeError, error:
      self.assertEqual( str(error), "can't set attribute" )
    try:
      edge.makeProperty( "name", "impossible" )
    except AttributeError, error:
      self.assertEqual( str(error), "_name or name is already defined as a member" )

    ## visited attr
    self.assertEqual( hasattr( edge, "visited" ), True )
    self.assertEqual( edge.visited, False )

    ## ro attrs
    for k, v in self.roAttrs.items():
      self.assertEqual( hasattr( edge, k ), True )
      self.assertEqual( getattr( edge, k ), v )
      try:
        setattr( edge, k, "new value" )
      except AttributeError, error:
        self.assertEqual( str(error), "can't set attribute" )

    ## rw attrs
    for k, v in self.rwAttrs.items():
      self.assertEqual( hasattr( edge, k ), True )
      self.assertEqual( getattr( edge, k ), v )
      setattr( edge, k, "new value" )
      self.assertEqual( getattr( edge, k ), "new value" )
 
    ## start and end
    self.assertEqual( edge.fromNode, self.fromNode )
    self.assertEqual( edge.toNode, self.toNode )
    ## in fromNode, not in toNode
    self.assertEqual( edge in self.fromNode, True )
    self.assertEqual( edge not in self.toNode, True )

########################################################################
class GraphTests(unittest.TestCase):
  """
  .. class:: GraphTests
  """
  def setUp( self ):
    """ setup test case """
    self.nodes = [ Node("1"), Node("2"), Node("3") ]
    self.edges = [ self.nodes[0].connect( self.nodes[1] ),
                   self.nodes[0].connect( self.nodes[2] ) ]
    self.aloneNode = Node("4")
    
  def tearDown( self ):
    """ clean up """
    del self.nodes
    del self.edges
    del self.aloneNode

  def testGraph(self):
    """ ctor nodes edges connect walk """

    ## create graph
    gr = Graph( "testGraph", self.nodes, self.edges )
  
    ## nodes and edges 
    for node in self.nodes:
      self.assertEqual( node in gr, True )
    for edge in self.edges:
      self.assertEqual( edge in gr, True )
    self.assertEqual( sorted(self.nodes), sorted( gr.nodes() ) )
    self.assertEqual( sorted(self.edges), sorted( gr.edges() ) )

    ## getNode
    for node in self.nodes:
      self.assertEqual( gr.getNode(node.name), node )

    ## connect
    aloneEdge = gr.connect( self.nodes[0], self.aloneNode )
    self.assertEqual( self.aloneNode in gr, True  )
    self.assertEqual( aloneEdge in gr, True  )

    ## addNode
    anotherNode = Node("5")
    anotherEdge = anotherNode.connect( self.aloneNode )
    gr.addNode( anotherNode )
    self.assertEqual( anotherNode in gr, True )
    self.assertEqual( anotherEdge in gr, True )

    ## walking

    ## walk no nodeFcn
    ret = gr.walkAll()
    self.assertEqual( ret, {} )
    for node in gr.nodes():
      self.assertEqual( node.visited, True )
    gr.reset()
    for node in gr.nodes():
      self.assertEqual( node.visited, False )
    ## walk with nodeFcn
    def nbEdges( node ):
      """ dummy node fcn """
      return len( node.edges() ) 
    ret = gr.walkAll( nodeFcn=nbEdges )
    self.assertEqual( ret, { '1': 3, '2' : 0, '3': 0, '4' : 0, '5': 1 } )

## test execution
if __name__ == "__main__":
  testLoader = unittest.TestLoader()
  tests = ( testLoader.loadTestsFromTestCase( testCase ) for testCase in ( DynamicPropTests, 
                                                                           NodeTests,
                                                                           EdgeTests,
                                                                           GraphTests ) )
  testSuite = unittest.TestSuite( tests )
  unittest.TextTestRunner(verbosity=3).run( testSuite )




