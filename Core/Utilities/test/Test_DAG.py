""".. module:: Test_DAG

Test cases for DIRAC.Core.Utilities.DAG module.

"""
import unittest

# sut
from  DIRAC.Core.Utilities.DAG import DAG

__RCSID__ = "$Id $"


########################################################################
class DAGTestCase( unittest.TestCase ):
  """ Test case for DIRAC.Core.Utilities.DAG module
	"""

  def test_full(self):
    """ test dag creation
    """
    dag = DAG()
    i_n = dag.getIndexNodes()
    self.assertEqual(i_n, [])
    dag.addNode('A')
    self.assertEqual(dag.graph, {'A': set()})
    dag.addNode('A')
    self.assertEqual(dag.graph, {'A': set()})
    dag.addNode('B')
    self.assertEqual(dag.graph, {'A': set(), 'B': set()})
    dag.addEdge('A', 'B')
    self.assertEqual(dag.graph, {'A': {'B'}, 'B': set()})
    dag.addEdge('A', 'B')
    self.assertEqual(dag.graph, {'A': {'B'}, 'B': set()})
    dag.addEdge('A', 'C')
    self.assertEqual(dag.graph, {'A': {'B'}, 'B': set()})
    dag.addNode('C')
    dag.addEdge('A', 'C')
    self.assertEqual(dag.graph, {'A': {'B', 'C'}, 'B': set(), 'C': set()})
    dag.addEdge('C', 'A') #this would be cyclic, so it should not change the graph
    self.assertEqual(dag.graph, {'A': {'B', 'C'}, 'B': set(), 'C': set()})
    dag.addNode('D')
    i_n = dag.getIndexNodes()
    self.assertEqual(i_n, ['A', 'D'])
    dag.addNode('E')
    i_n = dag.getIndexNodes()
    self.assertEqual(sorted(i_n), sorted(['A', 'D', 'E']))
    dag.addEdge('A', 'D')
    dag.addEdge('D', 'E')
    self.assertEqual(dag.graph, {'A': {'B', 'C', 'D'}, 'B': set(), 'C': set(), 'D': {'E'}, 'E': set()} )
    i_n = dag.getIndexNodes()
    self.assertEqual(i_n, ['A'])
    dag.addEdge('E', 'A')
    self.assertEqual(dag.graph, {'A': {'B', 'C', 'D'}, 'B': set(), 'C': set(), 'D': {'E'}, 'E': {'A'}} )

    class forTest(object):
      pass
    ft = forTest()
    dag.addNode(ft)
    self.assertEqual(dag.graph, {'A': {'B', 'C', 'D'}, 'B': set(), 'C': set(), 'D': {'E'}, 'E': {'A'}, ft: set()} )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( DAGTestCase )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
