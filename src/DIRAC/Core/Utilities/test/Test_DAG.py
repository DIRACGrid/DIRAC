""".. module:: Test_DAG

Test cases for DIRAC.Core.Utilities.DAG module.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import unittest

# sut
from  DIRAC.Core.Utilities.DAG import DAG, makeFrozenSet

__RCSID__ = "$Id $"


def listToSet(obj, recursive=False):
  """ Convert a list of objects into a set which can be used for comparison """
  result = set()
  for x in obj:
    if isinstance(x, (list, tuple)):
      x = frozenset(x) if recursive else tuple(x)
    elif isinstance(x, dict):
      x = tuple(map(tuple, sorted(x.items())))
    result.add(x)
  return result


########################################################################
class DAGTestCase( unittest.TestCase ):
  """ Test case for DIRAC.Core.Utilities.DAG module
  """
  pass

class DAGSimple(DAGTestCase):

  def test_makeFrozenSet(self):
    """ test makeFrozenSet
    """
    res = makeFrozenSet({'a':'b'})
    self.assertEqual(res, frozenset({('a','b')}))

    # dict with lists in
    dList1 = {'a':[]}
    res = makeFrozenSet(dList1)
    self.assertEqual(res, frozenset({('a',frozenset([]))}))

    dList2 = {'a':[0, 1]}
    res = makeFrozenSet(dList2)
    self.assertEqual(res, frozenset({('a',frozenset([0, 1]))}))

    dList3 = {'a':[0, 1], 'b':0}
    res = makeFrozenSet(dList3)
    self.assertEqual( res, frozenset( { ('a',frozenset([0, 1])), ('b', 0) } ) )

    # dict with sets in
    dSet1 = {'a':set()}
    res = makeFrozenSet(dSet1)
    self.assertEqual( res, frozenset( { ('a', frozenset([])) } ) )


    #dict with dicts in
    dDict1 = {'a': {'a':'b'}}
    res = makeFrozenSet(dDict1)
    self.assertEqual( res, frozenset( { ('a', frozenset( {('a', 'b')} ) ) } ) )


    # #dicts with sets, list, and dicts in
    dAll = {'a': {'a':'b'}, 'c':[0,1], 'd':set()}
    res = makeFrozenSet(dAll)
    self.assertEqual( res, frozenset( { ('a', frozenset( {('a', 'b')} ) ), ('c', frozenset([0, 1])), ('d', frozenset([])) } ) )


class DAGFull(DAGTestCase):

  def test_getList(self):
    """ test dag to list
    """
    dag = DAG()
    dag.addNode('A')
    l = dag.getList()
    self.assertEqual(l, ['A'])

    dag.addNode('C')
    dag.addEdge('A', 'C')
    l = dag.getList()
    self.assertEqual(l, ['A', 'C'])

    dag.addNode('B')
    dag.addEdge('C', 'B')
    l = dag.getList()
    self.assertEqual(l, ['A', 'C', 'B'])

    d = dict(zip('ab', range(2)))
    dag.addNode(d)
    dag.addEdge('B', d)
    l = dag.getList()
    self.assertEqual(l, ['A', 'C', 'B', d])

    l1 = list(range(2))
    dag.addNode(l1)
    dag.addEdge(d, l1)
    l = dag.getList()
    self.assertEqual(l, ['A', 'C', 'B', d, l1])

    dag.addNode('E')
    dag.addEdge(l1, 'E')
    l = dag.getList()
    self.assertEqual(l, ['A', 'C', 'B', d, l1, 'E'])

    dag1 = DAG()
    dag1.addNode(d)
    dag1.addNode(l1)
    dag1.addEdge(d, l1)
    l = dag1.getList()
    self.assertEqual(l, [d, l1])


  def test_full(self):
    """ test dag creation and more
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
    l = dag.getList()
    self.assertEqual(l, [])
    dag.addEdge('A', 'B')
    self.assertEqual(dag.graph, {'A': {'B'}, 'B': set()})
    l = dag.getList()
    self.assertEqual(set(l), {'A', 'B'})
    dag.addEdge('A', 'B')
    self.assertEqual(dag.graph, {'A': {'B'}, 'B': set()})
    dag.addEdge('A', 'C')
    self.assertEqual(dag.graph, {'A': {'B'}, 'B': set()})
    dag.addNode('C')
    dag.addEdge('A', 'C')
    self.assertEqual(dag.graph, {'A': {'B', 'C'}, 'B': set(), 'C': set()})
    l = dag.getList()
    self.assertEqual(l, ['A'])
    dag.addEdge('C', 'A') #this would be cyclic, so it should not change the graph
    self.assertEqual(dag.graph, {'A': {'B', 'C'}, 'B': set(), 'C': set()})
    dag.addNode('D')
    i_n = dag.getIndexNodes()
    self.assertEqual(set(i_n), {'A', 'D'})
    dag.addNode('E')
    i_n = dag.getIndexNodes()
    self.assertEqual(listToSet(i_n), listToSet(['A', 'D', 'E']))
    dag.addEdge('A', 'D')
    dag.addEdge('D', 'E')
    self.assertEqual(dag.graph, {'A': {'B', 'C', 'D'}, 'B': set(), 'C': set(), 'D': {'E'}, 'E': set()} )
    i_n = dag.getIndexNodes()
    self.assertEqual(i_n, ['A'])
    dag.addEdge('E', 'A')
    self.assertEqual(dag.graph, {'A': {'B', 'C', 'D'}, 'B': set(), 'C': set(), 'D': {'E'}, 'E': {'A'}} )

    #now an object
    class forTest(object):
      pass
    ft = forTest()
    dag.addNode(ft)
    self.assertEqual(dag.graph, {'A': {'B', 'C', 'D'}, 'B': set(), 'C': set(), 'D': {'E'}, 'E': {'A'}, ft: set()} )
    dag.addEdge('B', ft)
    self.assertEqual( dag.graph,
                      {'A': {'B', 'C', 'D'}, 'B': {ft}, 'C': set(), 'D': {'E'}, 'E': {'A'}, ft: set()}
                    )

    #now sets, dicts and lists as nodes
    d = dict(zip('ab', range(2)))
    dag.addNode(d)
    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C', 'D'},
                        'B': {ft},
                        'C': set(),
                        'D': {'E'},
                        'E': {'A'},
                        ft: set(),
                        frozenset({('a',0), ('b',1)}): set()})
    dag.addEdge(ft, d)
    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C', 'D'},
                        'B': {ft},
                        'C': set(),
                        'D': {'E'},
                        'E': {'A'},
                        ft: set([frozenset({('a',0), ('b',1)})]),
                        frozenset({('a',0), ('b',1)}): set()
                      }
                    )

    l = list(range(2))
    dag.addNode(l)
    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C', 'D'},
                        'B': {ft},
                        'C': set(),
                        'D': {'E'},
                        'E': {'A'},
                        ft: set([frozenset({('a',0), ('b',1)})]), #ft -> d
                        frozenset({('a',0), ('b',1)}): set(), #d
                        frozenset({0,1}): set() #l
                      }
                    )
    dag.addEdge(d, l)
    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C', 'D'},
                        'B': {ft},
                        'C': set(),
                        'D': {'E'},
                        'E': {'A'},
                        ft: set([frozenset({('a',0), ('b',1)})]), #ft -> d
                        frozenset({('a',0), ('b',1)}): set([frozenset({0,1})]), #d->l
                        frozenset({0,1}): set() #l
                      }
                    )

    del dag.graph['E']
    del dag.graph['D']
    del dag.graph[ft]
    del dag.graph[frozenset({('a',0), ('b',1)})]
    dag.graph['A'] = {'B', 'C'}

    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C'},
                        'B': {ft},
                        'C': set(),
                        frozenset({0,1}): set(), #l
                      }
                    )

    i_n = dag.getIndexNodes()
    self.assertEqual(listToSet(i_n), listToSet(['A', l]))

    d1 = {'a':'b'}
    dag.addNode(d1)
    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C'},
                        'B': {ft},
                        'C': set(),
                        frozenset({0,1}): set(), #l
                        frozenset({('a','b')}): set(), #d1
                      }
                    )

    l1 = ['a', 'b']
    dag.addNode(l1)
    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C'},
                        'B': {ft},
                        'C': set(),
                        frozenset({0,1}): set(), #l
                        frozenset({('a', 'b')}): set(), #d1
                        frozenset({'a', 'b'}): set() #l1
                      }
                    )

    i_n = dag.getIndexNodes()
    self.assertEqual(
        listToSet(i_n, recursive=True),
        listToSet(['A', l, d1, l1], recursive=True)
    )

    s1 = set()
    dag.addNode(s1)
    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C'},
                        'B': {ft},
                        'C': set(),
                        frozenset({0,1}): set(), #l
                        frozenset({('a', 'b')}): set(), #d1
                        frozenset({'a', 'b'}): set(), #l1
                        frozenset({}): set() # s1
                      }
                    )

    #dict with frozenset in
    dFSet1 = {'a':frozenset()}
    dag.addNode(dFSet1)
    self.assertEqual( dag.graph,
                      { 'A': {'B', 'C'},
                        'B': {ft},
                        'C': set(),
                        frozenset({0,1}): set(), #l
                        frozenset({('a', 'b')}): set(), #d1
                        frozenset({'a', 'b'}): set(), #l1
                        frozenset({}): set(), # s1
                        frozenset([('a', frozenset([]))]): set() #dFSet1
                      }
                    )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( DAGTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( DAGSimple ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( DAGFull ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
