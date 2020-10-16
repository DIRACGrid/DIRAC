""" This is the Regression test for DIRAC.MonitoringSystem.private.Plotters in order to ensure
    the proper working of the plotter.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import unittest

import math
import operator
from PIL import Image

from DIRAC.MonitoringSystem.private.Plotters.ComponentMonitoringPlotter import ComponentMonitoringPlotter
from functools import reduce
from six.moves import map


def compare(file1Path, file2Path):
  """
  Function used to compare two plots.

  :type file1Path: string
  :param file1Path: Path to the file1.
  :type file2Path: string
  :param file2Path: Path to the file2.

  :return: float value rms.
  """
  # Crops image to remove the "Generated on xxxx UTC" string
  image1 = Image.open(file1Path).crop((0, 0, 800, 570))
  image2 = Image.open(file2Path).crop((0, 0, 800, 570))
  h1 = image1.histogram()
  h2 = image2.histogram()
  rms = math.sqrt(reduce(operator.add,
                         list(map(lambda a, b: (a - b) ** 2, h1, h2))) / len(h1))
  return rms


class PlotterTestCase(unittest.TestCase):

  def setUp(self):
    pass

  def tearDown(self):
    pass


class ComponentMonitoringPlotterUnitTest(PlotterTestCase):
  # First you will need to generate plots on the DIRAC WebApp and then add it to the
  # DIRAC/tests/Integration/Monitoring/png/ folder.
  # Then change the data dictionary here according to the WebApp plot and then run this regression test.

  def test_plotRunningThreads(self):
    """ test for the method "_plotRunningThreads"
    """

    plotName = "ComponentMonitoringPlotter_plotRunningThreads"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 9.0,
                                                   1562857200: 0,
                                                   1562854500: 0,
                                                   1562855400: 0,
                                                   1562856300: 0,
                                                   1562858100: 0,
                                                   1562849100: 9.75,
                                                   1562860800: 0,
                                                   1562850000: 10.0,
                                                   1562848200: 9.142857142857142,
                                                   1562859900: 0,
                                                   1562850900: 9.428571428571429,
                                                   1562851800: 9.0,
                                                   1562847300: 8.5,
                                                   1562852700: 9.0}
                         },
                'unit': 'threads',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotRunningThreads(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotCpuUsage(self):
    """ test for the method "_plotCpuUsage"
    """

    plotName = "ComponentMonitoringPlotter_plotCpuUsage"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562857200: 0,
                                                   1562854500: 0,
                                                   1562855400: 0,
                                                   1562856300: 0,
                                                   1562858100: 0,
                                                   1562849100: 2.5875000450760126,
                                                   1562850000: 0.9428571707436016,
                                                   1562848200: 2.171428546309471,
                                                   1562859900: 0,
                                                   1562850900: 0.9428571473274913,
                                                   1562851800: 0.10000000149011612,
                                                   1562847300: 4.950000047683716,
                                                   1562852700: 0.033333333830038704}
                         },
                'unit': 'percentage',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotCpuUsage(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotMemoryUsage(self):
    """ test for the method "_plotMemoryUsage"
    """

    plotName = "ComponentMonitoringPlotter_plotMemoryUsage"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 93.03776041666667,
                                                   1562857200: 0,
                                                   1562854500: 0,
                                                   1562855400: 0,
                                                   1562856300: 0,
                                                   1562858100: 0,
                                                   1562849100: 249.76318359375,
                                                   1562860800: 0,
                                                   1562850000: 165.26674107142858,
                                                   1562848200: 260.68917410714283,
                                                   1562859900: 0,
                                                   1562850900: 180.12890625,
                                                   1562851800: 186.35546875,
                                                   1562847300: 121.13671875,
                                                   1562852700: 186.35807291666666}
                         },
                'unit': 'MB',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotMemoryUsage(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotRunningTime(self):
    """ test for the method "_plotRunningTime"
    """

    plotName = "ComponentMonitoringPlotter_plotRunningTime"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 313.32080459594727,
                                                   1562857200: 0,
                                                   1562854500: 0,
                                                   1562855400: 0,
                                                   1562856300: 0,
                                                   1562858100: 0,
                                                   1562849100: 1754.9678344726562,
                                                   1562860800: 0,
                                                   1562850000: 492.68797302246094,
                                                   1562848200: 854.48386492048,
                                                   1562859900: 0,
                                                   1562850900: 1332.7680140904017,
                                                   1562851800: 1812.8760986328125,
                                                   1562847300: 314.9701385498047,
                                                   1562852700: 3452.56787109375}
                         },
                'unit': 'seconds',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotRunningTime(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotConnections(self):
    """ test for the method "_plotConnections"
    """

    plotName = "ComponentMonitoringPlotter_plotConnections"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 0,
                                                   1562857200: 1.0,
                                                   1562854500: 1.0,
                                                   1562855400: 1.0,
                                                   1562856300: 1.0,
                                                   1562858100: 1.0,
                                                   1562849100: 0,
                                                   1562860800: 1.0,
                                                   1562850000: 0,
                                                   1562848200: 0,
                                                   1562859900: 0,
                                                   1562850900: 0,
                                                   1562851800: 0,
                                                   1562847300: 0,
                                                   1562852700: 0}
                         },
                'unit': 'Connections',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotConnections(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotActiveQueries(self):
    """ test for the method "_plotActiveQueries"
    """

    plotName = "ComponentMonitoringPlotter_plotActiveQueries"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 0,
                                                   1562857200: 0.06666666666666667,
                                                   1562854500: 0,
                                                   1562855400: 0.2,
                                                   1562856300: 0,
                                                   1562858100: 0,
                                                   1562849100: 0,
                                                   1562860800: 0,
                                                   1562850000: 0,
                                                   1562848200: 0,
                                                   1562859900: 0,
                                                   1562850900: 0,
                                                   1562851800: 0,
                                                   1562847300: 0,
                                                   1562852700: 0}
                         },
                'unit': 'ActiveQueries',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotActiveQueries(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotPendingQueries(self):
    """ test for the method "_plotPendingQueries"
    """

    plotName = "ComponentMonitoringPlotter_plotPendingQueries"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 0,
                                                   1562857200: 0,
                                                   1562854500: 0,
                                                   1562855400: 0,
                                                   1562856300: 0,
                                                   1562858100: 0,
                                                   1562849100: 0,
                                                   1562860800: 0,
                                                   1562850000: 0,
                                                   1562848200: 0,
                                                   1562859900: 0,
                                                   1562850900: 0,
                                                   1562851800: 0,
                                                   1562847300: 0,
                                                   1562852700: 0}
                         },
                'unit': 'PendingQueries',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotPendingQueries(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotMaxFD(self):
    """ test for the method "_plotMaxFD"
    """

    plotName = "ComponentMonitoringPlotter_plotMaxFD"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 0,
                                                   1562857200: 1.0,
                                                   1562861700: 2.5,
                                                   1562854500: 4.4,
                                                   1562855400: 2.3333333333333335,
                                                   1562856300: 2.076923076923077,
                                                   1562858100: 1.3333333333333333,
                                                   1562849100: 0,
                                                   1562860800: 3.4615384615384617,
                                                   1562850000: 0,
                                                   1562848200: 0,
                                                   1562859900: 0,
                                                   1562850900: 0,
                                                   1562851800: 0,
                                                   1562847300: 0,
                                                   1562852700: 0}
                         },
                'unit': 'MaxFD',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotMaxFD(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotActivityRunningThreads(self):
    """ test for the method "_plotActivityRunningThreads"
    """

    plotName = "ComponentMonitoringPlotter_plotActivityRunningThreads"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562796000,
                     'endTime': 1562882400,
                     'condDict': {'component': 'Monitoring_Monitoring'}
                     }

    plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 0,
                                                   1562857200: 6.866666666666666,
                                                   1562861700: 6.0,
                                                   1562854500: 5.6,
                                                   1562855400: 6.2,
                                                   1562856300: 6.538461538461538,
                                                   1562858100: 6.333333333333333,
                                                   1562849100: 0,
                                                   1562860800: 6.153846153846154,
                                                   1562850000: 0,
                                                   1562848200: 0,
                                                   1562859900: 6.2,
                                                   1562850900: 0,
                                                   1562851800: 0,
                                                   1562847300: 0,
                                                   1562852700: 0}
                         },
                'unit': 'RunningThreads',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotActivityRunningThreads(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, 'DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)


if __name__ == "__main__":
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PlotterTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ComponentMonitoringPlotterUnitTest))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
