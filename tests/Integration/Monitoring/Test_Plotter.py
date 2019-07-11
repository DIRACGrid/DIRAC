""" This is the Regression test for DIRAC.MonitoringSystem.private.Plotters in order to ensure
    the proper working of the plotter.
"""

from __future__ import absolute_import
import unittest

import sys
import math
import operator
from PIL import Image

from DIRAC.MonitoringSystem.private.Plotters.ComponentMonitoringPlotter import ComponentMonitoringPlotter
from functools import reduce
from six.moves import map


def compare(file1Path, file2Path):
  '''
    Function used to compare two plots

    returns 0.0 if both are identical
  '''
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
  """ First you will need to generate plots on the DIRAC WebApp and then add it to the DIRAC/tests/Integration/Monitoring/png/ folder.
      Then change the data dictionary here according to the WebApp plot and then run this regression test.
  """

  def test_plotRunningThreads(self):
    """ test for the method "_plotRunningThreads"
    """

    plotName = "ComponentMonitoringPlotter_plotRunningThreads"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562623200,
                     'endTime': 1562709600,
                     'condDict': {'component': 'Configuration_Server'}
                     }

    plotInfo = {'data': {'Configuration_Server': {1562659200: 6.0,
                                                  1562673600: 6.0,
                                                  1562660100: 6.0,
                                                  1562661000: 6.0,
                                                  1562651100: 6.0,
                                                  1562676300: 6.25,
                                                  1562675400: 6.142857142857143,
                                                  1562677200: 6.0,
                                                  1562671800: 6.2,
                                                  1562649300: 6.0,
                                                  1562650200: 6.0,
                                                  1562674500: 6.0,
                                                  1562658300: 6.0,
                                                  1562672700: 6.125}
                         },
                'unit': 'threads',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotRunningThreads(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotCpuUsage(self):
    """ test for the method "_plotCpuUsage"
    """

    plotName = "ComponentMonitoringPlotter_plotCpuUsage"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562623200,
                     'endTime': 1562709600,
                     'condDict': {'component': 'Configuration_Server'}
                     }

    plotInfo = {'data': {'Configuration_Server': {1562659200: 0.271428573344435,
                                                  1562673600: 0.17142857504742487,
                                                  1562660100: 0.17500000353902578,
                                                  1562661000: 0.10000000149011612,
                                                  1562651100: 0.22500000335276127,
                                                  1562676300: 0.32500001043081284,
                                                  1562675400: 0.3285714387893677,
                                                  1562677200: 0.30000001192092896,
                                                  1562671800: 0.10000000149011612,
                                                  1562649300: 0.7333333430190881,
                                                  1562650200: 0.12857143048729217,
                                                  1562674500: 0.1875000037252903,
                                                  1562658300: 0.15000000596046448,
                                                  1562672700: 0.12500000186264515}
                         },
                'unit': 'percentage',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotCpuUsage(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotMemoryUsage(self):
    """ test for the method "_plotMemoryUsage"
    """

    plotName = "ComponentMonitoringPlotter_plotMemoryUsage"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562623200,
                     'endTime': 1562709600,
                     'condDict': {'component': 'Configuration_Server'}
                     }

    plotInfo = {'data': {'Configuration_Server': {1562659200: 26.622767857142858,
                                                  1562673600: 27.118861607142858,
                                                  1562660100: 28.90380859375,
                                                  1562661000: 28.94921875,
                                                  1562651100: 30.0810546875,
                                                  1562676300: 27.07666015625,
                                                  1562675400: 27.028459821428573,
                                                  1562677200: 27.128125,
                                                  1562671800: 28.96640625,
                                                  1562649300: 31.048177083333332,
                                                  1562650200: 31.618303571428573,
                                                  1562674500: 26.9345703125,
                                                  1562658300: 29.453125,
                                                  1562672700: 28.9833984375}
                         },
                'unit': 'MB',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotMemoryUsage(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotRunningTime(self):
    """ test for the method "_plotRunningTime"
    """

    plotName = "ComponentMonitoringPlotter_plotRunningTime"
    reportRequest = {'grouping': 'component',
                     'groupingFields': ('%s', ['component']),
                     'startTime': 1562709600,
                     'endTime': 1562796000,
                     'condDict': {'component': 'Configuration_Server'}
                     }

    plotInfo = {'data': {'Configuration_Server': {1562738400: 78936.08482142857,
                                                  1562745600: 1393.6852798461914,
                                                  1562739300: 79775.76897321429,
                                                  1562747400: 1420.906736101423,
                                                  1562740200: 80676.3212890625,
                                                  1562737500: 77748.236328125,
                                                  1562765400: 17522.12667410714,
                                                  1562741100: 16424.6253112793,
                                                  1562760900: 13118.525716145834,
                                                  1562742000: 914.428466796875,
                                                  1562736600: 77267.6640625,
                                                  1562764500: 16622.072998046875,
                                                  1562762700: 14802.015380859375,
                                                  1562742900: 1754.463887532552,
                                                  1562766300: 18062.3916015625,
                                                  1562743800: 493.6506870814732,
                                                  1562746500: 2293.4739815848216,
                                                  1562748300: 437.6035334269206,
                                                  1562744700: 493.71276201520647,
                                                  1562763600: 15722.109375,
                                                  1562761800: 13898.59486607143}
                         },
                'unit': 'hours',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotRunningTime(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotConnections(self):
    """ test for the method "_plotConnections"
    """

    plotName = "ComponentMonitoringPlotter_plotConnections"
    reportRequest = {'grouping': 'componentName',
                     'groupingFields': ('%s', ['componentName']),
                     'startTime': 1562623200,
                     'endTime': 1562709600,
                     'condDict': {'componentName': 'Configuration/Server'}
                     }

    plotInfo = {'data': {'Configuration/Server': {1562666400: 1.0,
                                                  1562679000: 1.0,
                                                  1562667300: 1.0,
                                                  1562680800: 1.0,
                                                  1562668200: 1.0,
                                                  1562681700: 1.0,
                                                  1562683500: 1.0,
                                                  1562679900: 1.0,
                                                  1562682600: 1.0,
                                                  1562684400: 1.0}
                         },
                'unit': 'Connections',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotConnections(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotActiveQueries(self):
    """ test for the method "_plotActiveQueries"
    """

    plotName = "ComponentMonitoringPlotter_plotActiveQueries"
    reportRequest = {'grouping': 'componentName',
                     'groupingFields': ('%s', ['componentName']),
                     'startTime': 1562709600,
                     'endTime': 1562796000,
                     'condDict': {'componentName': 'Configuration/Server'}
                     }

    plotInfo = {'data': {'Configuration/Server': {1562745600: 0.07692307692307693,
                                                  1562752800: 0,
                                                  1562746500: 0.06666666666666667,
                                                  1562771700: 0,
                                                  1562754600: 0,
                                                  1562747400: 0.06666666666666667,
                                                  1562744700: 0,
                                                  1562769000: 0.06666666666666667,
                                                  1562748300: 0,
                                                  1562770800: 0,
                                                  1562749200: 0,
                                                  1562743800: 0,
                                                  1562750100: 0,
                                                  1562768100: 0.09090909090909091,
                                                  1562751000: 0,
                                                  1562753700: 0,
                                                  1562755500: 0,
                                                  1562751900: 0,
                                                  1562769900: 0.2}
                         },
                'unit': 'ActiveQueries',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotActiveQueries(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotPendingQueries(self):
    """ test for the method "_plotPendingQueries"
    """

    plotName = "ComponentMonitoringPlotter_plotPendingQueries"
    reportRequest = {'grouping': 'componentName',
                     'groupingFields': ('%s', ['componentName']),
                     'startTime': 1562709600,
                     'endTime': 1562796000,
                     'condDict': {'componentName': 'Configuration/Server'}
                     }

    plotInfo = {'data': {'Configuration/Server': {1562745600: 0,
                                                  1562752800: 0,
                                                  1562746500: 0,
                                                  1562771700: 0,
                                                  1562754600: 0,
                                                  1562747400: 0,
                                                  1562744700: 0,
                                                  1562769000: 0,
                                                  1562748300: 0,
                                                  1562770800: 0,
                                                  1562749200: 0,
                                                  1562743800: 0,
                                                  1562750100: 0, }
                         },
                'unit': 'PendingQueries',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotPendingQueries(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotMaxFD(self):
    """ test for the method "_plotMaxFD"
    """

    plotName = "ComponentMonitoringPlotter_plotMaxFD"
    reportRequest = {'grouping': 'componentName',
                     'groupingFields': ('%s', ['componentName']),
                     'startTime': 1562623200,
                     'endTime': 1562709600,
                     'condDict': {'componentName': 'Configuration/Server'}
                     }

    plotInfo = {'data': {'Configuration/Server': {1562666400: 13.0,
                                                  1562679000: 3.9,
                                                  1562667300: 4.533333333333333,
                                                  1562680800: 4.333333333333333,
                                                  1562668200: 3.3333333333333335,
                                                  1562681700: 4.4,
                                                  1562683500: 4.4,
                                                  1562679900: 4.4,
                                                  1562682600: 4.066666666666666,
                                                  1562684400: 4.2}
                         },
                'unit': 'MaxFD',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotMaxFD(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)

  def test_plotActivityRunningThreads(self):
    """ test for the method "_plotActivityRunningThreads"
    """

    plotName = "ComponentMonitoringPlotter_plotActivityRunningThreads"
    reportRequest = {'grouping': 'componentName',
                     'groupingFields': ('%s', ['componentName']),
                     'startTime': 1562709600,
                     'endTime': 1562796000,
                     'condDict': {'componentName': 'Configuration/Server'}
                     }

    plotInfo = {'data': {'Configuration/Server': {1562745600: 6.3076923076923075,
                                                  1562752800: 6.266666666666667,
                                                  1562746500: 6.2,
                                                  1562771700: 6.133333333333334,
                                                  1562754600: 6.0,
                                                  1562747400: 6.0,
                                                  1562744700: 6.25,
                                                  1562769000: 6.133333333333334,
                                                  1562748300: 6.25,
                                                  1562770800: 6.0,
                                                  1562749200: 6.2,
                                                  1562743800: 6.5,
                                                  1562750100: 6.076923076923077,
                                                  1562768100: 6.090909090909091,
                                                  1562751000: 6.0,
                                                  1562753700: 6.0,
                                                  1562755500: 6.230769230769231,
                                                  1562751900: 6.066666666666666,
                                                  1562769900: 6.066666666666666,
                                                  1562772600: 6.0}
                         },
                'unit': 'RunningThreads',
                'granularity': 900}

    obj = ComponentMonitoringPlotter(None, None)
    res = obj._plotActivityRunningThreads(reportRequest, plotInfo, plotName)
    self.assertEqual(res['OK'], True)
    self.assertEqual(res['Value'], {'plot': True, 'thumbnail': False})

    res = compare('%s.png' % plotName, '/home/dirac/DIRAC/tests/Integration/Monitoring/png/%s.png' % plotName)
    self.assertEqual(0.0, res)


if __name__ == "__main__":
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PlotterTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(ComponentMonitoringPlotterUnitTest))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
