""" This is the Regression test for DIRAC.MonitoringSystem.private.Plotters in order to ensure
    the proper working of the plotter.
"""

# pylint: disable=invalid-name,wrong-import-position

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from DIRAC.tests.Utilities.plots import compare

# sut
from DIRAC.MonitoringSystem.private.Plotters.ComponentMonitoringPlotter import ComponentMonitoringPlotter

plots_directory = os.path.join(os.path.dirname(__file__), 'plots')


def test_plotRunningThreads():
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
  assert res['OK'] is True
  assert res['Value'] == {'plot': True, 'thumbnail': False}

  res = compare('%s.png' % plotName,
                '%s.png' % os.path.join(plots_directory, plotName))
  assert res == 0.0


def test_plotCpuUsage():
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
  assert res['OK'] is True
  assert res['Value'] == {'plot': True, 'thumbnail': False}

  res = compare('%s.png' % plotName,
                '%s.png' % os.path.join(plots_directory, plotName))
  assert res == 0.0


def test_plotMemoryUsage():
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
  assert res['OK'] is True
  assert res['Value'] == {'plot': True, 'thumbnail': False}

  res = compare('%s.png' % plotName,
                '%s.png' % os.path.join(plots_directory, plotName))
  assert res == 0.0


# FIXME: failing

# def test_plotRunningTime():
#   """ test for the method "_plotRunningTime"
#   """

#   plotName = "ComponentMonitoringPlotter_plotRunningTime"
#   reportRequest = {'grouping': 'component',
#                    'groupingFields': ('%s', ['component']),
#                    'startTime': 1562796000,
#                    'endTime': 1562882400,
#                    'condDict': {'component': 'Monitoring_Monitoring'}
#                    }

#   plotInfo = {'data': {'Monitoring_Monitoring': {1562853600: 313.32080459594727,
#                                                  1562857200: 0,
#                                                  1562854500: 0,
#                                                  1562855400: 0,
#                                                  1562856300: 0,
#                                                  1562858100: 0,
#                                                  1562849100: 1754.9678344726562,
#                                                  1562860800: 0,
#                                                  1562850000: 492.68797302246094,
#                                                  1562848200: 854.48386492048,
#                                                  1562859900: 0,
#                                                  1562850900: 1332.7680140904017,
#                                                  1562851800: 1812.8760986328125,
#                                                  1562847300: 314.9701385498047,
#                                                  1562852700: 3452.56787109375}
#                        },
#               'unit': 'seconds',
#               'granularity': 900}

#   obj = ComponentMonitoringPlotter(None, None)
#   res = obj._plotRunningTime(reportRequest, plotInfo, plotName)
#   assert res['OK'] is True
#   assert res['Value'] == {'plot': True, 'thumbnail': False}

#   res = compare('%s.png' % plotName,
#                 '%s.png' % os.path.join(plots_directory, plotName))
#   assert res == 0.0


def test_plotConnections():
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
  assert res['OK'] is True
  assert res['Value'] == {'plot': True, 'thumbnail': False}

  res = compare('%s.png' % plotName,
                '%s.png' % os.path.join(plots_directory, plotName))
  assert res == 0.0


def test_plotActiveQueries():
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
  assert res['OK'] is True
  assert res['Value'] == {'plot': True, 'thumbnail': False}

  res = compare('%s.png' % plotName,
                '%s.png' % os.path.join(plots_directory, plotName))
  assert res == 0.0


def test_plotPendingQueries():
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
  assert res['OK'] is True
  assert res['Value'] == {'plot': True, 'thumbnail': False}

  res = compare('%s.png' % plotName,
                '%s.png' % os.path.join(plots_directory, plotName))
  assert res == 0.0


def test_plotMaxFD():
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
  assert res['OK'] is True
  assert res['Value'] == {'plot': True, 'thumbnail': False}

  res = compare('%s.png' % plotName,
                '%s.png' % os.path.join(plots_directory, plotName))
  assert res == 0.0


def test_plotActivityRunningThreads():
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
  assert res['OK'] is True
  assert res['Value'] == {'plot': True, 'thumbnail': False}

  res = compare('%s.png' % plotName,
                '%s.png' % os.path.join(plots_directory, plotName))
  assert res == 0.0
