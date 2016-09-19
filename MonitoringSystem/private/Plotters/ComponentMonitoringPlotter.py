"""
This class is used to define the plot using the plot attributes.
"""

from DIRAC import S_OK

from DIRAC.MonitoringSystem.Client.Types.ComponentMonitoring import ComponentMonitoring
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter import BasePlotter

__RCSID__ = "$Id$"

class ComponentMonitoringPlotter( BasePlotter ):

  """ 
  .. class:: ComponentMonitoringPlotter
  
  It is used to crate the plots. 
  
  param: str _typeName monitoring type
  param: list _typeKeyFields list of keys what we monitor (list of attributes)
  """
  
  _typeName = "ComponentMonitoring"
  _typeKeyFields =  ComponentMonitoring().getKeyFields() 

  _reportNumberOfThreadsName = "Number of running threads"
  def _reportNumberOfThreads( self, reportRequest ):
    """
    It is used to retrieve the data from the database.
    :param dict reportRequest contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length 
    """
    selectFields = ['Jobs']
    retVal = self._getTimedData( startTime = reportRequest[ 'startTime' ],
                                 endTime = reportRequest[ 'endTime' ],
                                 selectFields = selectFields,
                                 preCondDict = reportRequest[ 'condDict' ],
                                 metadataDict = None )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]    
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotNumberOfThreads( self, reportRequest, plotInfo, filename ):
    """
    It creates the plot.
    :param dict reportRequest plot attributes
    :param dict plotInfo contains all the data which are used to create the plot
    :param str filename
    :return S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    metadata = {'title' : 'Jobs by %s' % reportRequest[ 'grouping' ],
                'starttime' : reportRequest[ 'startTime' ],
                'endtime' : reportRequest[ 'endTime' ],
                'span' : plotInfo[ 'granularity' ],
                'skipEdgeColor' : True,
                'ylabel' : "jobs"}
    
    plotInfo[ 'data' ] = self._fillWithZero( granularity = plotInfo[ 'granularity' ],
                                             startEpoch = reportRequest[ 'startTime' ],
                                             endEpoch = reportRequest[ 'endTime' ],
                                             dataDict = plotInfo[ 'data' ] )
    
    return self._generateStackedLinePlot( filename = filename,
                                          dataDict = plotInfo[ 'data' ],
                                          metadata = metadata )


 
