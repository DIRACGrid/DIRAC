"""
This class is used to define the plot using the plot attributes.
"""

__RCSID__ = "$Id$"

from DIRAC                                                import S_OK, S_ERROR
from DIRAC.MonitoringSystem.Client.Types.WMSHistory       import WMSHistory
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter  import BasePlotter

class WMSHistoryPlotter( BasePlotter ):

  """ 
  .. class:: WMSHistoryPlotter
  
  It is used to crate the plots. 
  
  param: str _typeName monitoring type
  param: list _typeKeyFields list of keys what we monitor (list of attributes)
  """
  
  _typeName = "WMSHistory"
  _typeKeyFields =  WMSHistory().getKeyFields() 

  def _reportNumberOfJobs( self, reportRequest ):
    """
    It is used to retrieve the data from the database.
    :param dict reportRequest contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length 
    """
    selectFields = ['Jobs']
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ])
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]    
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotNumberOfJobs( self, reportRequest, plotInfo, filename ):
    """
    It creates the plot.
    :param dict reportRequest plot attributes
    :param dict plotInfo contains all the data which are used to create the plot
    :param str filename
    :return S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    metadata = { 'title' : 'Jobs by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : "jobs"  }
    plotInfo[ 'data' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'data' ] )    
    return self._generateStackedLinePlot( filename, plotInfo[ 'data' ], metadata )


  def _reportNumberOfReschedules( self, reportRequest ):
    """
    It is used to retrieve the data from the database.
    :param dict reportRequest contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length 
    """
    selectFields = ['Reschedules']
    retVal = self._getTimedData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ])
    
    if not retVal[ 'OK' ]:
      return retVal
    dataDict, granularity = retVal[ 'Value' ]
    return S_OK( { 'data' : dataDict, 'granularity' : granularity } )

  def _plotNumberOfReschedules( self, reportRequest, plotInfo, filename ):
    """
    It creates the plot.
    :param dict reportRequest plot attributes
    :param dict plotInfo contains all the data which are used to create the plot
    :param str filename
    :return S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    metadata = { 'title' : 'Reschedules by %s' % reportRequest[ 'grouping' ] ,
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ],
                 'span' : plotInfo[ 'granularity' ],
                 'skipEdgeColor' : True,
                 'ylabel' : "reschedules"  }
    plotInfo[ 'data' ] = self._fillWithZero( plotInfo[ 'granularity' ], reportRequest[ 'startTime' ], reportRequest[ 'endTime' ], plotInfo[ 'data' ] )
    return self._generateStackedLinePlot( filename, plotInfo[ 'data' ], metadata )

  def _reportAverageNumberOfJobs( self, reportRequest ):
    """
    It is used to retrieve the data from the database.
    :param dict reportRequest contains attributes used to create the plot.
    :return S_OK or S_ERROR {'data':value1, 'granularity':value2} value1 is a dictionary, value2 is the bucket length 
    """
    selectFields = ['Jobs']
                   
    retVal = self._getSummaryData( reportRequest[ 'startTime' ],
                                reportRequest[ 'endTime' ],
                                selectFields,
                                reportRequest[ 'condDict' ],
                                {"metric": "avg"} )
    if not retVal[ 'OK' ]:
      return retVal
    dataDict = retVal[ 'Value' ]
    return S_OK( { 'data' : dataDict  } )

  def _plotAverageNumberOfJobs( self, reportRequest, plotInfo, filename ):
    """
    It creates the plot.
    :param dict reportRequest plot attributes
    :param dict plotInfo contains all the data which are used to create the plot
    :param str filename
    :return S_OK or S_ERROR { 'plot' : value1, 'thumbnail' : value2 } value1 and value2 are TRUE/FALSE
    """
    metadata = { 'title' : 'Average Number of Jobs by %s' % reportRequest[ 'grouping' ],
                 'ylabel' : 'Jobs',
                 'starttime' : reportRequest[ 'startTime' ],
                 'endtime' : reportRequest[ 'endTime' ]
                }
    return self._generatePiePlot( filename, plotInfo[ 'data'], metadata )
