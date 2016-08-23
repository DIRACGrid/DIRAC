"""
It is a helper module which contains the available reports
"""

__RCSID__ = "$Id$"

from DIRAC                                                import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder          import getServiceSection
from DIRAC.MonitoringSystem.private.Plotters.BasePlotter  import BasePlotter as myBasePlotter
from DIRAC.Core.Utilities.Plotting.ObjectLoader           import loadObjects

import hashlib
import re

class PlottersList( object ):
  
  """ 
  .. class:: PlottersList
  
  Used to determine all available plotters used to create the plots
  
  :param dict __plotters stores the available plotters
  """
  def __init__( self ):
    
    """ c'tor
    :param self: self reference
    """
    
    objectsLoaded = loadObjects( 'MonitoringSystem/private/Plotters',
                                 re.compile( ".*[a-z1-9]Plotter\.py$" ),
                                 myBasePlotter )
    self.__plotters = {}
    for objName in objectsLoaded:
      self.__plotters[ objName[:-7] ] = objectsLoaded[ objName ]

  def getPlotterClass( self, typeName ):
    """
    It returns the plotter class for a given monitoring type
    """
    try:
      return self.__plotters[ typeName ]
    except KeyError:
      return None

class MainReporter( object ):
  
  """ 
  .. class:: MainReporter
  
  :param object __db database object
  :param str __setup DIRAC setup
  :param str __csSection CS section used to configure some parameters.
  :param list __plotterList available plotters
  """
  def __init__( self, db, setup ):
    """ c'tor
    :param self: self reference
    :param object the database module
    :param str setup DIRAC setup
    """
    self.__db = db
    self.__setup = setup
    self.__csSection = getServiceSection( "Monitoring/Monitoring", setup = setup )
    self.__plotterList = PlottersList()

  def __calculateReportHash( self, reportRequest ):
    """
    It creates an unique identifier
    :param dict reportRequest plot attributes used to create the plot
    
    """
    requestToHash = dict( reportRequest )
    granularity = gConfig.getValue( "%s/CacheTimeGranularity" % self.__csSection, 300 )
    for key in ( 'startTime', 'endTime' ):
      epoch = requestToHash[ key ]
      requestToHash[ key ] = epoch - epoch % granularity
    md5Hash = hashlib.md5()
    md5Hash.update( repr( requestToHash ) )
    md5Hash.update( self.__setup )
    return md5Hash.hexdigest()

  def generate( self, reportRequest, credDict ):
    """
    It is used to create a plot.
    :param dict reportRequest plot attributes used to create the plot
    
    Note: I know credDict is not used, but if we plan to add some policy, we need to use it!
    """
    typeName = reportRequest[ 'typeName' ]
    plotterClass = self.__plotterList.getPlotterClass( typeName )
    if not plotterClass:
      return S_ERROR( "There's no reporter registered for type %s" % typeName )
    
    reportRequest[ 'hash' ] = self.__calculateReportHash( reportRequest )
    plotter = plotterClass( self.__db, self.__setup, reportRequest[ 'extraArgs' ] )
    return plotter.generate( reportRequest )

  def list( self, typeName ):
    """
    It returns the available plots
    :param str typeName monitoring type
    """
    plotterClass = self.__plotterList.getPlotterClass( typeName )
    if not plotterClass:
      return S_ERROR( "There's no plotter registered for type %s" % typeName )
    plotter = plotterClass( self.__db, self.setup )
    return S_OK( plotter.plotsList() )
