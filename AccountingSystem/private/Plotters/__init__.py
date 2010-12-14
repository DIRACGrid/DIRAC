import re
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter
from DIRAC.AccountingSystem.private.ObjectLoader import loadObjects

class PlottersList:

  def __init__( self ):
    objectsLoaded = loadObjects( "AccountingSystem/private/Plotters",
                                 re.compile( ".*[a-z1-9]Plotter\.py$" ),
                                 BaseReporter )
    self.__plotters = {}
    for objName in objectsLoaded:
      self.__plotters[ objName[:-7] ] = objectsLoaded[ objName ]

  def getPlotterClass( self, typeName ):
    try:
      return self.__plotters[ typeName ]
    except KeyError:
      return None

gPlottersList = PlottersList()
