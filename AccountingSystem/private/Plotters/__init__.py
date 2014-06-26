import re
from DIRAC.AccountingSystem.private.Plotters.BaseReporter import BaseReporter as myBaseReporter
from DIRAC.AccountingSystem.private.TypeLoader import TypeLoader

class PlottersList:

  def __init__( self ):
    objectsLoaded = TypeLoader.getTypes()
    self.__plotters = {}
    for objName in objectsLoaded:
      self.__plotters[ objName[:-7] ] = objectsLoaded[ objName ]

  def getPlotterClass( self, typeName ):
    try:
      return self.__plotters[ typeName ]
    except KeyError:
      return None

gPlottersList = PlottersList()
