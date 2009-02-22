# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/FrameworkSystem/Service/PlottingHandler.py,v 1.1 2009/02/22 23:57:01 atsareg Exp $

""" Plotting Services generates graphs according to the client specifications
    and data
"""

__RCSID__ = "$Id: PlottingHandler.py,v 1.1 2009/02/22 23:57:01 atsareg Exp $"

import types
import os
import md5
from DIRAC import S_OK, S_ERROR, rootPath, gConfig, gLogger, gMonitor
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Utilities import Time
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.Graphs.PieGraph import DiracPieGraph
from PlotCache import gPlotCache
import tempfile

palette1 = [ '#75CFC7','#E5B78E','#CDB7EB','#AEC894','#982400','#006966',
             '#006700','#555000','#5B932D','#9B67F7','#00A397','#CA6D00']
palette2 = [ '#73C6BC','#DCAF8A','#C2B0E1','#A9BF8E','#800000','#00514A',
             '#004F00','#433B00','#528220','#825CE2','#009182','#B85D00']  

def initializePlottingHandler( serviceInfo ):

  #Get data location
  plottingSection = PathFinder.getServiceSection( "Framework/Plotting" )
  dataPath = gConfig.getValue( "%s/DataLocation" % plottingSection, "data/graphs" )
  
  print "AT >>>>", dataPath, rootPath
  
  dataPath = dataPath.strip()
  if "/" != dataPath[0]:
    dataPath = os.path.realpath( "%s/%s" % ( rootPath, dataPath ) )
  gLogger.info( "Data will be written into %s" % dataPath )
  try:
    os.makedirs( dataPath )
  except:
    pass
  try:
    testFile = "%s/acc.jarl.test" % dataPath
    fd = file( testFile, "w" )
    fd.close()
    os.unlink( testFile )
  except IOError:
    gLogger.fatal( "Can't write to %s" % dataPath )
    return S_ERROR( "Data location is not writable" )
    
  gPlotCache.setPlotsLocation( dataPath )
  gMonitor.registerActivity( "plotsDrawn", "Drawn plot images", "Plotting requests", "plots", gMonitor.OP_SUM )
  return S_OK()

class PlottingHandler( RequestHandler ):


  def getPieChat(self,data,metadata,fname):
  
    pieString = '' 
    tmpfile = open(fname,'w')
    pie = DiracPieGraph()
    pie.hex_colors = palette2
    pie.bottom_text = 'Bottom text'
    coords = pie.run( data, tmpfile, metadata, shadow=False )
    tmpfile.close()
    return S_OK({'plot':fname})  
    
  def __calculatePlotHash(self,data,metadata):
    m = md5.new()
    m.update(repr({'Data':data,'Metadata':metadata}))
    return m.hexdigest()  
    
  types_generatePieChat = [ types.DictType, types.DictType ]
  def export_generatePieChat( self, data, metadata ):
    """
    """

    plotHash = self.__calculatePlotHash(data,metadata)
    result = gPlotCache.getPlot(plotHash,data,metadata,self.getPieChat)
    if not result['OK']:
      return result
    return S_OK(plotHash)  

  def transfer_toClient( self, fileId, token, fileHelper ):
    """
    Get graphs data
    """
    retVal = gPlotCache.getPlotData( fileId )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = fileHelper.sendData( retVal[ 'Value' ] )
    if not retVal[ 'OK' ]:
      return retVal
    fileHelper.sendEOF()
    return S_OK()
