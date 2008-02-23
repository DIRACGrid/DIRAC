from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from pylab import *
import os

class Graph:
  
  def histogram(self,plotTitle,xLabel,yLabel,dataPoints,outputFile):
    """ Plot a histogram for the supplied data points
    """
    try:
      count, bins, patches = hist(dataPoints,100,fc='g')
      meanDuration = '%.1f' % mean(dataPoints)
      stdevDuration = '%.1f' % std(dataPoints)
      maxDuration =  '%.1f' % max(dataPoints)
      minDuration = '%.1f' % min(dataPoints)
      
      # give 5% space at each side of the data range
      xRange = (min(dataPoints)-max(dataPoints))*0.01 
      xMin = min(dataPoints) - xRange
      xMax = max(dataPoints) + xRange
      #if (xMin < 0) and (min(dataPoints) > 0):
      #  xMin = 0
      #xlim(xMin,xMax)

      # give 5% space at each side of the data range
      yRange = (max(count))*0.01
      yMax = max(count) + yRange
      #ylim(0,yMax)
      grid(linestyle='--', linewidth=0.02)
 
      legendXLoc = 0.666*xMax
      legendYStart = 0.95*yMax
      legendYDiff = 0.066*yMax
      text(legendXLoc, legendYStart, '%s%s' % ('Mean:'.ljust(7),meanDuration.rjust(6)) , fontsize = 20, color='k')
      text(legendXLoc,legendYStart-legendYDiff,'%s%s' % ('StDev:'.ljust(7),stdevDuration.rjust(6)), fontsize = 20,color ='k')
      text(legendXLoc,legendYStart-(2*legendYDiff),'%s%s' % ('Max: '.ljust(7),maxDuration.rjust(6)), fontsize = 20,color='k')
      text(legendXLoc,legendYStart-(3*legendYDiff),'%s%s' % ('Min: '.ljust(7),minDuration.rjust(6)), fontsize = 20,color='k')

      xlabel(xLabel,fontsize=20)
      ylabel(yLabel,fontsize=20)
      title(plotTitle,fontsize=20) 
      xticks(fontsize=15)
      yticks(fontsize=15)
      savefig(outputFile)
      show()

      return S_OK()
    except Exception,x:
      return S_ERROR(str(x)) 


