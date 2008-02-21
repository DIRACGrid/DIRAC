from DIRAC import S_OK, S_ERROR
from pylab import *
import os

class Graph:
  
  def histogram(self,plotTitle,xLabel,yLabel,dataPoints,outputFile):
    """ Plot a histogram for the supplied data points
    """
    try:
      n, bins, patches = hist(dataPoints,100,fc='g')
      meanDuration = '%.1f' % mean(dataPoints)
      stdevDuration = '%.1f' % std(dataPoints)
      maxDuration =  '%.1f' % max(dataPoints)
      minDuration = '%.1f' % min(dataPoints)

      text(600, 50, '%s%s' % ('Mean:'.ljust(7),meanDuration.rjust(6)) , fontsize = 20, color='k')
      text(600,45,'%s%s' % ('StDev:'.ljust(7),stdevDuration.rjust(6)), fontsize = 20,color ='k')
      text(600,40,'%s%s' % ('Max: '.ljust(7),maxDuration.rjust(6)), fontsize = 20,color='k')
      text(600,35,'%s%s' % ('Min: '.ljust(7),minDuration.rjust(6)), fontsize = 20,color='k')

      xlim(0,max(dataPoints))
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


