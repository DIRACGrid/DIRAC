########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Graphs/Palette.py,v 1.1 2009/06/01 22:03:05 atsareg Exp $
########################################################################

""" Palette is a tool to generate colors for various Graphs plots and legends
    
    The DIRAC Graphs package is derived from the GraphTool plotting package of the
    CMS/Phedex Project by ... <to be added>
"""

__RCSID__ = "$Id: Palette.py,v 1.1 2009/06/01 22:03:05 atsareg Exp $"

import md5

job_status_palette = {
   'Received':  '#D9E7F8',
   'Checking':  '#FAFAFA', 
   'Staging':   '#6190CD',
   'Waiting':   '#004EFF',
   'Matched':   '#FEF7AA',
   'Running':   '#FDEE65',
   'Stalled':   '#BC5757',
   'Completed': '#00FF21',
   'Done':      '#238802',
   'Failed':    '#FF0000',
   'Killed':    '#111111'
}

miscelaneous_pallette = {
   'Others':    '#666666',
   'NoLabels':  '#0025AD'
}

country_palette = {
  'France':'#73C6BC',
  'UK':'#DCAF8A',
  'Spain':'#C2B0E1',
  'Netherlands':'#A9BF8E',
  'Germany':'#800000',
  'Russia':'#00514A',
  'Italy':'#004F00',
  'Switzerland':'#433B00',
  'Poland':'#528220',
  'Hungary':'#825CE2',
  'Portugal':'#009182',
  'Turkey':'#B85D00'
}   

class Palette:

  def __init__(self,palette={},colors=[]):
  
    self.palette = country_palette 
    self.palette.update(job_status_palette)
    self.palette.update(miscelaneous_pallette)
    
    # extra generic colors
    self.colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']
    if colors:
      self.colors = colors
    self.cindex = 0
    self.ncolors = len(self.colors)
    
  def setPalette(self,palette):
    self.palette = palette  
    
  def getColor(self,label):
  
    if label in self.palette.keys():
      return self.palette[label]  
    else:
      #ind = self.cindex % self.ncolors
      #self.cindex += 1
      #return self.colors[ind]  
      return self.generateColor(label)
      
  def generateColor(self,label):
  
    myMD5 = md5.new()
    myMD5.update(label)
    hexstring = myMD5.hexdigest()
    color = "#"+hexstring[:6]
    return color    
