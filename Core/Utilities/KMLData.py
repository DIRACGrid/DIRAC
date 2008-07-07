########################################################################
# $Id: KMLData.py,v 1.3 2008/07/07 08:12:20 rgracian Exp $
########################################################################

""" Handles KML generation
"""

#############################################################################

class KMLData:

  #############################################################################
  def __init__(self):
    """ KMLData constructor
    """
    self.data = ''

  #############################################################################
  def __del__(self):
    """ KMLData destructor
    """
    pass

  #############################################################################
  def reset(self):
    """ Clears all KML data
    """
    self.data = ''

  #############################################################################
  def getKML(self):
    """ Returns a string which contains full KML data
    """
    header = ''
    header += '<?xml version="1.0" encoding="UTF-8"?>\n'
    header += '<kml xmlns="http://earth.google.com/kml/2.2">\n'
    header += '<Document>\n'
    header += '\t<name>DIRAC Grid Monitoring System</name>\n'

    footer = ''
    footer += '</Document>\n'
    footer += '</kml>\n'

    return '%s%s%s' % (header, self.data, footer)

  #############################################################################
  def writeFile(self, fileName):
    """ Writes the KML data to a file
    """
    fout = open(fileName, 'w')

    dataOut = self.getKML()
    fout.write(dataOut)
    fout.close()
  
  #############################################################################
  def addNodeStyle(self, name, icon, scale):
    self.data += '\t<Style id="%s">\n' % name
    self.data += '\t\t<IconStyle>\n'
    self.data += '\t\t\t<Icon>\n'
    #self.data += '\t\t\t\t<href>http://lhcbtest.pic.es/DIRAC/images/maps/%s</href>\n' % icon
    self.data += '\t\t\t\t<href>%s</href>\n' % icon
    self.data += '\t\t\t</Icon>\n'
    self.data += '\t\t\t<scale>%.1f</scale>\n' % scale
    self.data += '\t\t\t<hotSpot x="0.5" y="0.5" xunits="fraction" yunits="fraction"/>\n'
    self.data += '\t\t</IconStyle>\n'
    self.data += '\t</Style>\n'
    
  #############################################################################
  def addLinkStyle(self, name, color, width):
    # Note: color is in the form AABBGGRR (hexadecimal)
    self.data += '\t<Style id="%s">\n' % name
    self.data += '\t\t<LineStyle>\n'
    self.data += '\t\t\t<color>%s</color>\n' % color
    self.data += '\t\t\t<width>%d</width>\n' % width
    self.data += '\t\t</LineStyle>\n'
    self.data += '\t</Style>\n'

  #############################################################################
  def addMultipleScaledStyles(self, pathPrefix, prefix, data, suffix):
    for p in prefix:
      for d in data:
          self.addNodeStyle('%s%s' % (p, d), '%s%s%s' % (pathPrefix, p, suffix), data[d])

  #############################################################################
  def addNode(self, name, description, style, coord):
    self.data += '\t<Placemark>\n'
    self.data += '\t\t<name>\n'
    self.data += '\t\t\t<![CDATA[\n'
    self.data += '\t\t\t\t'
#    self.data += '<h4>%s</h4>\n' % name
    self.data += '%s\n' % name
    self.data += '\t\t\t]]>\n'
    self.data += '\t\t</name>\n'
    self.data += '\t\t<description>\n'
    self.data += '\t\t\t<![CDATA[\n'
    self.data += '\t\t\t\t'
#    self.data += '<h2>%s</h2>' % name
#    self.data += '<h3>%s</h3>\n' % description)        
    self.data += '%s\n' % description
    self.data += '\t\t\t]]>\n'
    self.data += '\t\t</description>\n'
    
    if style:
      self.data += '\t\t<styleUrl>#%s</styleUrl>\n' % style
      
    self.data += '\t\t<Point><coordinates>%.4f,%.4f,0</coordinates></Point>\n' % (coord[0], coord[1])
    self.data += '\t</Placemark>\n'
  
  #############################################################################  
  def addLink(self, name, description, style, src, dest):
    self.data += '\t<Placemark>\n'
    self.data += '\t\t<name>\n'
    self.data += '\t\t\t<![CDATA[\n'
    self.data += '\t\t\t\t'
#    self.data += '<h4>%s</h4>\n' % name
    self.data += '%s\n' % name
    self.data += '\t\t\t]]>\n'
    self.data += '\t\t</name>\n'
    self.data += '\t\t<description>\n'
    self.data += '\t\t\t<![CDATA[\n'
    self.data += '\t\t\t\t'
#    self.data += '<h2>%s</h2>' % name
#    self.data += '<h3>%s</h3>\n' % description)        
    self.data += '%s\n' % description
    self.data += '\t\t\t]]>\n'
    self.data += '\t\t</description>\n'
    
    if style:
      self.data += '\t\t<styleUrl>#%s</styleUrl>\n' % style
      
    self.data += '\t\t<LineString><coordinates>%.4f,%.4f,0 %.4f,%.4f,0</coordinates></LineString>\n'\
                         % (src[0], src[1], dest[0], dest[1])
    self.data += '\t</Placemark>\n'

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

