########################################################################
# $Id: PoolXMLSlice.py,v 1.1 2008/01/31 14:46:53 paterson Exp $
# File :   PoolXMLSlice.py
# Author : Stuart Paterson
########################################################################

""" The POOL XML Slice class provides a simple plugin module to create
    an XML file for applications to translate LFNs to TURLs. The input
    dictionary has LFNs as keys with all associated metadata as key,
    value pairs.
"""

__RCSID__ = "$Id: PoolXMLSlice.py,v 1.1 2008/01/31 14:46:53 paterson Exp $"

from DIRAC.DataManagementSystem.Client.PoolXMLCatalog               import PoolXMLCatalog
from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger

import os,sys,re

COMPONENT_NAME = 'PoolXMLSlice'

class PoolXMLSlice:

  #############################################################################
  def __init__(self,catalogName):
    """ Standard constructor
    """
    self.fileName = catalogName
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)

  #############################################################################
  def execute(self,dataDict):
    """Given a dictionary of resolved input data, this will create a POOL
       XML slice.
    """
    poolXMLCatName = self.fileName
    mdata = dataDict
    try:
      poolXMLCat = PoolXMLCatalog()
      self.log.verbose('Creating POOL XML slice')

      for lfn,mdata in dataDict.items():
        local = os.path.basename(mdata['pfn'])
        #lfn,pfn,size,se,guid tuple taken by POOL XML Catalogue
        if os.path.exists(local):
          poolXMLCat.addFile((lfn,os.path.abspath(local),0,mdata['se'],mdata['guid']))
        else:
          poolXMLCat.addFile((lfn,mdata['turl'],0,mdata['se'],mdata['guid']))

      xmlSlice = poolXMLCat.toXML()
      self.log.verbose('POOL XML Slice is: ')
      self.log.verbose(xmlSlice)
      poolSlice = open(poolXMLCatName,'w')
      poolSlice.write(xmlSlice)
      poolSlice.close()
      self.log.info('POOL XML Catalogue slice written to %s' %(poolXMLCatName))
      # Temporary solution to the problem of storing the SE in the Pool XML slice
      poolSlice_temp = open('%s.temp' %(poolXMLCatName),'w')
      xmlSlice = poolXMLCat.toXML(True)
      poolSlice_temp.write(xmlSlice)
      poolSlice_temp.close()
    except Exception,x:
      self.log.warn(str(x))
      return S_ERROR('Exception during construction of POOL XML slice')

    return S_OK('POOL XML Slice created')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#