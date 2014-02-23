""" The POOL XML Slice class provides a simple plugin module to create
    an XML file for applications to translate LFNs to TURLs. The input
    dictionary has LFNs as keys with all associated metadata as key,
    value pairs.
"""

from DIRAC.Resources.Catalog.PoolXMLCatalog                         import PoolXMLCatalog
from DIRAC                                                          import S_OK, S_ERROR, gLogger

import os, types

__RCSID__ = "$Id$"

COMPONENT_NAME = 'PoolXMLSlice'

class PoolXMLSlice( object ):

  #############################################################################
  def __init__( self, catalogName ):
    """ Standard constructor
    """
    self.fileName = catalogName
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )

  #############################################################################
  def execute( self, dataDict ):
    """ Given a dictionary of resolved input data, this will creates a POOL XML slice.
    """
    poolXMLCatName = self.fileName
    try:
      poolXMLCat = PoolXMLCatalog()
      self.log.verbose( 'Creating POOL XML slice' )

      for lfn, mdataList in dataDict.items():
        # lfn,pfn,se,guid tuple taken by POOL XML Catalogue
        if type( mdataList ) != types.ListType:
          mdataList = [mdataList]
        # As a file may have several replicas, set first the file, then the replicas
        poolXMLCat.addFile( ( lfn, None, None, mdataList[0]['guid'], None ) )
        for mdata in mdataList:
          path = ''
          if 'path' in mdata:
            path = mdata['path']
          elif os.path.exists( os.path.basename( mdata['pfn'] ) ):
            path = os.path.abspath( os.path.basename( mdata['pfn'] ) )
          else:
            path = mdata['turl']
          poolXMLCat.addReplica( ( lfn, path, mdata['se'], False ) )

      xmlSlice = poolXMLCat.toXML()
      self.log.verbose( 'POOL XML Slice is: ' )
      self.log.verbose( xmlSlice )
      poolSlice = open( poolXMLCatName, 'w' )
      poolSlice.write( xmlSlice )
      poolSlice.close()
      self.log.info( 'POOL XML Catalogue slice written to %s' % ( poolXMLCatName ) )
      try:
        # Temporary solution to the problem of storing the SE in the Pool XML slice
        poolSlice_temp = open( '%s.temp' % ( poolXMLCatName ), 'w' )
        xmlSlice = poolXMLCat.toXML( True )
        poolSlice_temp.write( xmlSlice )
        poolSlice_temp.close()
      except Exception, x:
        self.log.warn( 'Attempted to write catalog also to %s.temp but this failed' % ( poolXMLCatName ) )
    except Exception, x:
      self.log.error( str( x ) )
      return S_ERROR( 'Exception during construction of POOL XML slice' )

    return S_OK( 'POOL XML Slice created' )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
