""" :mod: GFAL2_XROOTStorage
    =================

    .. module: python
    :synopsis: XROOT module based on the GFAL2_StorageBase class.
"""


# from DIRAC
from DIRAC import gLogger
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase



class GFAL2_XROOTStorage( GFAL2_StorageBase ):

  """ .. class:: GFAL2_XROOTStorage

  Xroot interface to StorageElement using gfal2
  """

  def __init__( self, storageName, parameters ):
    """ c'tor

    :param self: self reference
    :param str storageName: SE name
    :param str protocol: protocol to use
    :param str rootdir: base path for vo files
    :param str host: SE host
    :param int port: port to use to communicate with :host:
    :param str spaceToken: space token
    :param str wspath: location of SRM on :host:
    """
    self.log = gLogger.getSubLogger( "GFAL2_XROOTStorage", True )
    # # init base class
    super( GFAL2_XROOTStorage, self ).__init__( storageName, parameters )

    self.pluginName = 'GFAL2_XROOT'

    self.protocolParameters['Port'] = 0
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0



  def _getExtendedAttributes( self, path ):
    """ Hard coding list of attributes and then call the base method of GFAL2_StorageBase

    :param self: self reference
    :param str path: path of which we want extended attributes
    :return: S_OK( attributeDict ) if successful. Where the keys of the dict are the attributes and values the respective values
    """

    # hard coding the attributes list for xroot because the plugin returns the wrong values
    # xrootd.* instead of xroot.* see: https://its.cern.ch/jira/browse/DMC-664
    attributes = ['xroot.cksum', 'xroot.space']
    res = super( GFAL2_XROOTStorage, self )._getExtendedAttributes( path, attributes )
    return res

  def _updateMetadataDict( self, metadataDict, attributeDict = None ):
    # Add metadata expected in some places if not provided by itself
    metadataDict['Lost'] = metadataDict.get( 'Lost', 0 )
    metadataDict['Cached'] = metadataDict.get( 'Cached', 1 )
    metadataDict['Unavailable'] = metadataDict.get( 'Unavailable', 0 )

  def _getSingleFile( self, src_url, dest_file ):
    """ Some XROOT StorageElements have problems with the checksum at the moment so to still be able to copy
    files from XROOT we disable the checksum check for this operation.

    :param self: self reference
    :param str src_url: path of the source file
    :param str dest_file: path of destination
    :returns: S_ERROR( errStr ) in case of an error
              S_OK( size of file ) if copying is successful

    """
    self.log.debug( "GFAL2_XROOTStorage._getSingleFile: Calling base method with checksum disabled" )
    res = super( GFAL2_XROOTStorage, self )._getSingleFile( src_url, dest_file, disableChecksum = True )
    return res
