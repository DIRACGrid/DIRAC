""" :mod: GFAL2_XROOTStorage
    =================

    .. module: python
    :synopsis: XROOT module based on the GFAL2_StorageBase class.
"""


# from DIRAC
from DIRAC import gLogger, S_OK
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase



class GFAL2_XROOTStorage( GFAL2_StorageBase ):

  """ .. class:: GFAL2_XROOTStorage

  Xroot interface to StorageElement using gfal2
  """


  PROTOCOL_PARAMETERS = GFAL2_StorageBase.PROTOCOL_PARAMETERS + ['SvcClass']

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
    # # init base class
    super( GFAL2_XROOTStorage, self ).__init__( storageName, parameters )

    self.log = gLogger.getSubLogger( "GFAL2_XROOTStorage", True )

    self.pluginName = 'GFAL2_XROOT'

    # why is this here ?!
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0



  def _getExtendedAttributes( self, path, _attributes = None ):
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
  
  def constructURLFromLFN( self, lfn, withWSUrl = False ):
    """ Extend the method defined in the base class to add the Service Class if defined
    """

    res = super(GFAL2_XROOTStorage, self).constructURLFromLFN(lfn, withWSUrl = withWSUrl)
    if not res['OK']:
      return res
    url = res['Value']
    svcClass = self.protocolParameters['SvcClass']
    if svcClass:
      url += '?svcClass=%s'%svcClass

    return S_OK(url)

