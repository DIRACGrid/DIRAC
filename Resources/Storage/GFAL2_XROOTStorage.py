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
    GFAL2_StorageBase.__init__( self, storageName, parameters )

#     self.log.setLevel( "DEBUG" )

    self.pluginName = 'GFAL2_XROOT'

    self.protocolParameters['Port'] = 0
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0


  def _getExtendedAttributes( self, path ):
    """ Hard coding list of attributes and then call the base method of GFAL2_StorageBase

    :param self: self reference
    :param str path: path of which we want extended attributes
    :return S_OK( attributeDict ) if successful. Where the keys of the dict are the attributes and values the respective values
    """

    # hard coding the attributes list for xroot because the plugin returns the wrong values
    # xrootd.* instead of xroot.* see: https://its.cern.ch/jira/browse/DMC-664
    attributes = ['xroot.cksum', 'xroot.space']
    res = GFAL2_StorageBase._getExtendedAttributes( self, path, attributes )
    return res

