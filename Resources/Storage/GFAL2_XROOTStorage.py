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

    # XROOT has problems with checksums at the moment.
    self.checksumType = None

#     self.log.setLevel( "DEBUG" )

    self.pluginName = 'GFAL2_XROOT'
    self.protocol = self.protocolParameters['Protocol']
    self.host = self.protocolParameters['Host']

    # Aweful hack to cope for the moment with the inability of RSS to deal with something else than SRM

    # self.port = ""
    # self.wspath = ""
    # self.spaceToken = ""

    self.protocolParameters['Port'] = 0
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0
