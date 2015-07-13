""" :mod: GFAL2_HTTPStorage
    =================

    .. module: python
    :synopsis: HTTP module based on the GFAL2_StorageBase class.
"""

# from DIRAC
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC import gLogger


class GFAL2_HTTPStorage( GFAL2_StorageBase ):

  """ .. class:: GFAL2_HTTPStorage

  HTTP interface to StorageElement using gfal2
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
    :param str wspath: location of HTTP on :host:
    """
    self.log = gLogger.getSubLogger( "GFAL2_HTTPStorage", True )
    # # init base class
    GFAL2_StorageBase.__init__( self, storageName, parameters )

    # TODO: test if HTTP can handle checksums.
    self.checksumType = None

#     self.log.setLevel( "DEBUG" )

    self.pluginName = 'GFAL2_HTTP'
    self.protocol = self.protocolParameters['Protocol']
    self.host = self.protocolParameters['Host']


    self.protocolParameters['Port'] = 0
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0
