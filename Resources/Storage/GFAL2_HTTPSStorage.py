""" :mod: GFAL2_HTTPSStorage
    =================

    .. module: python
    :synopsis: HTTPS module based on the GFAL2_StorageBase class.
"""

# from DIRAC
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC import gLogger


class GFAL2_HTTPSStorage( GFAL2_StorageBase ):

  """ .. class:: GFAL2_HTTPSStorage

  HTTP interface to StorageElement using gfal2
  """

  _INPUT_PROTOCOLS = ['file', 'http', 'https']
  _OUTPUT_PROTOCOLS = ['http', 'https']

  def __init__( self, storageName, parameters ):
    """ c'tor
    """
    # # init base class
    super( GFAL2_HTTPSStorage, self ).__init__( storageName, parameters )
    self.srmSpecificParse = False

    self.log = gLogger.getSubLogger( "GFAL2_HTTPSStorage" )

    self.pluginName = 'GFAL2_HTTPS'


    # We don't need extended attributes for metadata
    self._defaultExtendedAttributes = None
