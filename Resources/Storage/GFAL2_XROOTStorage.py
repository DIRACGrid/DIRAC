""" :mod: GFAL2_XROOTStorage
    =================

    .. module: python
    :synopsis: XROOT module based on the GFAL2_StorageBase class.
"""


# from DIRAC
from DIRAC import gLogger
from DIRAC.Resources.Storage.GFAL2_StorageBase import GFAL2_StorageBase
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse

class GFAL2_XROOTStorage( GFAL2_StorageBase ):

  """ .. class:: GFAL2_XROOTStorage

  Xroot interface to StorageElement using gfal2
  """


  PROTOCOL_PARAMETERS = GFAL2_StorageBase.PROTOCOL_PARAMETERS + ['SvcClass']
  DYNAMIC_OPTIONS = { 'SvcClass' : 'svcClass'}

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
    self.srmSpecificParse = False

    self.log = gLogger.getSubLogger( "GFAL2_XROOTStorage", True )

    self.pluginName = 'GFAL2_XROOT'

    # why is this here ?!
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0



    # We don't need extended attributes for metadata
    self._defaultExtendedAttributes = None


  def __addDoubleSlash( self, res ):
    """ Utilities to add the double slash between the host(:port) and the path

        :param res: DIRAC return structure which contains an URL if S_OK
        :return: DIRAC structure with corrected URL
    """
    if not res['OK']:
      return res
    url = res['Value']
    res = pfnparse( url, srmSpecific = self.srmSpecificParse )
    if not res['OK']:
      return res
    urlDict = res['Value']
    urlDict['Path'] = '/' + urlDict['Path']
    return pfnunparse( urlDict, srmSpecific = self.srmSpecificParse )

  def getURLBase( self, withWSUrl = False ):
    """ Overwrite to add the double slash """
    return self.__addDoubleSlash( super( GFAL2_XROOTStorage, self ).getURLBase( withWSUrl = withWSUrl ) )

  def constructURLFromLFN( self, lfn, withWSUrl = False ):
    """ Overwrite to add the double slash """
    return self.__addDoubleSlash( super( GFAL2_XROOTStorage, self ).constructURLFromLFN( lfn = lfn, withWSUrl = withWSUrl ) )

  def getCurrentURL( self, fileName ):
    """ Overwrite to add the double slash """
    return self.__addDoubleSlash( super( GFAL2_XROOTStorage, self ).getCurrentURL( fileName ) )


