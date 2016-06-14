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
    self.srmSpecificParse = False

    self.log = gLogger.getSubLogger( "GFAL2_XROOTStorage", True )

    self.pluginName = 'GFAL2_XROOT'

    # why is this here ?!
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0

    # We don't need extended attributes for metadata
    self._defaultExtendedAttributes = None



  def getURLBase( self, withWSUrl = False ):
    """ This will get the URL base. This is then appended with the LFN in DIRAC convention.

    :param self: self reference
    :param bool withWSUrl: flag to include Web Service part of the url
    :returns: URL
    """
    urlDict = dict( self.protocolParameters )
    if not withWSUrl:
      urlDict['WSUrl'] = ''
    if self.protocolParameters.get( 'Port', None ):
      url = "%(Protocol)s://%(Host)s:%(Port)s/%(Path)s" % urlDict
    else:
      url = "%(Protocol)s://%(Host)s/%(Path)s" % urlDict
    return S_OK( url )

  def constructURLFromLFN( self, lfn, withWSUrl = False ):
    """ Extend the method defined in the base class to add the Service Class if defined
    """

    res = super( GFAL2_XROOTStorage, self ).constructURLFromLFN( lfn, withWSUrl = withWSUrl )
    if not res['OK']:
      return res
    url = res['Value']
    svcClass = self.protocolParameters['SvcClass']
    if svcClass:
      url += '?svcClass=%s' % svcClass

    return S_OK( url )



