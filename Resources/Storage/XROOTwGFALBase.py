from types import StringType, ListType
# from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Resources.Utilities import checkArgumentFormat
from DIRAC.Resources.Storage.GFAL2StorageBase import GFAL2StorageBase

class XROOTwGFALBase( GFAL2StorageBase ):

  """ .. class:: XROOTStorage

  Xroot interface to StorageElement using pyxrootd
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

    # # init base class
    GFAL2StorageBase.__init__( self, storageName, parameters )
    self.log = gLogger.getSubLogger( "XROOTwGFALBase", True )
#     self.log.setLevel( "DEBUG" )

    self.pluginName = 'XROOT'
    self.protocol = self.protocolParameters['Protocol']
    self.host = self.protocolParameters['Host']

    # Aweful hack to cope for the moment with the inability of RSS to deal with something else than SRM

    # self.port = ""
    # self.wspath = ""
    # self.spaceToken = ""

    self.protocolParameters['Port'] = 0
    self.protocolParameters['WSUrl'] = 0
    self.protocolParameters['SpaceToken'] = 0


  def getTransportURL( self, path, protocols = False ):
    """ obtain the tURLs for the supplied path and protocols

    :param self: self reference
    :param str path: path on storage (pfn : root://...)
    :param mixed protocols: protocols to use (must be or include 'root')
    :returns Successful dict {path : path}
             Failed dict {path : error message }
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']

    if protocols:
      if type( protocols ) is StringType:
        if protocols != self.protocol:
          return S_ERROR( "XROOTwGFALBase.getTransportURL: Must supply desired protocols to this plug-in (%s)." % self.protocol )
      elif type( protocols ) is ListType:
        if self.protocol not in protocols:
          return S_ERROR( "XROOTwGFALBase.getTransportURL: Must supply desired protocols to this plug-in (%s)." % self.protocol )

    # For the time being, I assume I should not check whether the file exists or not
    # So I just return the list of urls keys
    successful = dict( [rootUrl, rootUrl] for rootUrl in urls )
    failed = {}

    return S_OK( { 'Failed' : failed, 'Successful' : successful } )
