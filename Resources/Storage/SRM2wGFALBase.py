from DIRAC.Resources.Storage.GFAL2StorageBase import GFAL2StorageBase
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup




__RCSID__ = "$Id$"

class SRM2wGFALBase( GFAL2StorageBase ):
  """ SRM2 SE class that inherits from GFAL2StorageBase
  """
  
  def __init__( self, storageName, parameters ):
    """ """
    GFAL2StorageBase.__init__( self, storageName, parameters )

    self.pluginName = 'SRM2V2'


    # #stage limit - 12h
    self.stageTimeout = gConfig.getValue( '/Resources/StorageElements/StageTimeout', 12 * 60 * 60 )  # gConfig -> [get] ConfigurationClient()
    # # 1 file timeout
    self.fileTimeout = gConfig.getValue( '/Resources/StorageElements/FileTimeout', 30 )
    # # nb of surls per gfal2 call
    self.filesPerCall = gConfig.getValue( '/Resources/StorageElements/FilesPerCall', 20 )
    # # gfal2 timeout
    self.gfal2Timeout = gConfig.getValue( "/Resources/StorageElements/GFAL_Timeout", 100 )
    # # gfal2 long timeout
    self.gfal2LongTimeOut = gConfig.getValue( "/Resources/StorageElements/GFAL_LongTimeout", 1200 )
    # # gfal2 retry on errno.ECONN
    self.gfal2Retry = gConfig.getValue( "/Resources/StorageElements/GFAL_Retry", 3 )


    # # set checksum type, by default this is 0 (GFAL_CKSM_NONE)
    self.checksumType = gConfig.getValue( "/Resources/StorageElements/ChecksumType", 0 )
    # enum gfal_cksm_type, all in lcg_util
    #   GFAL_CKSM_NONE = 0,
    #   GFAL_CKSM_CRC32,
    #   GFAL_CKSM_ADLER32,
    #   GFAL_CKSM_MD5,
    #   GFAL_CKSM_SHA1
    # GFAL_CKSM_NULL = 0
    self.checksumTypes = { None : 0, "CRC32" : 1, "ADLER32" : 2,
                           "MD5" : 3, "SHA1" : 4, "NONE" : 0, "NULL" : 0 }
    if self.checksumType:
      if str( self.checksumType ).upper() in self.checksumTypes:
        gLogger.debug( "SRM2V2Storage: will use %s checksum check" % self.checksumType )
        self.checksumType = self.checksumTypes[ self.checksumType.upper() ]
      else:
        gLogger.warn( "SRM2V2Storage: unknown checksum type %s, checksum check disabled" )
        # # GFAL_CKSM_NONE
        self.checksumType = 0
    else:
      # # invert and get name
      self.log.debug( "SRM2V2Storage: will use %s checksum" % dict( zip( self.checksumTypes.values(),
                                                                     self.checksumTypes.keys() ) )[self.checksumType] )
    self.voName = None
    ret = getProxyInfo( disableVOMS = True )
    if ret['OK'] and 'group' in ret['Value']:
      self.voName = getVOForGroup( ret['Value']['group'] )
    self.defaultLocalProtocols = gConfig.getValue( '/Resources/StorageElements/DefaultProtocols', [] )

    self.MAX_SINGLE_STREAM_SIZE = 1024 * 1024 * 10  # 10 MB ???
    self.MIN_BANDWIDTH = 0.5 * ( 1024 * 1024 )  # 0.5 MB/s ???

