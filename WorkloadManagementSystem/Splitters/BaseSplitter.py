
class BaseSplitter( object ):

  AFTER_OPTIMIZER = "JobPath"

  def __init__( self, logger ):
    self.__jobLog = logger

  @property
  def jobLog( self ):
    return self.__jobLog

  def splitJob( self, jobState ):
    return S_ERROR( "Method splitJob has to be overwritten in %s" % __class__.__name__ )
