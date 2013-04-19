
class BaseSplitter( object ):

  AFTER_OPTIMIZER = "JobSanity"

  def splitJob( self, jobState ):
    return S_ERROR( "Method splitJob has to be overwritten in %s" % __class__.__name__ )
