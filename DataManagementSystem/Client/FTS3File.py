import datetime


class FTS3File(object):

  FINAL_STATES = ['Canceled', 'Finished', 'Failed']

  def __init__( self ):

    self.status = None
    self.attempt = 0

    now = datetime.datetime.utcnow().replace( microsecond = 0 )

    self.creationTime = now
    self.lastUpdate = now


    self.rmsFileID = 0
    self.checksum = None
    self.size = 0

    self.lfn = None
    self.error = None

    self.targetSE = None
