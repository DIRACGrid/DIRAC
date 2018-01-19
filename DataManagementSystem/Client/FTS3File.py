import datetime
from DIRAC.DataManagementSystem.private.FTS3Utilities import FTS3Serializable

class FTS3File( FTS3Serializable ):
  """ This class represents an a File on which a given Operation
      (Transfer, Staging) should be executed
   """

  ALL_STATES = [ 'New',  # Nothing was attempted yet on this file
                 'Submitted', # From FTS: Initial state of a file as soon it's dropped into the database
                 'Ready', # From FTS: File is ready to become active
                 'Active', # From FTS: File went active
                 'Finished', # From FTS: File finished gracefully
                 'Canceled', # From FTS: Canceled by the user
                 'Staging', # From FTS: When staging of a file is requested
                 'Failed', # From FTS: File failure
                 'Defunct', # Totally fail, no more attempt will be made
                 ]
  
  FINAL_STATES = ['Canceled', 'Finished', 'Defunct']
  FTS_FINAL_STATES = ['Canceled', 'Finished', 'Done']
  INIT_STATE = 'New'

  _attrToSerialize = ['fileID', 'operationID', 'status', 'attempt', 'creationTime',
                      'lastUpdate', 'rmsFileID', 'checksum', 'size', 'lfn', 'error', 'targetSE']


  def __init__( self ):

    self.status = FTS3File.INIT_STATE
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
    
    
  @staticmethod
  def fromRMSFile( rmsFile, targetSE ):
    """ Returns an FTS3File constructed from an RMS File.
        It takes the value of LFN, rmsFileID, checksum and Size

        :param rmsFile: the RMS File to use as source
        :param targetSE: the SE target

        :returns: an FTS3File instance
    """
    ftsFile = FTS3File()
    ftsFile.lfn = rmsFile.LFN
    ftsFile.rmsFileID = rmsFile.FileID
    ftsFile.checksum = rmsFile.Checksum
    ftsFile.size = rmsFile.Size
    ftsFile.targetSE = targetSE

    return ftsFile

