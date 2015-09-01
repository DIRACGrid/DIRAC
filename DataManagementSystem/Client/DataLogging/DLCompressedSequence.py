'''
Created on Jun 19, 2015

@author: Corentin Berger
'''
from datetime import datetime

class DLCompressedSequence( object ):
  """ This class is here for the mapping with the table DLCompressedSequence
      value is a DLSequence json that is compressed
      status can be :
              -Waiting, the sequence is waiting for insertion
              -Ongoing, the insertion is ongoing
              -Done, the insertion is done
      lastUpdate is the last time we did something about this DLCompressedSequence
  """

  def __init__( self, value, status = 'Waiting', compressedSequenceID = None ):
    self.value = value
    self.lastUpdate = datetime.now()
    self.status = status
    self.compressedSequenceID = compressedSequenceID
