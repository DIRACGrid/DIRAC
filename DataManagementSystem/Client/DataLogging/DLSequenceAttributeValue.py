'''
Created on Jul 13, 2015

@author: Corentin Berger
'''


class DLSequenceAttributeValue( object ):
  """
    this is the DLSequenceAttributeValue class for data logging system
    this class is here because DLSequence objects can have different attributes to put into DataBase
    this class saves value of specific attribute of the sequence
  """

  def __init__( self, value ):
    super( DLSequenceAttributeValue, self ).__init__()
    self.value = value
    self.sequence = None
    self.sequenceAttribute = None
