import types
from DIRAC.Core.Utilities import Time

class DBUtils (object):

  def __init__( self, db, setup ):
    self.__db = db
    self.__setup = setup
    
  def getKeyValues( self, typeName, condDict ):
    """
    Get all valid key values in a type
    """
    return self.__db.getKeyValues( self.__setup, typeName, condDict )