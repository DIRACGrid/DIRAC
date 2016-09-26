import re
from DIRAC.Core.Utilities                                   import DIRACSingleton
from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
from DIRAC.Core.Utilities.Plotting.ObjectLoader             import loadObjects

class TypeLoader( object ):
  __metaclass__ = DIRACSingleton.DIRACSingleton

  def __init__( self ):
    self.__loaded = {}
    self.__path = "AccountingSystem/Client/Types"
    self.__parentCls = BaseAccountingType
    self.__reFilter = re.compile( r".*[a-z1-9]\.py$" )

  def getTypes( self ):
    if not self.__loaded:
      self.__loaded = loadObjects( self.__path, self.__reFilter, self.__parentCls )
    return self.__loaded
