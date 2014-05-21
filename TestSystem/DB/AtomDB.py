from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB


class AtomDB( DB ):

  def __init__( self ):
    DB.__init__( self, 'AtomDB', 'Test/AtomDB', 10 )
    retVal = self.__initializeDB()
    if not retVal[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % retVal[ 'Message' ] )

  def __initializeDB( self ):
    """
    Create the tables
    """
    retVal = self._query( "show tables" )
    if not retVal[ 'OK' ]:
      return retVal

    tablesInDB = [ t[0] for t in retVal[ 'Value' ] ]
    tablesD = {}

    if 'atom_table' not in tablesInDB:
      tablesD[ 'atom_table' ] = { 'Fields' : { 'Id': 'INTEGER NOT NULL AUTO_INCREMENT', 'Stuff' : 'VARCHAR(64) NOT NULL' },
                                      'PrimaryKey' : [ 'Id' ]
                                     }


    return self._createTables( tablesD )

  def addStuf( self, something ):
    return self._insert( 'atom_table', [ 'stuff' ], [ something ] )

