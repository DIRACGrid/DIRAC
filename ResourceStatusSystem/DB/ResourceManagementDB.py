################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.Utilities.MySQLMonkey import MySQLMonkey

class ResourceManagementDB(object):
  """
  The ResourceManagementDB class is a front-end to the ResourceManagementDB MySQL db.
  If exposes four basic methods:
  
  - insert
  - update
  - get
  - delete
  
  all them defined on the MySQL monkey class.
  Moreover, there are a set of key-worded parameters that can be used, specially
  on the getX and deleteX functions ( to know more, again, check the MySQL monkey
  documentation ).
  
  The DB schema has NO foreign keys, so there may be some small consistency checks,
  called validators on the insert and update functions.  

  The simplest way to instantiate an object of type :class:`ResourceManagementDB`
  is simply by calling

   >>> rmDB = ResourceManagementDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`.
  But there's the possibility to use other DB classes.
  For example, we could pass custom DB instantiations to it,
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rmDB = ResourceManagementDB( DBin = AnotherDB )

  Alternatively, for testing purposes, you could do:

   >>> from mock import Mock
   >>> mockDB = Mock()
   >>> rmDB = ResourceManagementDB( DBin = mockDB )

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rmDB = ResourceManagementDB( DBin = [ 'UserName', 'Password' ] )
   
  The ResourceStatusDB also exposes database Schema information, either on a 
  dictionary or on a MySQLSchema tree object.
  
  - getSchema
  - inspectSchema
    
  Alternatively, we can access the MySQLSchema XML and tree as follows:
  
   >>> rmDB = ResourceManagementDB()
   >>> xml  = rmDB.mm.SCHEMA
   >>> tree = rmDB.mm.mSchema
   >>> tree
   >>> tree.
   >>> tree.PolicyResult.Name
  
  """

  def __init__( self, *args, **kwargs ):
    """Constructor."""
    if len(args) == 1:
      if isinstance(args[0], str):
        maxQueueSize=10
      if isinstance(args[0], int):
        maxQueueSize=args[0]
    elif len(args) == 2:
      maxQueueSize=args[1]
    elif len(args) == 0:
      maxQueueSize=10

    if 'DBin' in kwargs.keys():
      DBin = kwargs[ 'DBin' ]
      if isinstance(DBin, list):
        from DIRAC.Core.Utilities.MySQL import MySQL
        self.db = MySQL( 'localhost', DBin[0], DBin[1], 'ResourceManagementDB' )
      else:
        self.db = DBin
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB( 'ResourceManagementDB', 'ResourceStatus/ResourceManagementDB', maxQueueSize )

    self.mm    = MySQLMonkey( self )  

  def insert( self, args, kwargs ):
    """      
    Inserts args in the DB making use of kwargs where parameters such as
    the table are specified ( filled automatically by the Client). In order to 
    do the insertion, it uses MySQLMonkey to do the parsing, execution and
    error handling. Typically you will not pass kwargs to this function, unless
    you know what are you doing and you have a very special use case.    
      
    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).
    
      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    """
    return self.mm.insert2( *args, **kwargs )

  def update( self, args, kwargs ):
    """   
    Updates row with values given on args. The row selection is done using the
    default of MySQLMonkey ( column.primary or column.keyColumn ). It can be
    modified using kwargs, but it is not explained here. The table keyword 
    argument is mandatory, and filled automatically by the Client. Typically 
    you will not pass kwargs to this function, unless you know what are you 
    doing and you have a very special use case.
       
    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).
    
      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    """
    return self.mm.update2( *args, **kwargs )

  def get( self, args, kwargs ):
    """  
    Uses arguments to build conditional SQL statement ( WHERE ... ). If the 
    sql statement desired is more complex, you can use kwargs to interact with
    the MySQLStatement parser and generate a more sophisticated query.
       
    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).
    
      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    """
    return self.mm.get2( *args, **kwargs )

  def delete( self, args, kwargs ):
    """     
    Uses arguments to build conditional SQL statement ( WHERE ... ). If the 
    sql statement desired is more complex, you can use kwargs to interact with
    the MySQLStatement parser and generate a more sophisticated query. There is
    only one forbidden query, with all parameters None ( this would mean a query
    of the type `DELETE * from TableName` ). The usage of kwargs is the same 
    as in the get function.
       
    :Parameters:
      **args** - `tuple`
        arguments for the mysql query ( must match table columns ! ).
    
      **kwargs** - `dict`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    """
    return self.mm.delete2( *args, **kwargs )
  
  def getSchema( self ):
    """  
    Returns a dictionary with database schema, this includes table and column
    names. It has two variants, columns and keyUsage. The first one has at least,
    as many keys as keyUsage, it is the complete schema. The second one is the 
    one used for the default updates and selects -- not taking into account 
    auto_increment fields, but taking into account primary and keyUsage fields.
      
    :Parameters:
      `None`
    
    :return: S_OK()
    """    
    return { 'OK': True, 'Value' : self.mm.SCHEMA }
    
  def inspectSchema( self ):
    """   
    Returns an object which represents the database schema and can be browsed.
     >>> db = ResourceManagementDB()
     >>> schema = db.inspectSchema()[ 'Value' ]
     >>> schema
         Schema 123:
         <TableName1>,<TableName2>...
     >>> schema.TableName1
         Table TableName1:
         <ColumnName1>,<ColumnName2>..
  
    Every column has a few attributes ( primary, keyUsage, extra, position,
    dataType and charMaxLen ). 
     
    :Parameters:
      `None`
    
    :return: S_OK()
    """    
    return { 'OK': True, 'Value' : self.mm.mSchema }

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

'''
This will go to the booster

################################################################################
## Web functions
################################################################################
#
################################################################################
#
#  def getDownTimesWeb(self, selectDict, _sortList = [], startItem = 0, maxItems = 1000):
#    """
#    Get downtimes registered in the RSS DB (with a web layout)
#
#    :params:
#      :attr:`selectDict`: { 'Granularity':['Site', 'Resource'], 'Severity': ['OUTAGE', 'AT_RISK']}
#
#      :attr:`sortList`
#
#      :attr:`startItem`
#
#      :attr:`maxItems`
#    """
#
#    granularity = selectDict['Granularity']
#    severity = selectDict['Severity']
#
#    if not isinstance(granularity, list):
#      granularity = [granularity]
#    if not isinstance(severity, list):
#      severity = [severity]
#
#    paramNames = ['Granularity', 'Name', 'Severity', 'When']
#
#    req = "SELECT Granularity, Name, Reason FROM PolicyRes WHERE "
#    req = req + "PolicyName LIKE 'DT_%' AND Reason LIKE \'%found%\' "
#    req = req + "AND Granularity in (%s)" %(','.join(['"'+x.strip()+'"' for x in granularity]))
#    resQuery = self.db._query(req)
#    if not resQuery['OK']:
#      raise RSSManagementDBException, resQuery['Message']
#    if not resQuery['Value']:
#      records = []
#    else:
#      resQuery = resQuery['Value']
#      records = []
#      for tuple_ in resQuery:
#        sev = tuple_[2].split()[2]
#        if sev not in severity:
#          continue
#        when = tuple_[2].split(sev)[1][1:]
#        if when == '':
#          when = 'Ongoing'
#        records.append([tuple_[0], tuple_[1], sev, when])
#
#    finalDict = {}
#    finalDict['TotalRecords'] = len(records)
#    finalDict['ParameterNames'] = paramNames
#
#    # Return all the records if maxItems == 0 or the specified number otherwise
#    if maxItems:
#      finalDict['Records'] = records[startItem:startItem+maxItems]
#    else:
#      finalDict['Records'] = records
#
#    finalDict['Extras'] = None
#
#    return finalDict
#
################################################################################
'''