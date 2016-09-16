"""
This class a wrapper around elasticsearch-py. It is used to query
Elasticsearch database.

"""

from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from elasticsearch.exceptions import ConnectionError, TransportError, NotFoundError
from elasticsearch.helpers import BulkIndexError
from elasticsearch import helpers

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities import DErrno

__RCSID__ = "$Id$"

class ElasticSearchDB( object ):
  
  """
  .. class:: ElasticSearchDB

  :param str url: the url to the database for example: el.cern.ch:9200
  :param str gDebugFile: is used to save the debug information to a file
  :param int timeout the default time out to Elasticsearch
  """
  __chunk_size = 1000
  __url = ""
  __timeout = 120
  clusterName = ''  
  ########################################################################
  def __init__( self, host, port ):
    """ c'tor
    :param self: self reference
    :param str host: name of the database for example: MonitoringDB
    :param str port: The full name of the database for example: 'Monitoring/MonitoringDB'
    :param bool debug: save the debug information to a file   
    """
    self._connected = False
    self.__url = "%s:%d" % ( host, port )
    self.__client = Elasticsearch( self.__url, timeout = self.__timeout )
    self.__tryToConnect()
      
  ########################################################################  
  def query( self, index, query ):
    """It exexutes a query and it returns the result
    query is a dictionary. More info: search for elasticsearch dsl
    
    :param self: self reference
    :param dict query: It is the query in ElasticSerach DSL language
     
    """
    return self.__client.search( index = index, body = query )
  
  def _Search( self, indexname ):
    """
    it returns the object which can be used for reatriving ceratin value from the DB
    """
    return  Search( using = self.__client, index = indexname )
  
  ########################################################################
  def _Q( self, name_or_query = 'match', **params ):
    """
    It is a wrapper to ElasticDSL Query module used to create a query object. 
    :param str name_or_query is the type of the query
    """
    return Q( name_or_query, **params )
  
  def _A( self, name_or_agg, aggsfilter = None, **params ):
    """
    It us a wrapper to ElasticDSL aggregation module, used to create an aggregation
    """
    return A( name_or_agg, aggsfilter, **params )
  ########################################################################
  def __tryToConnect( self ):
    """Before we use the database we try to connect and retrive the cluster name
    
    :param self: self reference
         
    """
    try:
      if self.__client.ping():
        # Returns True if the cluster is running, False otherwise
        result = self.__client.info()
        self.clusterName = result.get( "cluster_name", " " )
        gLogger.info( "Database info", result )
        self._connected = True
      else:
        self._connected = False
        gLogger.error( "Cannot connect to the database!" )
    except ConnectionError as e:
      gLogger.error( repr(e) )
      self._connected = False 

  ########################################################################
  def getIndexes( self ):
    """
    It returns the available indexes...
    """
    return [ index for index in self.__client.indices.get_aliases() ]
  
  ########################################################################
  def getDocTypes( self, indexName ):
    """
    :param str indexName is the number of the index...
    :return S_OK or S_ERROR 
    """
    result = []
    try:
      result = self.__client.indices.get_mapping( indexName )
    except Exception as e: # pylint: disable=broad-except
      gLogger.error( e )
    doctype = ''
    for indexConfig in result:
      if not result[indexConfig].get( 'mappings' ):
        # there is a case when the mapping exits and the value is None...
        # this is usually an empty index or a corrupted index.
        gLogger.warn( "Index does not have mapping %s!" % indexConfig )
        continue
      if result[indexConfig].get( 'mappings' ) :
        doctype = result[indexConfig]['mappings']
        break  # we supose the mapping of all indexes are the same...
      
    if not doctype:
      return S_ERROR( "%s does not exists!" % indexName )
      
    return S_OK( doctype ) 
  
  ########################################################################
  def exists( self, indexName ):
    """
    it checks the existance of an index
    :param str indexName: the name of the index
    """
    return self.__client.indices.exists( indexName )
  
  ########################################################################
  def _generateFullIndexName( self, indexName ):
    """
    Given an index prefix we create the actual index name. Each day an index is created.
    :param str indexName: it is the name of the index
    """
    today = datetime.today().strftime( "%Y-%m-%d" )
    return "%s-%s" % ( indexName, today )
  
  def createIndex( self, indexPrefix, mapping ):
    """
    :param str indexPrefix: it is the index name. 
    :param dict mapping: the configuration of the index.
    
    """
    result = None
    fullIndex = self._generateFullIndexName( indexPrefix )  # we have to create the an index in each day...
    if self.exists( fullIndex ):
      result = S_OK( fullIndex )
    else:
      try:
        gLogger.info( "Create index: ", fullIndex + str( mapping ) )
        self.__client.indices.create( fullIndex, body = {'mappings': mapping} )
        result = S_OK( fullIndex )
      except Exception as e: # pylint: disable=broad-except
        gLogger.error( "Can not create the index:", e )
        result = S_ERROR( e )
    return result
  
  def deleteIndex( self, indexName ):
    """
    :param str indexName the name of the index to be deleted...
    """
    try:
      retVal = self.__client.indices.delete( indexName )
    except  NotFoundError  as e:
      return S_ERROR( DErrno.EELNOFOUND, e )
    except ValueError as e:
      return S_ERROR( DErrno.EVALUE, e )
    
    if retVal.get( 'acknowledged' ): 
      #if the value exists and the value is not None
      return S_OK( indexName )
    else:
      return S_ERROR( retVal )
  
  def index( self, indexName, doc_type, body ):
    """
    :param str indexName the name of the index to be deleted...
    :param str doc_type the type of the document
    :param dict body the data which will be indexed
    :return the index name in case of success.
    """
    try:
      res = self.__client.index( index = indexName,
                                 doc_type = doc_type,
                                 body = body )
    except TransportError as e:
      return S_ERROR( e )
    
    if res.get( 'created' ):
      # the created is exists but the value can be None. 
      return S_OK( indexName )
    else:
      return S_ERROR( res )
    
  
  def bulk_index( self, indexprefix, doc_type, data, mapping = None ):
    """
    :param str indexPrefix: it is the index name. 
    :param str doc_type
    :param list data contains a list of dictionary 
    """
    gLogger.info( "%d records will be insert to %s" % ( len( data ), doc_type ) )
    if mapping is None:
      mapping = {}
      
    indexName = self._generateFullIndexName( indexprefix )
    gLogger.debug("inserting datat to %s index" % indexName)
    if not self.exists( indexName ):
      retVal = self.createIndex( indexprefix, mapping )
      if not retVal['OK']:
        return retVal
    docs = []
    for row in data:
      body = {
          '_index': indexName,
          '_type': doc_type,
          '_source': {}
      }
      body['_source'] = row
      try:
        body['_source']['timestamp'] = datetime.fromtimestamp( row.get( 'timestamp', int( Time.toEpoch() ) ) )
      except TypeError as e:
        body['_source']['timestamp'] = row.get( 'timestamp' )
      docs += [body]
    try:
      res = helpers.bulk( self.__client, docs, chunk_size = self.__chunk_size )
    except BulkIndexError as e:
      return S_ERROR( e )
    
    if res[0] == len( docs ):
      # we have inserted all documents...
      return S_OK( len( docs ) )
    else:
      return S_ERROR( res )
    return res
  
  def getUniqueValue( self, indexName, key, orderBy = False ):
    """
    :param str indexName the name of the index which will be used for the query
    :param dict orderBy it is a dictionary in case we want to order the result {key:'desc'} or {key:'asc'} 
    It returns a list of unique value for a certain key from the dictionary.
    """
    
    query = self._Search( indexName )
    if orderBy:
      query.aggs.bucket( key, 
                         'terms', 
                         field = key, 
                         size = 0, 
                         order = orderBy ).metric( key, 
                                                   'cardinality', 
                                                   field = key )
    else:
      query.aggs.bucket( key, 
                         'terms', 
                         field = key, 
                         size = 0 ).metric( key, 
                                            'cardinality', 
                                            field = key )
    
    try:
      result = query.execute()
    except TransportError as e:
      return S_ERROR( e )
    
    values = []
    for bucket in result.aggregations[key].buckets:
      values += [bucket['key']]
    del query
    return S_OK( values )
