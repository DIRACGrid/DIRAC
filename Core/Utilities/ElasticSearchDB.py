"""
This class a wrapper around elasticsearch-py. It is used to query
Elasticsearch database.

"""
import os
import tempfile

from datetime import datetime
from datetime import timedelta

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
from elasticsearch.exceptions import ConnectionError, TransportError, NotFoundError
from elasticsearch.helpers import BulkIndexError, bulk

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Security import Locations, X509Chain

__RCSID__ = "$Id$"

def getCert():
  """
  get the host certificate
  """
  cert = Locations.getHostCertificateAndKeyLocation()
  if cert:
    cert = cert[0]
  else:
    cert = "/opt/dirac/etc/grid-security/hostcert.pem"
  return cert

def generateCAFile():
  """
  Generate a single CA file with all the PEMs
  """
  caDir = Locations.getCAsLocation()
  for fn in ( os.path.join( os.path.dirname( caDir ), "cas.pem" ),
              os.path.join( os.path.dirname( getCert() ), "cas.pem" ),
              False ):
    if not fn:
      fn = tempfile.mkstemp( prefix = "cas.", suffix = ".pem" )[1]
    
    try:
      
      with open(fn, "w" ) as fd:
        for caFile in os.listdir( caDir ):
          caFile = os.path.join( caDir, caFile )
          result = X509Chain.X509Chain.instanceFromFile( caFile )
          if not result[ 'OK' ]:
            continue
          chain = result[ 'Value' ]
          expired = chain.hasExpired()
          if not expired[ 'OK' ] or expired[ 'Value' ]:
            continue
          fd.write( chain.dumpAllToString()[ 'Value' ] )
      
      gLogger.info( "CAs used from: %s" % str( fn ) )    
      return fn
    except IOError as err:
      gLogger.warn( err )
      
  return False

class ElasticSearchDB( object ):

  """
  .. class:: ElasticSearchDB

  :param str url: the url to the database for example: el.cern.ch:9200
  :param str gDebugFile: is used to save the debug information to a file
  :param int timeout: the default time out to Elasticsearch
  :param int RESULT_SIZE: The number of data points which will be returned by the query.
  """
  __chunk_size = 1000
  __url = ""
  __timeout = 120
  clusterName = ''
  RESULT_SIZE = 10000
  ########################################################################
  def __init__( self, host, port, user = None, password=None, indexPrefix = ''):
    """ c'tor
    :param self: self reference
    :param str host: name of the database for example: MonitoringDB
    :param str port: The full name of the database for example: 'Monitoring/MonitoringDB'
    :param bool debug: save the debug information to a file
    :param str user: user name to access the db
    :param str password: if the db is password protected we need to provide a password
    :param str indexPrefix: it is the indexPrefix used to get all indexes
    """
    self.__indexPrefix = indexPrefix
    self._connected = False
    if user and password:
      self.__url = "https://%s:%s@%s:%d" % ( user, password, host, port )
    else:
      self.__url = "%s:%d" % ( host, port )
    self.__client = Elasticsearch( self.__url, timeout = self.__timeout, use_ssl = True, verify_certs = True, ca_certs = generateCAFile() )
    self.__tryToConnect()

  def getIndexPrefix( self ):
    """
    It returns the DIRAC setup.
    """
    return self.__indexPrefix

  ########################################################################
  def query( self, index, query ):
    """It executes a query and it returns the result
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
    It is a wrapper to ElasticDSL aggregation module, used to create an aggregation
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
        self.clusterName = result.get( "cluster_name", " " ) #pylint: disable=no-member
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

    #we only return indexes which belong to a specific prefix for example 'lhcb-production' or 'dirac-production etc.
    return [ index for index in self.__client.indices.get_alias( "%s*" % self.__indexPrefix ) ]

  ########################################################################
  def getDocTypes( self, indexName ):
    """
    :param str indexName is the name of the index...
    :return S_OK or S_ERROR
    """
    result = []
    try:
      gLogger.debug( "Getting mappings for ", indexName )
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

  def createIndex( self, indexPrefix, mapping ):
    """
    :param str indexPrefix: it is the index name.
    :param dict mapping: the configuration of the index.

    """
    result = None
    fullIndex = generateFullIndexName( indexPrefix )  # we have to create an index each day...
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
    :param str indexName: the name of the index to be deleted...
    :param str doc_type: the type of the document
    :param dict body: the data which will be indexed
    :return: the index name in case of success.
    """
    try:
      res = self.__client.index( index = indexName,
                                 doc_type = doc_type,
                                 body = body )
    except TransportError as e:
      return S_ERROR( e )

    if res.get( 'created' ):  #pylint: disable=no-member
      # the created is exists but the value can be None.
      return S_OK( indexName )
    else:
      return S_ERROR( res )


  def bulk_index( self, indexprefix, doc_type, data, mapping = None ):
    """
    :param str indexPrefix: it is the index name.
    :param str doc_type: the type of the document
    :param data: contains a list of dictionary
    :type data: python:list
    """
    gLogger.info( "%d records will be insert to %s" % ( len( data ), doc_type ) )
    if mapping is None:
      mapping = {}

    indexName = generateFullIndexName( indexprefix )
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

      if 'timestamp' not in row:
        gLogger.warn( "timestamp is not given! Note: the actual time is used!" )

      timestamp = row.get( 'timestamp', int( Time.toEpoch() ) ) #if the timestamp is not provided, we use the current utc time.
      try:
        if isinstance(timestamp, datetime):
          body['_source']['timestamp'] = int( timestamp.strftime('%s') ) * 1000
        elif isinstance(timestamp, basestring):
          timeobj = datetime.strptime( timestamp, '%Y-%m-%d %H:%M:%S.%f' )
          body['_source']['timestamp'] = int( timeobj.strftime('%s') ) * 1000
        else: #we assume  the timestamp is an unix epoch time (integer).
          body['_source']['timestamp'] = timestamp  * 1000
      except (TypeError, ValueError) as e:
        # in case we are not able to convert the timestamp to epoch time....
        gLogger.error( "Wrong timestamp", e )
        body['_source']['timestamp'] = int( Time.toEpoch() ) * 1000
      docs += [body]
    try:
      res = bulk( self.__client, docs, chunk_size = self.__chunk_size )
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

    endDate = datetime.utcnow()

    startDate = endDate - timedelta( days = 30 )

    timeFilter = self._Q( 'range',
                          timestamp = {'lte':int(Time.toEpoch( endDate )) * 1000,
                                       'gte': int(Time.toEpoch( startDate )) * 1000, } )
    query = query.filter( 'bool', must = timeFilter )
    if orderBy:
      query.aggs.bucket( key,
                         'terms',
                         field = key,
                         size = self.RESULT_SIZE,
                         order = orderBy ).metric( key,
                                                   'cardinality',
                                                   field = key )
    else:
      query.aggs.bucket( key,
                         'terms',
                         field = key,
                         size = self.RESULT_SIZE ).metric( key,
                                            'cardinality',
                                            field = key )

    try:
      query = query.extra( size = self.RESULT_SIZE ) #do not need the raw data.
      gLogger.debug( "Query", query.to_dict() )
      result = query.execute()
    except TransportError as e:
      return S_ERROR( e )

    values = []
    for bucket in result.aggregations[key].buckets:
      values += [bucket['key']]
    del query
    gLogger.debug( "Nb of unique rows retrieved", len( values ) )
    return S_OK( values )


def generateFullIndexName( indexName ):
  """
  Given an index prefix we create the actual index name. Each day an index is created.
  :param str indexName: it is the name of the index
  """
  today = datetime.today().strftime( "%Y-%m-%d" )
  return "%s-%s" % ( indexName, today )
