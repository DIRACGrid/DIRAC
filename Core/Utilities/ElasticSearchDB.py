########################################################################
# $Id: $
########################################################################

"""
This class a wrapper around elasticsearch-py. It is used to query
Elasticsearch database.

"""

__RCSID__ = "$Id$"

from DIRAC                      import gLogger
from elasticsearch              import Elasticsearch
from elasticsearch.exceptions   import ConnectionError

class ElasticSearchDB( object ):
  
  """
  .. class:: ElasticSearchDB

  :param str url: the url to the database for example: el.cern.ch:9200
  :param str gDebugFile: is used to save the debug information to a file
  """
  
  __url = ""
  ########################################################################
  def __init__( self, host, port, debug = False ):
    """ c'tor
    :param self: self reference
    :param str host: name of the database for example: MonitoringDB
    :param str port: The full name of the database for example: 'Monitoring/MonitoringDB'
    :param bool debug: save the debug information to a file   
    """
    global gDebugFile
    
    if 'log' not in dir( self ):
      self.log = gLogger.getSubLogger( 'ElasticSearch' )
    self.logger = self.log
    
    self.__url = "http://%s:%s" % ( host, port )
        
    if debug:
      try:
        gDebugFile = open( "%s.debug.log" % self.__dbName, "w" )
      except IOError as e:
        self.log.error( e )
      
    self.client = Elasticsearch( self.__url )
    self.__tryToConnect()
  
  ########################################################################  
  def query( self, query ):
    """It exexutes a query and it returns the result
    query is a dictionary. More info: search for elasticsearch dsl
    
    :param self: self reference
    :param dict query: It is the query in ElasticSerach DSL language
     
    """
    return self.client.search( query )
  
  ########################################################################
  def __tryToConnect( self ):
    """Before we use the database we try to connect and retrive the cluster name
    
    :param self: self reference
         
    """
    try:
      if self.client.ping():
        result = self.client.info()
        self.setClusterName ( result.get( "cluster_name", " " ) )
        self.log.info( "Database info", result )
        self._connected = True
      else:
        self.log.error( "Cannot connect to the database!" )
    except ConnectionError as e:
      self.log.error( e )
      self._connected = False 

  ########################################################################
  def getIndexes(self):
    """
    It returns the available indexes...
    """
    return [ index for index in self.client.indices.get_aliases() ]
  
  ########################################################################
  def getDocTypes(self, indexes):
    try:
      result = self.client.indices.get_mapping(indexes)
    except Exception as e:
      print e
    doctype = ''
    for i in result:
      doctype = result[i]['mappings'].keys()[0]
      break
    return doctype 
  
  ########################################################################
  def checkIndex(self, indexName):
    """
    it checks the existance of an index
    :param str indexName: the name of the index
    """
    return self.client.indices.exists(indexName)