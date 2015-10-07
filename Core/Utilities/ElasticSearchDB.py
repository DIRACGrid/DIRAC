__RCSID__ = "$Id$"

"""
This class a wrapper around elasticsearch-py. It is used to query
Elasticsearch database.

"""

from DIRAC                      import gLogger
from elasticsearch              import Elasticsearch

class ElasticSearchDB:
  
  def __init__( self, host, port, debug = False ):
    """
     The elasticsearch client is used to execute queries.
    """
    global gDebugFile
    
    if 'log' not in dir( self ):
      self.log = gLogger.getSubLogger( 'ElasticSearch' )
    self.logger = self.log
    
    self.__url = "http://%s:%s" % (host, port)
    
    
    if debug:
      try:
        gDebugFile = open( "%s.debug.log" % self.__dbName, "w" )
      except IOError:
        pass
      
    self.client = Elasticsearch( self.__url )
    
  def query( self, query ):
    """It exexutes a query and it returns the result
    query is a dictionary. More info: search for elasticsearch dsl
    """
    return self.client.search( query )
    
