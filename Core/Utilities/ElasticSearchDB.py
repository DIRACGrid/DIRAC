__RCSID__ = "$Id$"

"""
This class a wrapper around elasticsearch-py. It is used to query
Elasticsearch database.

"""

from elasticsearch import Elasticsearch

class ElasticSearchDB:
  
  def __init__( self, url = None ):
    """
     The elasticsearch client is used to execute queries.
    """
    self.client = Elasticsearch( url )
    
  def query( self, query ):
    """It exexutes a query and it returns the result
    query is a dictionary. More info: search for elasticsearch dsl
    """
    return self.client.search( query )
    
