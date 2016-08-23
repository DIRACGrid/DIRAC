########################################################################
# $Id: $
########################################################################

""" ElasticDB is a base class used to connect an Elasticsearch database and manages 
queries.
"""

from DIRAC import gLogger
from DIRAC.Core.Utilities.ElasticSearchDB import ElasticSearchDB
from DIRAC.ConfigurationSystem.Client.Utilities import getElasticDBParameters


__RCSID__ = "$Id$"

class ElasticDB( ElasticSearchDB ):

  """
  .. class:: ElasticDB

  :param str __dbHost: the host name of the Elasticsearch database
  :param str __dbPort: The port where the Elasticsearch database is listening
  :param str __clusterName: The name of the cluster.
  """
  
  ########################################################################
  def __init__( self, dbname, fullName ):
    """ c'tor

    :param self: self reference
    :param str dbName: name of the database for example: MonitoringDB
    :param str fullName: The full name of the database for example: 'Monitoring/MonitoringDB'
    :param bool debug: save the debug information to a file 
    """
    
    database_name = dbname
    self.log = gLogger.getSubLogger( database_name )

    result = getElasticDBParameters( fullName )
    if not result['OK'] :
      raise RuntimeError( 'Cannot get database parameters: %s' % result['Message'] )

    dbParameters = result[ 'Value' ]
    self.__dbHost = dbParameters[ 'Host' ]
    self.__dbPort = dbParameters[ 'Port' ]
    
    super( ElasticDB, self ).__init__( self.__dbHost, self.__dbPort )

    if not self._connected:
      raise RuntimeError( 'Can not connect to DB %s, exiting...' % self.dbName )


    self.log.info( "==================================================" )
    self.log.info( "Host:           " + self.__dbHost )
    self.log.info( "Port:           " + str( self.__dbPort ) )
    self.log.info( "ClusterName:    " + self.__clusterName )
    self.log.info( "==================================================" )
  
  ########################################################################
  def setDbHost( self, hostName ):
    """
     It is used to set the cluster host
      
      :param str hostname it is the host name of the elasticsearch
    """
    self.__dbHost = hostName
    
  ########################################################################
  def getDbHost( self ):
    """
     It returns the elasticsearch database host     
    """
    return self.__dbHost
  
  ########################################################################
  def setDbPort( self, port ):
    """
     It is used to set the cluster port
      
      :param self: self reference
      :param str port: the port of the elasticsearch.
    """
    self.__dbPort = port
  
  ########################################################################
  def getDbPort( self ):
    """
       It returns the database port
    
      :param self: self reference
    """
    return self.__dbPort  
