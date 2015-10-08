########################################################################
# $Id: $
########################################################################

""" ElasticDB is a base class used to connect an Elasticsearch database and manages 
queries.
"""

__RCSID__ = "$Id$"

from DIRAC                                       import gLogger, gConfig
from DIRAC.Core.Utilities.ElasticSearchDB        import ElasticSearchDB
from DIRAC.ConfigurationSystem.Client.Utilities  import getElasticDBParameters

class ElasticDB( ElasticSearchDB ):

  """
  .. class:: ElasticDB

  :param str dbHost: the host name of the Elasticsearch database
  :param str dbPort: The port where the Elasticsearch database is listening
  :param str clusterName: The name of the cluster.
  """
  __dbHost = None
  __dbPort = None
  __clusterName = ""
  
  ########################################################################
  def __init__( self, dbname, fullName, debug = False ):
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
    
    ElasticSearchDB.__init__( self, self.__dbHost, self.__dbPort, debug = debug )

    if not self._connected:
      raise RuntimeError( 'Can not connect to DB %s, exiting...' % self.dbName )


    self.log.info( "==================================================" )
    self.log.info( "Host:           " + self.__dbHost )
    self.log.info( "Port:           " + str( self.__dbPort ) )
    self.log.info( "ClusterName:    " + self.__clusterName )
    self.log.info( "==================================================" )

  ########################################################################
  def setClusterName( self, name ):
    """
      It is used to set the cluster name
      
      :param self: self reference
      :param str requestName: request name
    
    """
    self.__clusterName = name
  
  ########################################################################
  def getClusterName( self ):
    """
    It returns the cluster name
    
    :param self: self reference
    """
    return self.__clusterName
  
  ########################################################################
  def setDbHost( self, hostName ):
    """
     It is used to set the cluster host
      
      :param self: self reference
      :param str requestName: request name
    """
    self.__dbHost = hostName
    
  ########################################################################
  def getDbHost( self ):
    """
     It returns the elasticsearch database host     
    :param self: self reference
    """
    return self.__dbHost
  
  ########################################################################
  def setDbPort( self, port ):
    """
     It is used to set the cluster port
      
      :param self: self reference
      :param str requestName: request name
    """
    self.__dbPort = port
  
  ########################################################################
  def getDbPort( self ):
    """
       It returns the database port
    
      :param self: self reference
    """
    return self.__dbPort  
