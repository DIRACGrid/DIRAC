########################################################################
# $Id: $
########################################################################

"""
It is a wrapper on top of Elasticsearch. It is used to manage the DIRAC monitoring types. 
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC.MonitoringSystem.private.TypeLoader import TypeLoader
from DIRAC import S_OK, S_ERROR, gConfig

########################################################################
class MonitoringDB( ElasticDB ):

  def __init__( self, name = 'Monitoring/MonitoringDB' ):
    ElasticDB.__init__( self, 'MonitoringDB', name )
    self.__documents = {}
    self.__loadIndexes()
    
  def __loadIndexes( self ):
    """
    It loads all monitoring indexes and types.
    """
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't get a list of setups: %s" % retVal[ 'Message' ] )
    
    setupsList = retVal[ 'Value' ]
    objectsLoaded = TypeLoader().getTypes()

    # Load the files
    for pythonClassName in sorted( objectsLoaded ):
      typeClass = objectsLoaded[ pythonClassName ]
      for setup in setupsList:
        indexName = "%s_%s" % ( setup, typeClass().getIndex() )
        doc_type = "%s_%s" % ( setup, typeClass().getDocType() )
        self.__documents[doc_type] = indexName
        self.registerType( doc_type, indexName )
  
  def registerType(self, type, index):
    """
    It register the type and index, if does not exists
    :param str type: Type of the documents
    :param str index: name of the index
    """ 
    print "III", index
    all_index = "%s-*" % index
    print all_index
    if self.checkIndex(all_index):  
      indexes = self.getIndexes()
      for i in indexes:
        print 'DSDS', indexes
        print 'index', index
        print self.getDocTypes(index)
    else:
      print 'nem letezik!!!' 
                                                         
    
