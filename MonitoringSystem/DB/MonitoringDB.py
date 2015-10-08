"""
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base.ElasticDB import ElasticDB
from DIRAC.MonitoringSystem.private.TypeLoader import TypeLoader
from DIRAC import S_OK, S_ERROR, gConfig

class MonitoringDB( ElasticDB ):

  def __init__( self, name = 'Monitoring/MonitoringDB' ):
    ElasticDB.__init__( self, 'MonitoringDB', name )
    self.__loadIndexes()
    
  def __loadIndexes(self):
    print 'DSDHSJDGJSH'
    retVal = gConfig.getSections( "/DIRAC/Setups" )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't get a list of setups: %s" % retVal[ 'Message' ] )
    setupsList = retVal[ 'Value' ]
    objectsLoaded = TypeLoader().getTypes()
    print "DSDSDS", objectsLoaded

    
    #Load the files
    for pythonClassName in sorted( objectsLoaded ):
      typeClass = objectsLoaded[ pythonClassName ]
      for setup in setupsList:
        typeName = "%s_%s" % ( setup, pythonClassName )
        
        print typeName
        """
        typeDef = typeClass().getDefinition()
        #dbTypeName = "%s_%s" % ( setup, typeName )
        definitionKeyFields, definitionAccountingFields, bucketsLength = typeDef[1:]
        #If already defined check the similarities
        if typeName in self.dbCatalog:
          bucketsLength.sort()
          if bucketsLength != self.dbBucketsLength[ typeName ]:
            bucketsLength = self.dbBucketsLength[ typeName ]
            self.log.warn( "Bucket length has changed for type %s" % typeName )
          keyFields = [ f[0] for f in definitionKeyFields ]
          if keyFields != self.dbCatalog[ typeName ][ 'keys' ]:
            keyFields = self.dbCatalog[ typeName ][ 'keys' ]
            self.log.error( "Definition fields have changed", "Type %s" % typeName )
          valueFields = [ f[0] for f in definitionAccountingFields ]
          if valueFields != self.dbCatalog[ typeName ][ 'values' ]:
            valueFields = self.dbCatalog[ typeName ][ 'values' ]
            self.log.error( "Accountable fields have changed", "Type %s" % typeName )
        #Try to re register to check all the tables are there
        retVal = self.registerType( typeName, definitionKeyFields,
                                    definitionAccountingFields, bucketsLength )
        if not retVal[ 'OK' ]:
          self.log.error( "Can't register type", "%s: %s" % ( typeName, retVal[ 'Message' ] ) )
        #If it has been properly registered, update info
        elif retVal[ 'Value' ]:
          #Set the timespan
          self.dbCatalog[ typeName ][ 'dataTimespan' ] = typeClass().getDataTimespan()
          self.dbCatalog[ typeName ][ 'definition' ] = { 'keys' : definitionKeyFields,
                                                         'values' : definitionAccountingFields }
                                                         """
                                                         
    