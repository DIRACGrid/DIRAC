"""
  This module contains methods for the validation of production definitions

"""

__RCSID__ = "$Id $"


# # imports
import json

# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog


class ProdValidator( object ):

  def __init__( self ):
    self.transClient = TransformationClient()
    self.fc = FileCatalog()

  def checkTransStatus( self, transID ):
    res = self.transClient.getTransformationParameters( transID, 'Status' )
    if not res['OK']:
      return res
    status = res['Value']
    if status != 'New':
      return S_ERROR("checkTransStatus failed. Invalid transformation status: %s:" % status  )

    return S_OK()


  def checkTransDependency( self, transID, parentTransID ):
    """ check if the transformation and the parent transformation are linked """
    res = self.transClient.getTransformationParameters( transID, 'InputMetaQuery' )
    if not res['OK']:
      return res
    inputquery = res['Value']
    if not inputquery:
      return S_ERROR("No InputMetaQuery defined for transformation %s" % transID )

    res = self.transClient.getTransformationParameters( parentTransID, 'OutputMetaQuery' )
    if not res['OK']:
      return res
    parentoutputquery = res['Value']
    if not parentoutputquery:
      return S_ERROR("No OutputMetaQuery defined for parent transformation %s" % parentTransID )

    mq = MetaQuery( json.loads(inputquery) )
    parentMq = MetaQuery( json.loads(parentoutputquery) )

    ### Check the matching between inputquery and parent outputmeta query
    ### Currently very simplistic: just support expression with "=" and "in" operators
    gLogger.notice( "Applying checkMatchQuery" )
    res = self.checkMatchQuery( mq, parentMq )

    if not res['OK']:
      gLogger.error( "checkMatchQuery failed" )
      return res
    if not res['Value']:
      return S_ERROR("checkMatchQuery result is False" )

    return S_OK()

  def checkMatchQuery( self, mq, mqParent ):
    """  Check the logical intersection between the two metaqueries
    """
    ### Get the query dict
    MetaQueryDict = mq.getMetaQuery()
    ParentMetaQueryDict = mqParent.getMetaQuery()

    ### Get the metadata types defined in the catalog
    res = self.fc.getMetadataFields()
    if not res['OK']:
      gLogger.error( "Error in getMetadataFields: %s" % res['Message'] )
      return res
    if not res['Value']:
      gLogger.error( "Error: no metadata fields defined" )
      return res

    MetaTypeDict = res['Value']['FileMetaFields']
    MetaTypeDict.update( res['Value']['DirectoryMetaFields'] )

    def checkformatQuery( MetaQueryDict ):
      '''Check format query and transform all dict values in dict for uniform treatement'''
      for meta, value in MetaQueryDict.items():
        values = []
        if isinstance( value, dict ):
          operation = value.keys()[0]
          if operation not in ['=','in']:
            msg = 'Operation %s is not supported' % operation
            return S_ERROR( msg )
        else:
          values.append( value )
          MetaQueryDict[meta] = {"in":values}

      return S_OK( MetaQueryDict )


    def compareValues( value, parentvalue ):
      if set(value.values()[0]).issubset(set(parentvalue.values()[0])) or set(parentvalue.values()[0]).issubset(set(value.values()[0])):
        return True
      else:
        return False

    res = checkformatQuery( MetaQueryDict )
    if not res['OK']:
      return res
    MetaQueryDict = res['Value']

    res = checkformatQuery( ParentMetaQueryDict )
    if not res['OK']:
      return res
    ParentMetaQueryDict = res['Value']

    for meta, value in MetaQueryDict.items():
      mtype = MetaTypeDict[meta]
      if mtype not in ['VARCHAR(128)','int','float']:
        msg = 'Metatype %s is not supported' % mtype
        return S_ERROR( msg )
      if meta not in ParentMetaQueryDict:
        msg = 'Metadata %s not in parent transformation query' % meta
        return S_ERROR( msg )
      if compareValues(value,ParentMetaQueryDict[meta]):
        continue
      else:
        msg = "Metadata values %s do not match with %s" % (value,ParentMetaQueryDict[meta])
        gLogger.error( msg )
        return S_OK( False )

    return S_OK( True )







