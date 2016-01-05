"""
Class and utilities for managing dynamic monitoring logs in Elasticsearch
"""

import datetime
from DIRAC.Core.Utilities.ElasticSearchDB import ElasticSearchDB
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class DynamicMonitoringDB( object ):
  """
  This class manages logs stored in Elasticsearch
  """

  def __init__( self, address, port ):
    self.esDB = ElasticSearchDB( address, port )
    self.docType = 'ComponentMonitoring'

  def _dictToQuery( self, matchFieldsDict ):
    """
    Transforms a given dictionary with the fields to match into a query dictionary
    Returns the query dictionary to be passed to the elasticsearch API library
    matchFieldsDict is the dictionary with the fields that the 'match' part of the query should contain
    """
    query = { 'bool': { 'must': [] } }
    for key in matchFieldsDict:
      query[ 'bool' ][ 'must' ].append( { 'match': { key : matchFieldsDict[ key ] } } )

    return query

  def insertLog( self, log ):
    """
    Creates a new log entry in ElasticSearch
    log should be a dictionary containing the logging information to be stored
    """
    return self.esDB.addToIndex( '%s_index' % self.docType.lower(), self.docType, log )

  def getLastLog( self, host, component, maxAge = 10 ):
    """
    Retrieves the last log for the given component
    host is the name of the host where the component is installed
    component is the name of the component in the form system_component
    maxAge tells the method how many days in the past to look for the log
    If after reaching maxAge days, no logs have been found, an error will be returned
    """
    date = datetime.datetime.utcnow()

    for i in range( maxAge ):
      indexName = '%s_index-%s' % ( self.docType.lower(), date.strftime( '%Y-%m-%d' ) )
      try:
        if self.esDB.checkIndex( indexName ):
          result = self.esDB.query( indexName, { 'query': \
            { 'bool' : { 'must': [ { 'match': { 'host': host } }, { 'match': { 'component': component } } ] } } \
            , 'sort': { 'timestamp': { 'order': 'desc' } }, 'size': 1 } )
          if len( result[ 'hits' ][ 'hits' ] ) >= 1:
            return S_OK( [ result[ 'hits' ][ 'hits' ][0] ] )
      except Exception, e:
        return S_ERROR( e )

    return S_ERROR( 'No logs for this component found' )

  def getLogHistory( self, host, component, size = 10 ):
    """
    Returns a list with the log history for a given component
    host is the name of the host where the component is installed
    component is the name of the component in the form system_component
    size indicates how many entries should be retrieved from the log
    """
    logs = []
    date = datetime.datetime.utcnow()

    while len( logs ) < size:
      indexName = '%s_index-%s' % ( self.docType.lower(), date.strftime( '%Y-%m-%d' ) )

      try:
        if self.esDB.checkIndex( indexName ):
          result = self.esDB.query( indexName, { 'query': \
            { 'bool' : { 'must': [ { 'match': { 'host': host } }, { 'match': { 'component': component } } ] } } \
            , 'sort': { 'timestamp': { 'order': 'desc' } }, 'size': size - len( logs ) } )
        else:
          break
      except Exception, e:
        return S_ERROR( e )

      logs = logs + result[ 'hits' ][ 'hits' ]
      date = date - datetime.timedelta( days = 1 )

    if len( logs ) > 0:
      return S_OK( logs )
    else:
      return S_ERROR( 'No logs found for the component' )

  def getLogsPeriod( self, host, component, initialDate = '', endDate = '' ):
    """
    Retrieves the history of logging entries for the given component during a given given time periodtime period
    initialDate and endDate are strings indicating the start and end of the time period, respectively and
    should be in the format 'DD/MM/YYYY hh:mm'
    """

    if not initialDate and not endDate:
      return self.getLogHistory( host, component, 10 )

    logs = []
    if initialDate:
      date1 = datetime.datetime.strptime( initialDate, '%d/%m/%Y %H:%M' )
    else:
      date1 = datetime.datetime.min
    if endDate:
      date2 = datetime.datetime.strptime( endDate, '%d/%m/%Y %H:%M' )
    else:
      date2 = datetime.datetime.utcnow()

    date = date2

    while date >= date1:
      indexName = '%s_index-%s' % ( self.docType.lower(), date.strftime( '%Y-%m-%d' ) )

      result = self.esDB.getDocCount( indexName )
      if not result[ 'OK' ]:
        return result
      nDocs = result[ 'Value' ]

      try:
        if self.esDB.checkIndex( indexName ):
          result = self.esDB.query( indexName, { 'query': { 'filtered': {
            'query': { 'bool' : { 'must': [ { 'match': { 'host': host } }, { 'match': { 'component': component } } ] } }, \
            'filter': { 'range': { 'timestamp': { 'from': date1, 'to': date2 } } } } }, \
            'sort': { 'timestamp': { 'order': 'desc' } }, 'size': nDocs } )
        else:
          break
      except Exception, e:
        return S_ERROR( e )

      logs = logs + result[ 'hits' ][ 'hits' ]
      date = date - datetime.timedelta( days = 1 )

    if len( logs ) > 0:
      return S_OK( logs )
    else:
      return S_ERROR( 'No logs found for the component' )

  def deleteLogs( self, matchFields ):
    """
    Deletes all the logs for the specified component regardless of date
    matchFields is a dictionary containing pairs key-value that should match in the logs to be deleted
    """
    try:
      self.esDB.deleteDocuments( { 'query': self._dictToQuery( matchFields ) } )
    except Exception, e:
      return S_ERROR( e )

    return S_OK( 'Logs deleted correctly' )

  def deleteLogsByDate( self, dates ):
    """
    Deletes all the indexes and their logs for the dates specified
    dates is a list of datetime objects with the dates from which logs are to be deleted
    """
    indexesToDelete = reduce( lambda x, y: [x] + [y], map( lambda x: '%s_index-%s' % ( self.docType.lower(), x.strftime( '%Y-%m-%d' ) ), dates ) )

    try:
      self.esDB.deleteIndexes( indexesToDelete )
    except Exception, e:
      return S_ERROR( e )

    return S_OK( 'Logs deleted correctly' )
