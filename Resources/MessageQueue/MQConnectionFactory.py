"""  The MQ creates MQConnection objects
"""
from DIRAC                 import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities  import ObjectLoader

__RCSID__ = "$Id$"

def getMQueue( queueName ):

  mqService = None
  elements = queueName.split( '::' )
  if len( elements ) == 2:
    mqService, queue = elements
  else:
    queue = queueName


  result = gConfig.getSections( '/Resources/MQServices' )
  if not result['OK']:
    return result
  sections = result['Value']
  if mqService and not mqService in sections:
    return S_ERROR( 'Requested MQService %s not found in the CS' % mqService )
  elif not mqService and len( sections ) == 1:
    mqService = sections[0]

  queuePath = ''
  servicePath = ''
  if mqService:
    servicePath = '/Resources/MQServices/%s' % mqService
    result = gConfig.getSections( '/Resources/MQServices/%s/Queues' % mqService )
    if result['OK'] and queue in result['Value']:
      queuePath = '/Resources/MQServices/%s/Queues/%s' % ( mqService, queue )
  else:
    for section in sections:
      result = gConfig.getSections( '/Resources/MQServices/%s/Queues' % section )
      if result['OK']:
        if queue in result['Value']:
          if queuePath:
            return S_ERROR( 'Ambiguous queue %s definition' % queue )
          else:
            servicePath = '/Resources/MQServices/%s' % section
            queuePath = '/Resources/MQServices/%s/Queues/%s' % ( section, queue )

  result = gConfig.getOptionsDict( servicePath )
  if not result['OK']:
    return result
  serviceDict = result['Value']

  if queuePath:
    result = gConfig.getOptionsDict( queuePath )
    if not result['OK']:
      return result
    serviceDict.update( result['Value'] )
  serviceDict['Queue'] = queue

  return S_OK( serviceDict )

class MQConnectionFactory( object ):

  #############################################################################
  def __init__(self, mqType=''):
    """ Standard constructor
    """
    self.mqType = mqType
    self.log = gLogger.getSubLogger( self.mqType )

  #############################################################################
  def __getMQConnection( self, queueName = None, parameters = {} ):
    """This method returns the CE instance corresponding to the supplied
       CEUniqueID.  If no corresponding CE is available, this is indicated.
    """

    queueParameters = {}
    if queueName is not None:
      result = getMQueue( queueName )
      if not result['OK']:
        return result
      queueParameters = result['Value']
    if parameters:
      queueParameters.update( parameters )

    mqType = queueParameters.get( 'MQType' )
    if not mqType:
      mqType = self.mqType
    if not mqType:
      return S_ERROR( 'No MQType specified' )

    subClassName = mqType + 'MQConnection'
    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject( 'Resources.MessageQueue.%s' % subClassName, subClassName )
    if not result['OK']:
      self.log.error( 'Failed to load object', '%s: %s' % ( subClassName, result['Message'] ) )
      return result

    ceClass = result['Value']
    try:
      mqConnection = ceClass()
      mqConnection.setParameters( queueParameters )

    except Exception as x:
      msg = 'MQConnectionFactory could not instantiate %s object: %s' % ( subClassName, str( x ) )
      self.log.exception()
      self.log.warn( msg )
      return S_ERROR( msg )

    return S_OK( mqConnection )

  def getMQListener( self, queueName = None, parameters = {}, messageCallback = None ):

    result = self.__getMQConnection( queueName = queueName,
                                     parameters = parameters )
    if not result['OK']:
      return result

    mqConnection = result['Value']
    result = mqConnection.setupConnection( receive = True, messageCallback = messageCallback )
    if not result['OK']:
      return result

    return S_OK( mqConnection )

  def getMQPublisher( self, queueName = None, parameters = {} ):

    result = self.__getMQConnection( queueName = queueName,
                                     parameters = parameters )
    if not result['OK']:
      return result

    mqConnection = result['Value']
    result = mqConnection.setupConnection()
    if not result['OK']:
      return result

    return S_OK( mqConnection )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
