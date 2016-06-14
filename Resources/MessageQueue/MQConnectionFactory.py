"""  The MQ Factory creates MQConnection objects
"""
from DIRAC                 import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities  import ObjectLoader
from DIRAC.Resources.MessageQueue.Utilities import getMQueue

__RCSID__ = "$Id$"

class MQConnectionFactory( object ):

  #############################################################################
  def __init__(self, mqType=''):
    """ Standard constructor
    """
    self.mqType = mqType
    self.log = gLogger.getSubLogger( self.mqType )

  #############################################################################
  def __getMQConnection( self, queueName = None, parameters = {} ):
    """ This method returns the MQConnection instance corresponding to the parameters and queue

       :param str queueName: name of the queue. Can be provided as just queueName or <MQServer>::<queueName>
                             forms
       :param dict parameters: dictionary of connection parameters
       :return: S_OK(MQconnectionObject)/ S_ERROR
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

  def getMQListener( self, queueName = None, parameters = {} ):
    """ Get a MQConnection object in a Listener mode without connection
        initialized

    :param str queueName: queueName
    :param dict parameters: MQ connection parameters
    :return: S_OK( MQconnectionObject )/ S_ERROR
    """
    result = self.__getMQConnection( queueName = queueName,
                                     parameters = parameters )
    if not result['OK']:
      return result

    mqConnection = result['Value']
    return S_OK( mqConnection )

  def getMQPublisher( self, queueName = None, parameters = {} ):
    """ Get a MQConnection object in a Publisher mode

    :param str queueName: queueName
    :param dict parameters: MQ connection parameters
    :return: S_OK( MQconnectionObject )/ S_ERROR
    """

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
