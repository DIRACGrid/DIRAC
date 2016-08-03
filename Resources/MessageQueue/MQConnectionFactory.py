"""  The MQ Factory creates MQConnection objects
"""
from DIRAC                 import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities  import ObjectLoader
from DIRAC.Resources.MessageQueue.Utilities import getMQueue
from DIRAC.Core.Utilities.DErrno import EMQUKN

__RCSID__ = "$Id$"

class MQConnectionFactory( object ):

  #############################################################################
  def __init__(self, mqType=''):
    """ Standard constructor
    """
    self.mqType = mqType
    self.log = gLogger.getSubLogger( self.mqType )

  #############################################################################
  def __getMQConnection( self, queueName = None, parameters = None ):
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
    if parameters is not None:
      queueParameters.update( parameters )

    mqType = queueParameters.get( 'MQType' )
    if not mqType:
      mqType = self.mqType
    if not mqType:
      return S_ERROR( EMQUKN, 'No MQType specified' )

    subClassName = mqType + 'MQConnection'
    objectLoader = ObjectLoader.ObjectLoader()
    result = objectLoader.loadObject( 'Resources.MessageQueue.%s' % subClassName, subClassName )
    if not result['OK']:
      self.log.error( 'Failed to load object', '%s: %s' % ( subClassName, result['Message'] ) )
      return result

    ceClass = result['Value']
    try:
      mqConnection = ceClass()
      result = mqConnection.setParameters( queueParameters )
      if not result['OK']:
        return result

    except Exception as exc:
      msg = 'MQConnectionFactory could not instantiate %s object: %s' % ( subClassName, repr( exc ) )
      self.log.exception( 'Could not instantiate MQConnection object', IException = exc )
      self.log.warn( msg )
      return S_ERROR( EMQUKN, msg )

    return S_OK( mqConnection )

  def getMQListener( self, queueName = None, parameters = None ):
    """ Get a MQConnection object in a Listener mode without connection
        initialized

    :param str queueName: queueName
    :param dict parameters: MQ connection parameters
    :return: S_OK( MQconnectionObject )/ S_ERROR
    """
    return self.__getMQConnection( queueName = queueName,
                                   parameters = parameters )

  def getMQPublisher( self, queueName = None, parameters = None ):
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
