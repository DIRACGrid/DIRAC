""" Utilities for the MessageQueue package
"""

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.CSAPI       import CSAPI

__RCSID__ = "$Id$"

def getMQParamsFromCS( destinationName ):
  """ Get parameter of a MQ destination (queue/topic) from the CS

  :param str destinationName: name of the queue/topic either just the queue/topic name, in this case
                       the default MQServer will be used, or in th form <MQServer>::<destinationName>
  :return: S_OK( parameterDict )/ S_ERROR
  """

  # API initialization is required to get an up-to-date configuration from the CS
  csAPI = CSAPI()
  csAPI.initialize()

  mqService = ''
  elements = destinationName.split( '::' )
  if len( elements ) == 2:
    mqService, queue = elements
  else:
    queue = destinationName

  # get both queues and topics
  print "this is mqService:" + mqService
  result = gConfig.getConfigurationTree( '/Resources/MQServices', mqService, queue )
  if not result['OK'] or len( result['Value'] ) == 0:
    return S_ERROR( 'Requested MQService or queue/topic not found in the CS: %s::%s' % ( mqService, queue ) )

  queuePath = None
  for path, value in result['Value'].iteritems():

    # check section paths for duplicate names
    # endswith() guarantees that similar queue names are discarded
    if not value and path.endswith( queue ):
      if queuePath:
        return S_ERROR( 'Ambiguous queue/topic %s definition' % queue )
      else:
        queuePath = path

  # set-up internal parameter depending on the destination type
  tmp = queuePath.split( 'Queues' )[0].split( 'Topics' )
  servicePath = tmp[0]

  serviceDict = {}
  if len(tmp) > 1:
    serviceDict['Topic'] = queue
  else:
    serviceDict['Queue'] = queue

  result = gConfig.getOptionsDict( servicePath )
  if not result['OK']:
    return result
  serviceDict.update( result['Value'] )

  result = gConfig.getOptionsDict( queuePath )
  if not result['OK']:
    return result
  serviceDict.update( result['Value'] )

  return S_OK( serviceDict )
