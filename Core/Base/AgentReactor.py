
from DIRAC import S_OK, S_ERROR, gLogger

class AgentReactor:

  def __init__( self ):
    self.__agentModules = {}

  def loadAgentModules( self, modulesList ):
    for module in modulesList:
      result = self.loadAgentModule( module )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def loadAgentModule( self, fullName ):
    modList = fullName.split( "/" )
    if len( modList ) != 2:
      return S_ERROR( "Can't load %s: Invalid agent name" % ( fullName ) )
    gLogger.info( "Loading %s" % fullName )
    system, agentName = modList
    try:
      agentModule = __import__( 'DIRAC.%sSystem.Agent.%s' % ( system, agentName ),
                              globals(),
                              locals(), agentName )
      agentClass = getattr( agentModule, agentName )
      agent = agentClass( fullName )
    except Exception, e:
      gLogger.exception( "Can't load agent %s" % fullName )
      return S_ERROR( "Can't load agent %s: %s" % ( fullName, str(e) ) )
    self.__agentModules[ fullName ] = { 'instance' : agent,
                                        'class' : agentClass,
                                        'module' : agentModule }
    return S_OK()
