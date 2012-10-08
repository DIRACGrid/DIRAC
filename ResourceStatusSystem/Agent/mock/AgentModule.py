from DIRAC import S_OK

class AgentModule:
  
  def __init__( self, agentName, loadName, baseAgentName = False, properties = {} ):
    pass
  
  def am_initialize( self, *initArgs ):
    return S_OK()
  
  def am_setOption( self, optionName, value ):
    return S_OK()
  
  def am_getOption( self, optionName, defaultValue = False ):
    return defaultValue

  def am_getWorkDirectory( self ):
    return ""
