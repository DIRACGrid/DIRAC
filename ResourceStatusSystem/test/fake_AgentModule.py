################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

""" 
  fake AgentModule class. Every function can simply return S_OK()
"""

from DIRAC import S_OK

class AgentModule:
  
  def __init__( self, agentName, baseAgentName, properties = {} ):
    pass
  
  def am_initialize( self, *initArgs ):
    return S_OK()
  
  def am_setOption( self, optionName, value ):
    return S_OK()
  
  def am_getOption( self, optionName, defaultValue = False ):
    return 8

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  