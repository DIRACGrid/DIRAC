# $HeadURL:  $
''' BaseAction
  
  Base class for Actions.
  
'''

from DIRAC import gLogger

__RCSID__  = '$Id:  $'

class BaseAction( object ):
  '''
    Base class for all actions. It defines a constructor an a run main method.
  '''
  
  def __init__( self, name, decissionParams, enforcementResult, singlePolicyResults, clients ):

    # enforcementResult supposed to look like:
    # { 
    #   'Status'        : <str>,
    #   'Reason'        : <str>, 
    #   'PolicyActions' : <list>,
    #   [ 'EndDate' : <str> ]
    # } 

    # decissionParams supposed to look like:
    # {
    #   'element'     : None,
    #   'name'        : None,
    #   'elementType' : None,
    #   'statusType'  : None,
    #   'status'      : None,
    #   'reason'      : None,
    #   'tokenOwner'  : None
    # }

    self.actionName          = name # 'BaseAction'
    self.decissionParams     = decissionParams
    self.enforcementResult   = enforcementResult
    self.singlePolicyResults = singlePolicyResults
    self.clients             = clients

  def run( self ):
    '''
      Method to be over written by the real actions
    '''
    
    gLogger.info( '%s: you may want to overwrite this method' % self.actionName )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF