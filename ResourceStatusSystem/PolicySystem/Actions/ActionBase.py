# $HeadURL $
''' ActionBase
  
  Base class for Actions.
  
'''

__RCSID__  = '$Id: $'

class ActionBase( object ):
  
  def __init__(self, granularity, name, status_type, pdp_decision, **kw):
    """Base class for actions. Arguments are:
    - granularity : string
    - name        : string
    - status_type : string, type of status
    - pdp_decsion : dict, result of pdp.takeDecision() function
    - kw : dict, optional keywords arguments relevant to a specific
      test
    """
    self.granularity  = granularity
    self.name         = name
    self.status_type  = status_type
    self.pdp_decision = pdp_decision
    self.new_status   = pdp_decision["PolicyCombinedResult"]
    self.kw           = kw

  def run(self):
    """To be overloaded"""
    pass

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF