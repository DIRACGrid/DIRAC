################################################################################
# $HeadURL $
################################################################################
"""
  ResourcePolType Actions
"""

__RCSID__  = "$Id$"

from DIRAC.ResourceStatusSystem.PolicySystem.Actions.ActionBase import ActionBase
from datetime import datetime

class ResourceAction(ActionBase):
  def run(self):
    token = 'RS_SVC'

    if self.new_status['Action']:

    ## If HammerCloud in place, or other instrument to get out of the
    ## Probing state, you can uncomment this:

    # if res['Status'] == 'Probing':
    #   token = 'RS_Hold'
    ####################################################################

      self.kw["rsClient"].modifyElementStatus(self.granularity, self.name, self.status_type,
                                           status = self.new_status[ 'Status' ],
                                           reason = self.new_status[ 'Reason' ],
                                           tokenOwner=token)

    else:
      self.kw["rsClient"].setReason( self.granularity, self.name,
                                  self.status_type, self.new_status['Reason'] )

    now = datetime.utcnow()

    for resP in self.pdp_decision['SinglePolicyResults']:
      if not resP.has_key( 'OLD' ):
        self.kw["rmClient"].addOrModifyPolicyResult( self.granularity, self.name,
                                                  resP['PolicyName'], self.status_type,
                                                  resP['Status'], resP['Reason'], now, now )

    if "EndDate" in self.new_status.keys():
      self.kw["rsClient"].setDateEnd( self.granularity, self.name,
                                   self.status_type, self.new_status['EndDate'] )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
