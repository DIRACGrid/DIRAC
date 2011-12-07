################################################################################
# $HeadURL $
################################################################################
"""
  ResourcePolType Actions
"""

__RCSID__  = "$Id$"


from datetime import datetime

################################################################################

def ResourcePolTypeActions( granularity, name, statusType, resDecisions, rsAPI, rmAPI ):
  """
  Action that updates the RSS DBs. Fields are:
  - granularity  : Granularity of the resource that have been tested
  - name         : Name of the resource that have been tested
  - statusType   : Class of status of the resource that have been tested (NEW)
  - resDecisions : Dict {PolicyCombinedResult:..., SinglePolicyResults:...}
  - rsAPI        :
  - rmAPI        :
  """

  res = resDecisions['PolicyCombinedResult']

  token = 'RS_SVC'

  if res['Action']:

    ## If HammerCloud in place, or other instrument to get out of the
    ## Probing state, you can uncomment this:

    # if res['Status'] == 'Probing':
    #   token = 'RS_Hold'
    ####################################################################

    rsAPI.modifyElementStatus( granularity, name, statusType,
                               status=res[ 'Status' ], reason=res[ 'Reason' ],
                               tokenOwner=token)

  else:
    rsAPI.setReason( granularity, name, statusType, res['Reason'] )

  now = datetime.utcnow()

  for resP in resDecisions['SinglePolicyResults']:
    if not resP.has_key( 'OLD' ):
      rmAPI.addOrModifyPolicyResult( granularity, name, resP['PolicyName'], statusType, resP['Status'], resP['Reason'], now, now )

  if res.has_key( 'EndDate' ):
    rsAPI.setDateEnd( granularity, name, statusType, res['EndDate'] )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
