""" ResourcePolType Actions
"""

def ResourcePolTypeActions( granularity, name, statusType, resDecisions, rsClient, rmDB ):
  """
  Action that updates the RSS DBs. Fields are:
  - granularity  : Granularity of the resource that have been tested
  - name         : Name of the resource that have been tested
  - statusType   : Class of status of the resource that have been tested (NEW)
  - resDecisions : Dict {PolicyCombinedResult:..., SinglePolicyResults:...}
  - rsClient     :
  - rmDB         :
  """

  res = resDecisions['PolicyCombinedResult']

  token = 'RS_SVC'

  if res['Action']:

    if res['Status'] == 'Probing':
      token = 'RS_Hold'

    if granularity == 'Site':
      rsClient.setSiteStatus( name, statusType, res['Status'], res['Reason'], token )
      #rsDB.setMonitoredToBeChecked( ['Service', 'Resource', 'StorageElement'], granularity, name )

    elif granularity == 'Service':
      rsClient.setServiceStatus( name, statusType, res['Status'], res['Reason'], token )
      #rsDB.setMonitoredToBeChecked( ['Site', 'Resource', 'StorageElement'], granularity, name )

    elif granularity == 'Resource':
      rsClient.setResourceStatus( name, statusType, res['Status'], res['Reason'], token )
      #rsDB.setMonitoredToBeChecked( ['Site', 'Service', 'StorageElement'], granularity, name )

    elif granularity == 'StorageElement':
      rsClient.setStorageElementStatus( name, statusType, res['Status'], res['Reason'], token )
      #rsDB.setMonitoredToBeChecked( ['Site', 'Service', 'Resource'], granularity, name )

  else:
    rsClient.setReason( granularity, name, statusType, res['Reason'] )#, 'RS_SVC' )
    #rsDB.setLastMonitoredCheckTime( granularity, name, statusType )

  for resP in resDecisions['SinglePolicyResults']:
    if not resP.has_key( 'OLD' ):
      rmDB.addOrModifyPolicyRes( granularity, name,
                                 resP['PolicyName'], resP['Status'], resP['Reason'] )

  if res.has_key( 'EndDate' ):
    rsClient.setDateEnd( granularity, name, res['EndDate'] )
