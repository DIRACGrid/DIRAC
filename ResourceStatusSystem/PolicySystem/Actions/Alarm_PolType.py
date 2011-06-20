"""
AlarmPolType Actions
"""

import copy

from DIRAC.ResourceStatusSystem.Policy.Configurations import AssigneeGroups
from DIRAC.ResourceStatusSystem.Utilities.CS import getMailForUser

#configModule = __import__( "LHCbDIRAC.ResourceStatusSystem.Policy.Configurations",
#                           globals(), locals(), ['*'] )
#AssigneeGroups = copy.deepcopy( configModule.AssigneeGroups )

def getUsersToNotify( granularity, setup, siteType = None, serviceType = None,
                     resourceType = None ):

  NOTIF = []

  for ag in AssigneeGroups.keys():

    if setup in AssigneeGroups[ag][ 'Setup' ] \
          and granularity in AssigneeGroups[ ag ][ 'Granularity' ]:
        
      if siteType is not None and siteType not in AssigneeGroups[ ag ][ 'SiteType' ]:
        continue
    
      if serviceType is not None and serviceType not in AssigneeGroups[ ag ][ 'ServiceType' ]:
        continue
    
      if resourceType is not None and resourceType not in AssigneeGroups[ ag ][ 'ResourceType' ]:
        continue
    
      NOTIF.append( { 'Users': AssigneeGroups[ ag ][ 'Users' ],
                      'Notifications': AssigneeGroups[ ag ][ 'Notifications' ] } )

  return NOTIF

def AlarmPolTypeActions( granularity, name, siteType, serviceType, resourceType, res, nc, setup, rsDB ):
  # raise alarms, right now makes a simple notification

  if res[ 'Action' ]:

    notif = "%s %s is perceived as" % ( granularity, name )
    notif = notif + " %s. Reason: %s." % ( res[ 'Status' ], res[ 'Reason' ] )

    NOTIF_D = getUsersToNotify( granularity,
                                      setup, siteType,
                                      serviceType,
                                      resourceType )

    for notification in NOTIF_D:
      for user in notification[ 'Users' ]:
          
        if 'Web' in notification[ 'Notifications' ]:
          nc.addNotificationForUser( user, notif )
          
        if 'Mail' in notification[ 'Notifications' ]:
          mailMessage = "Granularity = %s \n" % granularity
          mailMessage = mailMessage + "Name = %s\n" % name
          mailMessage = mailMessage + "New perceived status = %s\n" % res[ 'Status' ]
          mailMessage = mailMessage + "Reason for status change = %s\n" % res[ 'Reason' ]

          was = rsDB.getMonitoredsHistory( granularity,
                                           [ 'Status', 'Reason', 'DateEffective' ],
                                           name, False, 'DESC', 1 )[ 0 ]

          mailMessage = mailMessage + "Was in status \"%s\", " % was[ 0 ]
          mailMessage = mailMessage + "with reason \"%s\", since %s\n" % ( was[ 1 ], was[ 2 ] )

          mailMessage = mailMessage + "Setup = %s\n" % setup

          nc.sendMail( getMailForUser( user )[ 'Value' ][ 0 ],
                       '%s: %s' % ( name, res[ 'Status' ] ), mailMessage )
