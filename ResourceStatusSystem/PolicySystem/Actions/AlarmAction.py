## $HeadURL $
#''' AlarmAction
#  
#  AlarmPolType Actions.
#  
#'''
#
#from DIRAC                                                      import gLogger
#from DIRAC.ResourceStatusSystem.Utilities                       import CS
#from DIRAC.ResourceStatusSystem.Utilities                       import Utils
#from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient
#from DIRAC.ResourceStatusSystem.PolicySystem.Actions.ActionBase import ActionBase
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
#from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
#
#__RCSID__  = '$Id: $'
#
#class AlarmAction( ActionBase ):
#  
#  def __init__(self, granularity, name, status_type, pdp_decision, **kw):
#    ActionBase.__init__( self, granularity, name, status_type, pdp_decision, **kw )
#
#    try:             self.rsClient = self.kw["Clients"][ 'ResourceStatusClient' ]
#    except KeyError: self.rsClient = ResourceStatusClient()
#    try:             self.rmClient = self.kw["Clients"][ 'ResourceManagementClient' ]
#    except KeyError: self.rmClient = ResourceManagementClient()
#
#  def _getUsersToNotify(self):
#    groups = CS.getTypedDictRootedAtOperations("AssigneeGroups/" + CS.getSetup()).values()
#    concerned_groups = [g for g in groups if Utils.dictMatch(self.kw["Params"], g)]
#    return [{'Users':g['Users'],
#             'Notifications':g['Notifications']} for g in concerned_groups]
#
#  def run(self):
#    """ Do actions required to notify users.
#    Mandatory keyword arguments:
#    - Granularity
#    Optional keyword arguments:
#    - SiteType
#    - ServiceType
#    - ResourceType
#    """
#
#    # Initializing variables
#    nc = NotificationClient()
#
#    # raise alarms, right now makes a simple notification
#
#    if 'Granularity' not in self.kw['Params'].keys():
#      raise ValueError, "You have to provide a argument Granularity = <desired_granularity>"
#
#    if self.new_status['Action']:
#
#      notif = "%s %s is perceived as" % (self.granularity, self.name)
#      notif = notif + " %s. Reason: %s." % (self.new_status['Status'], self.new_status['Reason'])
#
#      users_to_notify = self._getUsersToNotify()
#
#      for notif in users_to_notify:
#        for user in notif['Users']:
#          if 'Web' in notif['Notifications']:
#            gLogger.info("Sending web notification to user %s" % user)
#            nc.addNotificationForUser(user, notif)
#          if 'Mail' in notif['Notifications']:
#            gLogger.info("Sending mail notification to user %s" % user)
#            
#            was = self.rsClient.getElementHistory( self.granularity, 
#                                                   elementName = self.name,
#                                                   statusType = self.status_type,
#                                                   meta = {"order": "DESC", 'limit' : 1,
#                                                    "columns":  ['Status', 'Reason', 'DateEffective']})#[0]
#            
#            if not was[ 'OK' ]:
#              gLogger.error( was[ 'Message' ] )
#              continue
#            was = was[ 'Value' ][ 0 ]
#
#            mailMessage = """                             ---TESTING---
#--------------------------------------------------------------------------------
#            
#RSS changed the status of the following resource:
#
#Granularity:\t%s
#Name:\t\t%s
#New status:\t%s
#Reason:\t\t%s
#Was:\t\t%s (%s) since %s
#Setup:\t\t%s
#
#If you think RSS took the wrong decision, please set the status manually:
#
#Use: dirac-rss-set-status -g <granularity> -n <element_name> -s <desired_status> [-t status_type]
#(if you omit the optional last part of the command, all status types are matched.)
#
#This notification has been sent according to those parameters:
#%s
#""" % (self.granularity, self.name, self.new_status['Status'],
#       self.new_status['Reason'], was[0], was[1], was[2], CS.getSetup(), str(users_to_notify))
#
#            # Actually send the mail!
#            
#            resUser = self.rmClient.getUserRegistryCache( user )
#            if not resUser[ 'OK' ]:
#              gLogger.error( resUser[ 'Message' ] )
#              continue
#            resUser = resUser[ 'Value' ][ 0 ][ 2 ]
#                       
#            nc.sendMail(resUser,
#                        '[RSS][%s][%s] %s -> %s'
#                        % (self.granularity, self.name,  self.new_status['Status'], was[0]), mailMessage)
#
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF