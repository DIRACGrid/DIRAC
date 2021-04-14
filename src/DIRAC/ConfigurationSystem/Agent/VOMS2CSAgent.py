"""
The VOMS2CSAgent extracts VO user information from VOMS servers.

It performs the following operations:

- Extracts user info from the VOMS server using its REST interface
- Finds user DN's not yet registered in the DIRAC Registry
- For each new DN it constructs a DIRAC login name by a best guess or using the nickname VOMS attribute
- Registers new users to the DIRAC Registry including group membership
- Updates information for already registered users, including group membership
- Sends report for performed operation to the VO administrator

The agent is performing its operations with credentials of the VO administrator as defined
in the ``/Registry/VO/<VO_name>`` configuration section.

The following options can be set for the VOMS2CSAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN VOMS2CSAgent
  :end-before: ##END
  :dedent: 2
  :caption: VOMS2CSAgent options

.. note:: The options *AutoAddUsers*, *AutoModifyUsers*, *AutoDeleteUsers* can be overridden by the
  corresponding options defined in the ``/Registry/VO/<VO_name>`` configuration section.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC import S_OK, gConfig, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption, getUserOption
from DIRAC.ConfigurationSystem.Client.VOMS2CSSynchronizer import VOMS2CSSynchronizer
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

__RCSID__ = "$Id$"


class VOMS2CSAgent(AgentModule):

  def __init__(self, *args, **kwargs):
    """ Defines default parameters
    """
    super(VOMS2CSAgent, self).__init__(*args, **kwargs)

    self.voList = ['any']
    self.dryRun = True

    self.autoAddUsers = True
    self.autoModifyUsers = True
    self.autoDeleteUsers = True
    self.detailedReport = True
    self.makeFCEntry = False
    self.autoLiftSuspendedStatus = True
    self.mailFrom = 'noreply@dirac.system'
    self.syncPluginName = None

  def initialize(self):
    """ Initialize the default parameters
    """

    self.dryRun = self.am_getOption('DryRun', self.dryRun)

    # # General agent options, can be overridden by VO options
    self.autoAddUsers = self.am_getOption('AutoAddUsers', self.autoAddUsers)
    self.autoModifyUsers = self.am_getOption('AutoModifyUsers', self.autoModifyUsers)
    self.autoDeleteUsers = self.am_getOption('AutoDeleteUsers', self.autoDeleteUsers)
    self.autoLiftSuspendedStatus = self.am_getOption('AutoLiftSuspendedStatus', self.autoLiftSuspendedStatus)
    self.makeFCEntry = self.am_getOption('MakeHomeDirectory', self.makeFCEntry)
    self.syncPluginName = self.am_getOption('SyncPluginName', self.syncPluginName)

    self.detailedReport = self.am_getOption('DetailedReport', self.detailedReport)
    self.mailFrom = self.am_getOption('MailFrom', self.mailFrom)

    self.voList = self.am_getOption('VO', self.voList)
    if not self.voList:
      return S_ERROR("Option 'VO' not configured")
    if self.voList[0].lower() == "any":
      result = gConfig.getSections('/Registry/VO')
      if not result['OK']:
        return result
      self.voList = result['Value']
      self.log.notice("VOs", self.voList)

    return S_OK()

  def execute(self):

    for vo in self.voList:
      voAdminUser = getVOOption(vo, "VOAdmin")
      voAdminMail = None
      if voAdminUser:
        voAdminMail = getUserOption(voAdminUser, "Email")
      voAdminGroup = getVOOption(vo, "VOAdminGroup", getVOOption(vo, "DefaultGroup"))

      self.log.info('Performing VOMS sync',
                    'for VO %s with credentials %s@%s' % (vo, voAdminUser, voAdminGroup))

      autoAddUsers = getVOOption(vo, "AutoAddUsers", self.autoAddUsers)
      autoModifyUsers = getVOOption(vo, "AutoModifyUsers", self.autoModifyUsers)
      autoDeleteUsers = getVOOption(vo, "AutoDeleteUsers", self.autoDeleteUsers)
      autoLiftSuspendedStatus = getVOOption(vo, "AutoLiftSuspendedStatus", self.autoLiftSuspendedStatus)
      syncPluginName = getVOOption(vo, 'SyncPluginName', self.syncPluginName)

      vomsSync = VOMS2CSSynchronizer(vo,
                                     autoAddUsers=autoAddUsers,
                                     autoModifyUsers=autoModifyUsers,
                                     autoDeleteUsers=autoDeleteUsers,
                                     autoLiftSuspendedStatus=autoLiftSuspendedStatus,
                                     syncPluginName=syncPluginName)

      result = self.__syncCSWithVOMS(vomsSync,  # pylint: disable=unexpected-keyword-arg
                                     proxyUserName=voAdminUser,
                                     proxyUserGroup=voAdminGroup)
      if not result['OK']:
        self.log.error('Failed to perform VOMS to CS synchronization:', 'VO %s: %s' % (vo, result["Message"]))
        continue
      resultDict = result['Value']
      newUsers = resultDict.get("NewUsers", [])
      modUsers = resultDict.get("ModifiedUsers", [])
      delUsers = resultDict.get("DeletedUsers", [])
      susUsers = resultDict.get("SuspendedUsers", [])
      csapi = resultDict.get("CSAPI")
      adminMessages = resultDict.get("AdminMessages", {'Errors': [], 'Info': []})
      voChanged = resultDict.get("VOChanged", False)
      self.log.info("Run user results",
                    ": new %d, modified %d, deleted %d, new/suspended %d" %
                    (len(newUsers), len(modUsers), len(delUsers), len(susUsers)))

      if csapi.csModified:
        # We have accumulated all the changes, commit them now
        self.log.info("There are changes to the CS ready to be committed", "for VO %s" % vo)
        if self.dryRun:
          self.log.info("Dry Run: CS won't be updated")
          csapi.showDiff()
        else:
          result = csapi.commitChanges()
          if not result['OK']:
            self.log.error("Could not commit configuration changes", result['Message'])
            return result
          self.log.notice("Configuration committed", "for VO %s" % vo)
      else:
        self.log.info("No changes to the CS recorded at this cycle", "for VO %s" % vo)

      # Add user home directory in the file catalog
      if self.makeFCEntry and newUsers:
        self.log.info("Creating home directories for users", str(newUsers))
        result = self.__addHomeDirectory(vo, newUsers,  # pylint: disable=unexpected-keyword-arg
                                         proxyUserName=voAdminUser,
                                         proxyUserGroup=voAdminGroup)
        if not result['OK']:
          self.log.error('Failed to create user home directories:', 'VO %s: %s' % (vo, result["Message"]))
        else:
          for user in result['Value']['Failed']:
            self.log.error("Failed to create home directory", "user: %s, operation: %s" %
                           (user, result['Value']['Failed'][user]))
            adminMessages['Errors'].append("Failed to create home directory for user %s: operation %s" %
                                           (user, result['Value']['Failed'][user]))
          for user in result['Value']['Successful']:
            adminMessages['Info'].append("Created home directory for user %s" % user)

      if voChanged or self.detailedReport:
        mailMsg = ""
        if adminMessages['Errors']:
          mailMsg += "\nErrors list:\n  %s" % "\n  ".join(adminMessages['Errors'])
        if adminMessages['Info']:
          mailMsg += "\nRun result:\n  %s" % "\n  ".join(adminMessages['Info'])
        if self.detailedReport:
          result = vomsSync.getVOUserReport()
          if result['OK']:
            mailMsg += '\n\n'
            mailMsg += result['Value']
          else:
            mailMsg += 'Failed to produce a detailed user report'
            mailMsg += result['Message']
        if self.dryRun:
          self.log.info("Dry Run: mail won't be sent")
          self.log.info(mailMsg)
        else:
          NotificationClient().sendMail(self.am_getOption('MailTo', voAdminMail),
                                        "VOMS2CSAgent run log", mailMsg,
                                        self.mailFrom)

    return S_OK()

  @executeWithUserProxy
  def __syncCSWithVOMS(self, vomsSync):
    return vomsSync.syncCSWithVOMS()

  @executeWithUserProxy
  def __addHomeDirectory(self, vo, newUsers):

    fc = FileCatalog(vo=vo)
    defaultVOGroup = getVOOption(vo, "DefaultGroup", "%s_user" % vo)

    failed = {}
    successful = {}
    for user in newUsers:
      result = fc.addUser(user)
      if not result['OK']:
        failed[user] = "addUser"
        continue
      dirName = '/%s/user/%s/%s' % (vo, user[0], user)
      result = fc.createDirectory(dirName)
      if not result['OK']:
        failed[user] = "createDirectory"
        continue
      result = fc.changePathOwner({dirName: user}, recursive=False)
      if not result['OK']:
        failed[user] = "changePathOwner"
        continue
      result = fc.changePathGroup({dirName: defaultVOGroup}, recursive=False)
      if not result['OK']:
        failed[user] = "changePathGroup"
        continue
      successful[user] = True

    return S_OK({"Successful": successful, "Failed": failed})
