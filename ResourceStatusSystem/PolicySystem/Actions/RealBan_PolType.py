################################################################################
# $HeadURL $
################################################################################
"""
  RealBan_PolType Actions
"""
import time

from DIRAC.ResourceStatusSystem.Utilities            import CS
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException

# FIXME: Get rid of this very temporary hack. ##
class DummyObj(object):
  enforce = None
################################################################################

def where(_a, _b):
  return "Module RealBan_PolType"
self = DummyObj()

################################################################################

def RealBanPolTypeActions(granularity, name, res, da, csAPI):
  # implement real ban
  setup = CS.getSetup()

  if res['Action']:

    if granularity == 'Site':

      banList = da.getBannedSites()
      if not banList['OK']:
        raise RSSException, where(self, self.enforce) + banList['Message']
      else:
        banList = banList['Value']

      if res['Status'] == 'Banned':

        if name not in banList:
          banSite = da.banSiteFromMask(name, res['Reason'])
          if not banSite['OK']:
            raise RSSException, where(self, self.enforce) + banSite['Message']
          if 'Production' in setup:
            address = getOperationMails('Production')
          else:
            address = 'fstagni@cern.ch'

          subject = '%s is banned for %s setup' %(name, setup)
          body = 'Site %s is removed from site mask for %s ' %(name, setup)
          body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
          body += 'Comment:\n%s' %res['Reason']
          sendMail = da.sendMail(address,subject,body)
          if not sendMail['OK']:
            raise RSSException, where(self, self.enforce) + sendMail['Message']

      else:
        if name in banList:
          addSite = da.addSiteInMask(name, res['Reason'])
          if not addSite['OK']:
            raise RSSException, where(self, self.enforce) + addSite['Message']
          if setup == 'LHCb-Production':
            address = CS.getOperationMails('Production')
          else:
            address = 'fstagni@cern.ch'

          subject = '%s is added in site mask for %s setup' %(name, setup)
          body = 'Site %s is added to the site mask for %s ' %(name, setup)
          body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
          body += 'Comment:\n%s' %res['Reason']
          sendMail = da.sendMail(address,subject,body)
          if not sendMail['OK']:
            raise RSSException, where(self, self.enforce) + sendMail['Message']

    elif granularity == 'StorageElement':

      presentReadStatus = CS.getStorageElementStatus( name, 'ReadAccess')

      if res['Status'] == 'Banned':

        if presentReadStatus != 'InActive':
          banSE = csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(name), "InActive")
          if not banSE['OK']:
            raise RSSException, where(self, self.enforce) + banSE['Message']
          banSE = csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(name), "InActive")
          if not banSE['OK']:
            raise RSSException, where(self, self.enforce) + banSE['Message']
          commit = csAPI.commit()
          if not commit['OK']:
            raise RSSException, where(self, self.enforce) + commit['Message']
          if 'Production' in setup:
            address = CS.getOperationMails('Production')
          else:
            address = 'fstagni@cern.ch'

          subject = '%s is banned for %s setup' %(name, setup)
          body = 'SE %s is removed from mask for %s ' %(name, setup)
          body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
          body += 'Comment:\n%s' %res['Reason']
          sendMail = da.sendMail(address,subject,body)
          if not sendMail['OK']:
            raise RSSException, where(self, self.enforce) + sendMail['Message']

      else:

        if presentReadStatus == 'InActive':

          allowSE = csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(name), "Active")
          if not allowSE['OK']:
            raise RSSException, where(self, self.enforce) + allowSE['Message']
          allowSE = csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(name), "Active")
          if not allowSE['OK']:
            raise RSSException, where(self, self.enforce) + allowSE['Message']
          commit = csAPI.commit()
          if not commit['OK']:
            raise RSSException, where(self, self.enforce) + commit['Message']
          if setup == 'LHCb-Production':
            address = CS.getOperationMails('Production')
          else:
            address = 'fstagni@cern.ch'

          subject = '%s is allowed for %s setup' %(name, setup)
          body = 'SE %s is added to the mask for %s ' %(name, setup)
          body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
          body += 'Comment:\n%s' %res['Reason']
          sendMail = da.sendMail(address,subject,body)
          if not sendMail['OK']:
            raise RSSException, where(self, self.enforce) + sendMail['Message']

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
