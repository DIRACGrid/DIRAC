################################################################################
# $HeadURL $
################################################################################
"""
  RealBan_PolType Actions
"""
import time

from DIRAC.ResourceStatusSystem.Utilities                       import Utils, CS
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.ActionBase import ActionBase
from DIRAC.Interfaces.API.DiracAdmin                            import DiracAdmin
from DIRAC.ConfigurationSystem.Client.CSAPI                     import CSAPI

da    = DiracAdmin()
csAPI = CSAPI()

class RealBanAction(ActionBase):
  def run(self):
    """Implement real ban"""
    setup = CS.getSetup()

    if self.new_status['Action']:
      if self.granularity == 'Site':
        banList = Utils.unpack(da.getBannedSites())
        if self.new_status['Status'] == 'Banned':

          if self.name not in banList:
            Utils.unpack(da.banSiteFromMask(self.name, self.new_status['Reason']))

            address = CS.getOperationMails('Production') if 'Production' in setup else 'fstagni@cern.ch'
            subject = '%s is banned for %s setup' %(self.name, setup)
            body = 'Site %s is removed from site mask for %s ' %(self.name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %self.new_status['Reason']
            Utils.unpack(da.sendMail(address,subject,body))

        else:
          if self.name in banList:
            Utils.unpack(da.addSiteInMask(self.name, self.new_status['Reason']))

            address = CS.getOperationMails('Production') if setup == 'LHCb-Production' else 'fstagni@cern.ch'
            subject = '%s is added in site mask for %s setup' % (self.name, setup)
            body = 'Site %s is added to the site mask for %s ' % (self.name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' % (time.asctime())
            body += 'Comment:\n%s' %self.new_status['Reason']
            Utils.unpack(da.sendMail(address,subject,body))

      elif self.granularity == 'StorageElement':
        presentReadStatus = CS.getSEStatus( self.name, 'ReadAccess')

        if self.new_status['Status'] == 'Banned':
          if presentReadStatus != 'InActive':
            Utils.unpack(csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(self.name), "InActive"))
            Utils.unpack(csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(self.name), "InActive"))
            Utils.unpack(csAPI.commit())

            address = CS.getOperationMails('Production') if 'Production' in setup else 'fstagni@cern.ch'
            subject = '%s is banned for %s setup' %(self.name, setup)
            body = 'SE %s is removed from mask for %s ' %(self.name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %self.new_status['Reason']
            Utils.unpack(da.sendMail(address,subject,body))

        else:
          if presentReadStatus == 'InActive':
            Utils.unpack(csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(self.name), "Active"))
            Utils.unpack(csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(self.name), "Active"))
            Utils.unpack(csAPI.commit())

            address = CS.getOperationMails('Production') if setup == 'LHCb-Production' else 'fstagni@cern.ch'
            subject = '%s is allowed for %s setup' %(self.name, setup)
            body = 'SE %s is added to the mask for %s ' %(self.name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %self.new_status['Reason']
            Utils.unpack(da.sendMail(address,subject,body))
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
