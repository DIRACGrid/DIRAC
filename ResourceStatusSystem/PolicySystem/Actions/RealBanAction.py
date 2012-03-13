################################################################################
# $HeadURL $
################################################################################
"""
  RealBan_PolType Actions
"""
import time

from DIRAC.ResourceStatusSystem.Utilities                       import CS
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
        banList = da.getBannedSites()
        if not banList[ 'OK' ]:
          print banList[ 'Message' ]
          return banList
                
        if self.new_status['Status'] == 'Banned':

          if self.name not in banList:
            banSite = da.banSiteFromMask( self.name, self.new_status['Reason'] )
            if not banSite[ 'OK' ]:
              print banSite[ 'Message' ]
              return banSite

            address = CS.getOperationMails('Production') if 'Production' in setup else 'fstagni@cern.ch'
            subject = '%s is banned for %s setup' %(self.name, setup)
            body = 'Site %s is removed from site mask for %s ' %(self.name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %self.new_status['Reason']
            
            da.sendMail(address,subject,body)

        else:
          if self.name in banList:
            addSite = da.addSiteInMask(self.name, self.new_status['Reason'])
            if not addSite[ 'OK' ]:
              print addSite
              return addSite

            address = CS.getOperationMails('Production') if setup == 'LHCb-Production' else 'fstagni@cern.ch'
            subject = '%s is added in site mask for %s setup' % (self.name, setup)
            body = 'Site %s is added to the site mask for %s ' % (self.name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' % (time.asctime())
            body += 'Comment:\n%s' %self.new_status['Reason']
            da.sendMail(address,subject,body)

      elif self.granularity == 'StorageElement':
        presentReadStatus = CS.getSEStatus( self.name, 'ReadAccess')

        if self.new_status['Status'] == 'Banned':
          if presentReadStatus != 'InActive':
            csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(self.name), "InActive")
            csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(self.name), "InActive")
            csAPI.commit()

            address = CS.getOperationMails('Production') if 'Production' in setup else 'fstagni@cern.ch'
            subject = '%s is banned for %s setup' %(self.name, setup)
            body = 'SE %s is removed from mask for %s ' %(self.name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %self.new_status['Reason']
            da.sendMail(address,subject,body)

        else:
          if presentReadStatus == 'InActive':
            csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(self.name), "Active")
            csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(self.name), "Active")
            csRes = csAPI.commit()
            if not csRes[ 'OK' ]:
              print csRes[ 'Message' ]
              return csRes

            address = CS.getOperationMails('Production') if setup == 'LHCb-Production' else 'fstagni@cern.ch'
            subject = '%s is allowed for %s setup' %(self.name, setup)
            body = 'SE %s is added to the mask for %s ' %(self.name, setup)
            body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
            body += 'Comment:\n%s' %self.new_status['Reason']
            da.sendMail(address,subject,body)
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF