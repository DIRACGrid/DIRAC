################################################################################
# $HeadURL $
################################################################################
"""
  RealBan_PolType Actions
"""
import time

from DIRAC.ResourceStatusSystem.Utilities            import Utils, CS
from DIRAC.Interfaces.API.DiracAdmin                 import DiracAdmin
from DIRAC.ConfigurationSystem.Client.CSAPI          import CSAPI

da    = DiracAdmin()
csAPI = CSAPI()

def RealBanPolTypeActions(granularity, name, res):
  # implement real ban
  setup = CS.getSetup()

  if res['Action']:
    if granularity == 'Site':
      banList = Utils.unpack(da.getBannedSites())
      if res['Status'] == 'Banned':

        if name not in banList:
          Utils.unpack(da.banSiteFromMask(name, res['Reason']))

          address = CS.getOperationMails('Production') if 'Production' in setup else 'fstagni@cern.ch'
          subject = '%s is banned for %s setup' %(name, setup)
          body = 'Site %s is removed from site mask for %s ' %(name, setup)
          body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
          body += 'Comment:\n%s' %res['Reason']
          Utils.unpack(da.sendMail(address,subject,body))

      else:
        if name in banList:
          Utils.unpack(da.addSiteInMask(name, res['Reason']))

          address = CS.getOperationMails('Production') if setup == 'LHCb-Production' else 'fstagni@cern.ch'
          subject = '%s is added in site mask for %s setup' % (name, setup)
          body = 'Site %s is added to the site mask for %s ' % (name, setup)
          body += 'setup by the DIRAC RSS on %s.\n\n' % (time.asctime())
          body += 'Comment:\n%s' %res['Reason']
          Utils.unpack(da.sendMail(address,subject,body))

    elif granularity == 'StorageElement':
      presentReadStatus = CS.getSEStatus( name, 'ReadAccess')

      if res['Status'] == 'Banned':
        if presentReadStatus != 'InActive':
          Utils.unpack(csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(name), "InActive"))
          Utils.unpack(csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(name), "InActive"))
          Utils.unpack(csAPI.commit())

          address = CS.getOperationMails('Production') if 'Production' in setup else 'fstagni@cern.ch'
          subject = '%s is banned for %s setup' %(name, setup)
          body = 'SE %s is removed from mask for %s ' %(name, setup)
          body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
          body += 'Comment:\n%s' %res['Reason']
          Utils.unpack(da.sendMail(address,subject,body))

      else:
        if presentReadStatus == 'InActive':
          Utils.unpack(csAPI.setOption("/Resources/StorageElements/%s/ReadAccess" %(name), "Active"))
          Utils.unpack(csAPI.setOption("/Resources/StorageElements/%s/WriteAccess" %(name), "Active"))
          Utils.unpack(csAPI.commit())

          address = CS.getOperationMails('Production') if setup == 'LHCb-Production' else 'fstagni@cern.ch'
          subject = '%s is allowed for %s setup' %(name, setup)
          body = 'SE %s is added to the mask for %s ' %(name, setup)
          body += 'setup by the DIRAC RSS on %s.\n\n' %(time.asctime())
          body += 'Comment:\n%s' %res['Reason']
          Utils.unpack(da.sendMail(address,subject,body))
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
