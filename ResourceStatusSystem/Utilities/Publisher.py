########################################################################
# $HeadURL:  $
########################################################################

__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Policy import Configurations
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter
from DIRAC.ResourceStatusSystem.Client.Command.CommandCaller import CommandCaller


class Publisher:
  """ Class Publisher is in charge of getting dispersed information,
      to be published on the web.
  """

#############################################################################

  def __init__(self):
    """ Standard constructor
    """
    
    self.ig = InfoGetter()
    self.cc = CommandCaller() 
    
  
#############################################################################

  def getInfo(self, granularity, name, view, rsDBIn = None, commandCallerIn = None):
    """ 
    """
    
    if granularity not in ValidRes:
      raise InvalidRes, where(self, self.getInfo)

    if view not in Configurations.views_panels.keys():
      raise InvalidView, where(self, self.getInfo)
    
    
    if rsDBIn is not None:
      rsDB = rsDBIn
    else:
      from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
      rsDB = ResourceStatusDB()
    
    if commandCallerIn is not None:
      self.cc = commandCallerIn

    resType = None        
    if granularity in ('Resource', 'Resources'):
      resType = rsDB.getMonitoredsStatusWeb(granularity, 
                                            {'ResourceName':name}, [], 0, 1)['Records'][0][3]
                                            
    self.ig = InfoGetter()
    infoToGet = self.ig.getInfoToApply(('view_info', ), None, None, None, 
                                       None, None, resType, view)[0]['Panels']
    
    infoToGet_res = []
    
    for panel in infoToGet.keys():
      infoForPanel = infoToGet[panel]
      
      infoForPanel_res = []
      
      for info in infoForPanel:
        policyResToGet = info.keys()[0]
        if granularity in ('Site', 'Sites'):
          if panel == 'Service_Computing_Panel':
            nameForPolRes = 'Computing@' + name
          elif panel == 'Service_Storage_Panel':
            nameForPolRes = 'Storage@' + name
          elif panel == 'OtherServices_Panel':
            nameForPolRes = 'OtherS@' + name
          else:
            nameForPolRes = name
        else:
          nameForPolRes = name
        pol_res = rsDB.getPolicyRes(nameForPolRes, policyResToGet)
        
        othersInfo = info.values()[0]
        if not isinstance(othersInfo, list):
          othersInfo = [othersInfo]
        
        extra = None
        
        info_res = []
        
        for oi in othersInfo:
          
          format = oi.keys()[0]
          what = oi.values()[0]
          
          if format == 'RSS':
            info_bit_got = self._getInfoFromRSSDB(name, what)
#            info_bit_got = rsDB.getMonitoredsList(gran, paramsList = paramsL, siteName = siteName, 
#                                                  serviceName = serviceName)
#            info_bit_got = [x[0] for x in info_bit_got]
          else:
            info_bit_got = self.cc.commandInvocation(granularity, name, extra, None, 
                                                     None, what)
          
          info_res.append( { format: info_bit_got } )
          
        infoForPanel_res.append( {'policy': {policyResToGet: pol_res}, 
                                  'infos': info_res } )
        
      infoToGet_res.append( {panel: infoForPanel_res} )
    
    return infoToGet_res

#############################################################################

  def _getInfoFromRSSDB(self, name, what):

      paramsL = ['Status']

      siteName = None
      serviceName = None
      
      if what == 'ServiceOfSite':
        gran = 'Service'
        paramsL.append('ServiceName')
        siteName = name
      elif what == 'ResOfCompService':
        gran = 'Resources'
        paramsL.append('ResourceName')
        serviceName = 'Computing@' + name
      elif what == 'ResOfStorService':
        gran = 'Resources'
        paramsL.append('ResourceName')
        serviceName = 'Storage@' + name
      elif what == 'StorageElementsOfSite':
        gran = 'StorageElements'
        paramsL.append('StorageElementName')
        siteName = name

      info_bit_got = rsDB.getMonitoredsList(gran, paramsList = paramsL, siteName = siteName, 
                                            serviceName = serviceName)
      
      return info_bit_got

#############################################################################
