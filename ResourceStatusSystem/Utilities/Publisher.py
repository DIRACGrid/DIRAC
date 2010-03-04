########################################################################
# $HeadURL:  $
########################################################################

__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter


class Publisher:
  """ Class Publisher is in charge of getting dispersed information,
      to be published on the web.
  """

#############################################################################

  def __init__(self):
    """ Standard constructor
    """
    
    self.ig = InfoGetter()
  
#############################################################################

  def getInfo(self, granularity, name, view, commandIn = None, rsDBIn = None):
    """ 
    """
    
    if rsDBIn is not None:
      rsDB = rsDBIn
    else:
      from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
      rsDB = ResourceStatusDB()
    
    resType = None        
    if granularity in ('Resource', 'Resources'):
      resType = rsDB.getMonitoredsStatusWeb(granularity, 
                                            {'ResourceName':name}, [], 0, 1)['Records'][0][3]
                                            
    self.ig = InfoGetter()
    infoToGet = self.ig.getInfoToApply(('view_info', ), None, None, None, 
                                  None, None, resType, view)[0]['Panels']
    
    cc = CommandCaller() 
    
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
            nameForPolRes = 'Other@' + name
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
            
            paramsL = ['Status']

            siteName = None
            serviceName = None
            
            if what == 'ServiceOfSite':
              gran = 'Service'
              siteName = name
            elif what == 'ResOfCompService':
              gran = 'Resources'
              serviceName = 'Computing@' + name
            elif what == 'ResOfStorService':
              gran = 'Resources'
              serviceName = 'Storage@' + name
            elif what == 'StorageElementsOfSite':
              gran = 'StorageElements'
              siteName = name

            info_bit_got = rsDB.getMonitoredsList(gran, paramsList = paramsL, siteName = siteName, 
                                                  serviceName = serviceName)
            
            info_bit_got = [x[0] for x in info_bit_got]
            
          else:
            info_bit_got = cc.commandInvocation(granularity, name, extra, commandIn, 
                                                None, what)
          
          info_res.append( { format: info_bit_got } )
          
        infoForPanel_res.append( {'policy': {policyResToGet: pol_res}, 
                                  'infos': info_res } )
        
      infoToGet_res.append( {panel: infoForPanel_res} )
    
    return infoToGet_res

#############################################################################
