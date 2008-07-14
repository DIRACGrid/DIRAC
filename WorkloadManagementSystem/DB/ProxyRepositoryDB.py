########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/Attic/ProxyRepositoryDB.py,v 1.18 2008/07/14 18:06:27 acasajus Exp $
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""
#TODO: Delete after complete migration to new proxy style
__RCSID__ = "$Id: ProxyRepositoryDB.py,v 1.18 2008/07/14 18:06:27 acasajus Exp $"

import time
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.GridCredentials import *

#############################################################################
class ProxyRepositoryDB(DB):

  def __init__(self, systemInstance='Default',maxQueueSize=10):

    DB.__init__(self,'ProxyRepositoryDB','WorkloadManagement/ProxyRepositoryDB',maxQueueSize)

    result = gConfig.getOption('/DIRAC/VirtualOrganization')
    if result['OK']:
      self.VO = result['Value']
    else:
      self.VO = "unknown"

    result = gConfig.getOption('/DIRAC/DefaultGroup')
    if result['OK']:
      self.defaultGroup = result['Value']
    else:
      self.defaultGroup = "unknown"

    result = gConfig.getOptionsDict('Groups/DiracToVOMSGroupMapping')
    if result['OK']:
      self.vomsGroupMappingDict = result['Value']
    else:
      self.vomsGroupMappingDict = {}

    self.servercert = gConfig.getValue('/DIRAC/Security/CertFile',
                              '/opt/dirac/etc/grid-security/hostcert.pem')
    self.serverkey = gConfig.getValue('/DIRAC/Security/KeyFile',
                              '/opt/dirac/etc/grid-security/hostkey.pem')

#############################################################################
  def storeProxy(self,proxy,dn,group):
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group.
        The grid proxy will be converted into a VOMS proxy if possible
    """

    result = getVOMSAttributes(proxy,'db')
    if not result['OK']:
      return S_ERROR('Can not analyze proxy')

    attributeString = result['Value']
    if attributeString:
      proxyType = "VOMS"
      proxyAttr = attributeString
    else:
      proxyType = "GRID"
      proxyAttr = ''

    result = getProxyTimeLeft(proxy)
    if not result['OK']:
      return S_ERROR('Proxy not valid')

    # Force the VOMS conversion with the proper role corresponding to the
    # group in the request
    force_VOMS = False
    if self.vomsGroupMappingDict.has_key(group):
      if attributeString != self.vomsGroupMappingDict[group]:
        force_VOMS = True
    else:
      return S_ERROR('Non-valid user group requested for proxy '+group)

    time_left = result['Value']
    ownergroup = group

    # Check what we have already got in the repository
    proxy_exists = False
    cmd = 'SELECT ExpirationTime,ProxyType FROM Proxies WHERE UserDN=\'%s\' AND UserGroup=\'%s\'' % (dn,group)
    result = self._query( cmd )
    if not result['OK']:
      return result
    # check if there is a previous ticket for the DN
    if result['Value']:
      expired = result['Value'][0][0]
      old_type = result['Value'][0][1]
      old_time_left = time.mktime(expired.timetuple())-time.time()
      time_delta = time_left - old_time_left
      relative_time_delta = time_delta/time_left
      proxy_exists = True

    # Decide if we should store and convert the new proxy
    if not proxy_exists:
      if proxyType != "VOMS" or force_VOMS:
        result = self.__convertProxyToVOMS(proxy,group,dn,proxyType)
        if not result['OK']:
          return S_ERROR('Failed to convert proxy to VOMS')

        proxy_to_store = result['VOMS']
        proxyType = "VOMS"
        proxyAttr = result['ProxyAttributes']
        time_left = result['TimeLeft']
      else:
        proxy_to_store = proxy

      cmd = 'INSERT INTO Proxies ( Proxy, UserDN, UserGroup, ExpirationTime, ' \
            'ProxyType, ProxyAttributes ) VALUES ' \
            '(\'%s\', \'%s\', \'%s\', NOW() + INTERVAL %d second, \'%s\', \'%s\')' % (proxy_to_store,dn,group,time_left,proxyType,proxyAttr)
      result = self._update( cmd )
      if result['OK']:
        self.log.verbose( 'Proxy inserted for DN="%s" and Group="%s"' % (dn,group) )
      else:
        self.log.error( 'Proxy insert failed for DN="%s" and Group="%s"' % (dn,group) )
        return S_ERROR('Failed to store proxy')
    else:
      # Check if we have to replace the old proxy
      force_proxy = False
      if old_type and old_type != 'VOMS' and proxyType == 'VOMS':
        force_proxy = True
      # Store new proxy if it is significantly longer than the existing one
      # or the new VOMS proxy replaces the old GRID proxy
      if relative_time_delta > 0.1 or force_proxy:
        # check if have to convert the proxy to VOMS
        if proxyType != "VOMS" or force_VOMS:
          result = self.__convertProxyToVOMS(proxy,group,dn,proxyType)
          if not result['OK']:
            return S_ERROR('Failed to convert proxy to VOMS')
          proxy_to_store = result['VOMS']
          proxyType == "VOMS"
          proxyAttr = result['ProxyAttributes']
          time_left = result['TimeLeft']
        else:
          proxy_to_store = proxy

        cmd = 'UPDATE Proxies SET Proxy=\'%s\',' % proxy
        cmd = cmd + ' ExpirationTime = NOW() + INTERVAL %d SECOND, ' % time_left
        cmd = cmd + ' ProxyType=\'%s\' ' % proxyType
        if proxyAttr:
          cmd = cmd + ', ProxyAttributes=\'%s\' ' % proxyAttr
        else:
          cmd = cmd + ', ProxyAttributes=\'\' '
        cmd = cmd + 'WHERE UserDN=\'%s\' AND UserGroup=\'%s\'' % ( dn, group )

        result = self._update(cmd)
        if result['OK']:
          self.log.verbose( 'Proxy Updated for DN=%s and Group=%s' % (dn,group) )
        else:
          self.log.error( 'Proxy Update Failed for DN=%s and Group=%s' % (dn,group) )
          self.log.error(result['Message'])
          return S_ERROR('Failed to store ticket')

    return S_OK()

#############################################################################
  def __convertProxyToVOMS(self,proxy,group,dn,proxytype):
    """ Convert the proxy to the VOMS proxy with the given group. If proxytype
        is already VOMS, recreate the grid proxy from MyProxy
    """

    # If proxy is VOMS, we have to start from MyProxy delegation
    result = getProxyTimeLeft(proxy)
    if result['OK']:
      time_left = result['Value']
    else:
      return S_ERROR('Invalid proxy')

    if proxytype == "VOMS":
      result = getMyProxyDelegation(proxy,time_left,
                                    server_cert=self.servercert,
                                    server_key=self.serverkey)

      if result['OK']:
        proxy_plain = result['Value']
      else:
        return S_ERROR('Failed to get MyProxy delegation')
    else:
      proxy_plain = proxy

    # Attempt to convert into a VOMS proxy

    self.log.verbose('Converting proxy to VOMS for '+dn)

    if self.vomsGroupMappingDict.has_key(group):
      proxyAttr = self.VO+":"+self.vomsGroupMappingDict[group]
      result = createVOMSProxy(proxy_plain,attributes=proxyAttr)
    else:
      proxyAttr = ''
      result = createVOMSProxy(proxy_plain,vo=self.VO)

    if result['OK']:
      self.log.info('VOMS conversion done for '+dn)
      new_proxy = result['Value']
      proxy_to_store = setDIRACGroupInProxy(new_proxy,group)
      proxyType = "VOMS"
      result = getProxyTimeLeft(proxy_to_store)
      if not result['OK']:
        return S_ERROR('Proxy not valid')
      time_left = result['Value']
    else:
      return S_ERROR('Failed to create VOMS proxy')

    result = S_OK()
    result['VOMS'] = proxy_to_store
    result['TimeLeft'] = time_left
    result['ProxyAttributes'] = proxyAttr.replace(self.VO+":",'')
    return result


#############################################################################
  def destroyProxy(self,userDN,userGroup):
    """ Remove proxy of the given user from the repository
    """

    req = "DELETE FROM Proxies WHERE UserDN='%s' AND UserGroup='%s'" % (userDN,userGroup)
    result = self._update(req)
    return result

#############################################################################
  def getProxy(self,userDN,userGroup=None):
    """ Get proxy string from the Proxy Repository for use with userDN
        in the userGroup
    """

    if userGroup:
      cmd = "SELECT Proxy from Proxies WHERE UserDN='%s' AND UserGroup = '%s'" % \
      (userDN,userGroup)
    else:
      cmd = "SELECT Proxy from Proxies WHERE UserDN='%s'" % userDN
    result = self._query(cmd)
    if not result['OK']:
      return result
    try:
      proxy = result['Value'][0][0]
      return S_OK(proxy)
    except:
      return S_ERROR('Failed to get proxy from the Proxy Repository')


#############################################################################
  def getUsers(self,validity=0):
    """ Get all the distinct users from the Proxy Repository. Optionally, only users
        with valid proxies within the given validity period expressed in seconds
    """

    if validity:
      cmd = "SELECT UserDN,UserGroup,ProxyType,PersistentFlag FROM Proxies WHERE (NOW() + INTERVAL %d SECOND) > ExpirationTime" % validity
    else:
      cmd = "SELECT UserDN,UserGroup,ProxyType,PersistentFlag FROM Proxies"
    result = self._query( cmd )
    if not result['OK']:
      return result
    try:
      dn_list = result['Value']
      result_list = [ (x[0],x[1],x[2],x[3] ) for x in dn_list]
      return S_OK(result_list)
    except:
      return S_ERROR('Failed to get proxy owner DNs and groups from the Proxy Repository')

#############################################################################
  def removeProxy(self,userDN,userGroup=None):
    """ Remove a proxy from the proxy repository
    """

    if userGroup:
      cmd = "DELETE  from Proxies WHERE UserDN='%s' AND UserGroup = '%s'" % \
      (userDN,userGroup)
    else:
      cmd = "DELETE  from Proxies WHERE UserDN='%s'" % userDN
    result = self._update( cmd )
    return result

#############################################################################
  def setProxyPersistencyFlag(self,userDN,userGroup,flag = True):
    """ Set the proxy PersistentFlag to the flag value
    """

    if flag:
      cmd = "UPDATE Proxies SET PersistentFlag='True' "
    else:
      cmd = "UPDATE Proxies SET PersistentFlag='False' "
    cmd = cmd + "where UserDN='%s' and UserGroup='%s'" % (userDN,userGroup)

    result = self._update(cmd)
    return result
