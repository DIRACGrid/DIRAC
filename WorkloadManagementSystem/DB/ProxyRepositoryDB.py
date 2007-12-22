########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/Attic/ProxyRepositoryDB.py,v 1.6 2007/12/22 15:53:13 atsareg Exp $
########################################################################
""" ProxyRepository class is a front-end to the proxy repository Database
"""

__RCSID__ = "$Id: ProxyRepositoryDB.py,v 1.6 2007/12/22 15:53:13 atsareg Exp $"

import time
from DIRAC  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.GridCredentials import getProxyTimeLeft

#############################################################################
class ProxyRepositoryDB(DB):

  def __init__(self, systemInstance='Default',maxQueueSize=10):

    DB.__init__(self,'ProxyRepositoryDB','WorkloadManagement/ProxyRepositoryDB',maxQueueSize)

#############################################################################
  def storeProxy(self,proxy,dn,group='/lhcb',proxyType='Unknown',proxyAttr=''):
    """ Store user proxy into the Proxy repository for a user specified by his
        DN and group
    """

    result = getProxyTimeLeft(proxy)
    if not result['OK']:
      return S_ERROR('Proxy not valid')
    time_left = result['Value']
    ownergroup = group

    cmd = 'SELECT ExpirationTime FROM Proxies WHERE UserDN=\'%s\' AND UserGroup=\'%s\'' % (dn,group)
    result = self._query( cmd )
    if not result['OK']:
      return result
    # check if there is a previous ticket for the DN
    if result['Value']:
      expired = result['Value'][0][0]
      old_time_left = time.mktime(expired.timetuple())-time.time()
      if time_left > (old_time_left+60):
        cmd = 'UPDATE Proxies SET Proxy=\'%s\',' % proxy
        cmd = cmd + ' ExpirationTime = NOW() + INTERVAL %d SECOND ' % time_left
        cmd = cmd + ' ProxyType = %s ' % proxyType
        if proxyAtr:
          cmd = cmd + ' ProxyAttributes = %s ' % proxyAttr
        cmd = cmd + 'WHERE UserDN=\'%s\' AND UserGroup=\'%s\'' % ( dn, group )
        result = self._update(cmd)
        if result['OK']:
          self.log.verbose( 'Proxy Updated for DN=%s and Group=%s' % (dn,group) )
        else:
          self.log.error( 'Proxy Update Failed for DN=%s and Group=%s' % (dn,group) )
          self.log.error(result['Message'])
          return S_ERROR('Failed to store ticket')
    else:
      cmd = 'INSERT INTO Proxies ( Proxy, UserDN, UserGroup, ExpirationTime, ' \
            'ProxyType, ProxyAttributes ) VALUES ' \
            '(\'%s\', \'%s\', \'%s\', NOW() + INTERVAL %d second, \'%s\', \'%s\')' % (proxy,dn,group,time_left,proxyType,proxyAttr)
      result = self._update( cmd )
      if result['OK']:
        self.log.verbose( 'Proxy Inserted for DN="%s" and Group="%s"' % (dn,group) )
      else:
        self.log.error( 'Proxy Insert Failed for DN="%s" and Group="%s"' % (dn,group) )
        return S_ERROR('Failed to store ticket')

    return S_OK()

#############################################################################
  def getProxy(self,userDN,userGroup=None):
    """ Get proxy string from the Proxy Repository for use with userDN in the userGroup
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
      cmd = "SELECT UserDN,UserGroup FROM Proxies WHERE (NOW() + INTERVAL %d SECOND) < ExpirationTime" % validity
    else:
      cmd = "SELECT UserDN,UserGroup FROM Proxies"
    result = self._query( cmd )
    if not result['OK']:
      return result
    try:
      dn_list = result['Value']
      result_list = [ (x[0],x[1]) for x in dn_list]
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
