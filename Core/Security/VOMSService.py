""" VOMSService class encapsulates connection to the VOMS service for a given VO
"""

__RCSID__ = "$Id$"

import requests
import os

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO

class VOMSService( object ):

  def __init__(self, vo = None):

    if vo is None:
      vo = getVO()
    if not vo:
      raise Exception( 'No VO name given' )

    self.vo = vo
    self.vomsVO = getVOOption( vo, "VOMSName" )
    if not self.vomsVO:
      raise Exception( "Can not get VOMS name for VO %s" % vo )

    self.urls = []
    result = gConfig.getSections( '/Registry/VO/%s/VOMSServers' % self.vo )
    if result['OK']:
      vomsServers = result['Value']
      for server in vomsServers:
        self.urls.append( 'https://%s:8443/voms/%s/apiv2/users' % (server, self.vomsVO))

    self.userDict = None

  def admGetVOName( self ):

    return S_OK( self.vo )

  def attGetUserNickname(self, dn, _ca=None):

    if self.userDict is None:
      result = self.getUsers()
      if not result['OK']:
        return result

    uDict = self.userDict.get(dn)
    if not uDict:
      return S_ERROR(DErrno.EVOMS, "No nickname defined")
    nickname = uDict.get('nickname')
    if not nickname:
      return S_ERROR(DErrno.EVOMS, "No nickname defined")
    return S_OK(nickname)

  def getUsers(self):
    """ Get all the users of the VOMS VO with their detailed information

    :return: user dictionary keyed by the user DN
    """

    userProxy = os.environ['X509_USER_PROXY']
    caPath = os.environ['X509_CERT_DIR']
    rawUserList = []
    for url in self.urls:

      try:
        result = requests.get(url,
                              headers = {"X-VOMS-CSRF-GUARD": "y"},
                              cert = userProxy,
                              verify = caPath,
                              params = {"startIndex": "0",
                                        "pageSize":"100"} )
      except requests.ConnectionError as exc:
        pass

      if result.status_code != 200:
        return S_ERROR(DErrno.ENOAUTH, "Failed to contact the VOMS server", result.text)

      userList = result.json()['result']

      print "AT >>> len(userList)", len(userList)

      rawUserList.extend(userList)
      if len(userList) < 100: break

    # We have got the user info, reformat it
    resultDict = {}
    for user in rawUserList:
      for cert in user['certificates']:
        dn = cert['subjectString']
        resultDict[dn] = user
        resultDict[dn]['CA'] = cert['issuerString']
        resultDict[dn]['certSuspended'] = cert['suspended']
        resultDict[dn]['certSuspensionReason'] = cert['suspensionReason']
        resultDict[dn]['mail'] = user['emailAddress']
        resultDict[dn]['Roles'] = user['fqans']
        for attribute in user['attributes']:
          if attribute.get('name') == 'nickname':
            resultDict[dn]['nickname'] = attribute.get('value')

    self.userDict = dict(resultDict)
    return S_OK(resultDict)
