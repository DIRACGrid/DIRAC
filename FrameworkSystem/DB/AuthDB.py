""" Auth class is a front-end to the Auth Database
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import json
import pprint
import random
import string

from ast import literal_eval
from datetime import datetime

from DIRAC import gConfig, S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.DB import DB

__RCSID__ = "$Id$"

# Session statuses

# successfully authed and ready to use
SESSION_READY = "authed"
# crashed
SESSION_FAILED = "failed"
# authed, paused for waiting when second flow will end
SESSION_REDIRECT = "redirect"
# just created
SESSION_PREPARED = "prepared"
# first request to get auth link
SESSION_PROGRESS = "in progress"
# finishing
SESSION_FINISHING = "finishing"


class AuthDB(DB):
  """ AuthDB class is a front-end to the OAuth Database
  """
  tableDict = {'Sessions': {'Fields': {'ID': 'VARCHAR(128)',
                                       'Status': 'VARCHAR(32) DEFAULT "prepared"',
                                       'Session': 'VARCHAR(64) NOT NULL',
                                       'Comment': 'MEDIUMBLOB',
                                       'Provider': 'VARCHAR(255) NOT NULL',
                                       'TokenType': 'VARCHAR(32) DEFAULT "bearer"',
                                       'ExpiresIn': 'DATETIME',
                                       'AccessToken': 'VARCHAR(1000)',
                                       'RefreshToken': 'VARCHAR(1000)',
                                       'LastAccess': 'DATETIME',
                                       'Reserved': 'VARCHAR(8) DEFAULT "no"'},
                            'PrimaryKey': 'Session',
                            'Engine': 'InnoDB'}}

  def __init__(self):
    """ Constructor
    """
    self.__permValues = ['USER', 'GROUP', 'VO', 'ALL']
    self.__permAttrs = ['ReadAccess', 'PublishAccess']
    DB.__init__(self, 'AuthDB', 'Framework/AuthDB')
    retVal = self.__initializeDB()
    if not retVal['OK']:
      raise Exception("Can't create tables: %s" % retVal['Message'])

  def _checkTable(self):
    """ Make sure the tables are created
    """
    return self.__initializeDB()

  def __initializeDB(self):
    """ Create the tables
    """
    retVal = self._query("show tables")
    if not retVal['OK']:
      return retVal

    tablesInDB = [t[0] for t in retVal['Value']]
    tablesD = {}

    if 'Sessions' not in tablesInDB:
      tablesD['Sessions'] = self.tableDict['Sessions']

    return self._createTables(tablesD)

  def updateSessionsFromDB(self, idPs=None, IDs=None, session=None):
    """ Get information about sessions

        :param list idPs: list of identity providers that sessions need to update, if None - update all
        :param list IDs: list of IDs that need to update, if None - update all
        :param str session: session to update

        :return: S_OK(dict)/S_ERROR()
    """
    resDict = {}
    cond = []
    if IDs:
      cond.append('ID IN ("%s") ' % ", ".join(IDs))
    if idPs:
      cond.append('Provider IN ("%s") ' % ", ".join(idPs))
    if session:
      cond.append('Session = "%s" ' % session)
    where = 'WHERE %s' % ' AND '.join(cond) if cond else ''
    result = self._query("SELECT DISTINCT ID, Provider, Session, Status, Reserved FROM `Sessions` %s" % where)
    if not result['OK']:
      return result
    for userID, idP, session, status, reserved in result['Value']:
      resDict[session] = {'ID': userID, 'Provider': idP, 'Status': status, 'Reserved': reserved}
      result = self.getSessionTokens(session)
      if not result['OK']:
        return result
      resDict[session]['Tokens'] = result['Value'] or {}
    return S_OK(resDict)

  def createNewSession(self, provider, session=None):
    """ Generates a state string to be used in authorizations

        :param str provider: provider
        :param str session: session number

        :return: S_OK(str)/S_ERROR()
    """
    if not session:
      result = self._query('SELECT Session FROM `Sessions`')
      if not result['OK']:
        return result
      allSessions = [s[0] for s in result['Value']]
      for i in range(100):
        num = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(30))
        if num not in allSessions:
          session = num
          break

    if not session:
      return S_ERROR("Need to modify Session manager!")

    reserved = 'yes' if self.isReservedSession(session) else 'no'
    result = self.insertFields('Sessions', ['Session', 'Provider', 'Reserved', 'LastAccess'],
                                           [session, provider, reserved, 'UTC_TIMESTAMP()'])
    return S_OK(session) if result['OK'] else result

  def isReservedSession(self, session):
    """ Check if session is reseved

        :param str session: session

        :return: bool
    """
    return re.match('^reserved_.*', session or '')

  def getSessionAuthLink(self, session):
    """ Return authorization URL from session

        :param str session: session id

        :return: S_OK(str)/S_ERROR()
    """
    cond = 'TIMESTAMPDIFF(SECOND,LastAccess,UTC_TIMESTAMP()) < 300'
    result = self.__getFields(['Comment'], cond=cond, Session=session, Status='prepared')
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('%s session is change status or deleted.')
    url = result['Value']['Comment']
    if not url:
      return S_ERROR('No link found.')
    result = self.updateSession(session, {'Status': 'in progress', 'Comment': ''})
    if not result['OK']:
      return result
    return S_OK(url)

  def getReservedSessions(self, userIDs=None, idPs=None):
    """ Find reserved session

        :param list userIDs: user ID
        :param list idPs: provider

        :return: S_OK(list)/S_ERROR() -- list contain dictionaries with information
    """
    cond = ['Reserved = "yes" AND Status = "authed"']
    if idPs:
      cond.append('Provider IN ("%s")' % '", "'.join(idPs))
    if userIDs:
      cond.append('ID IN ("%s")' % '", "'.join(userIDs))
    return self.__getFields(['Session', 'Provider', 'ID'], cond=" AND ".join(cond))

  def getSessionTokens(self, session):
    """ Get tokens dict by session

        :param str session: session number

        :return: S_OK(dict)/S_ERROR()
    """
    return self.__getFields(["AccessToken", "ExpiresIn", "RefreshToken", "TokenType"], Session=session)

  def getSessionProvider(self, session):
    """ Get tokens dict by session

        :param str session: session number

        :return: S_OK(dict)/S_ERROR()
    """
    result = self.__getFields(['Provider'], Session=session)
    return S_OK(result['Value']['Provider']) if result['OK'] else result

  def getSessionStatus(self, session):
    """ Get status dictionary by session id

        :param str session: session id

        :return: S_OK(dict)/S_ERROR()
    """
    return self.__getFields(fields=['ID', 'Session', 'Status', 'Comment', 'Provider'], Session=session)

  def getSessionID(self, session):
    """ Get user ID by session

        :param str session: session

        :return: S_OK(str)/S_ERROR()
    """
    result = self.__getFields(['ID'], Session=session)
    return S_OK(result['Value']['ID']) if result['OK'] else result

  def getSessionLifetime(self, session):
    """ Get lifetime of session

        :param str session: session number

        :return: S_OK(int)/S_ERROR() -- lifetime in a seconds
    """
    result = self.__getFields(['ExpiresIn'], Session=session)
    if result['OK']:
      exp = result['Value']['ExpiresIn']
      if not exp:
        return S_OK(0)
      result = self._query("SELECT TIME_TO_SEC(TIMEDIFF('%s', UTC_TIMESTAMP()))" % exp)

    return S_OK(result['Value'][0][0]) if result['OK'] else result

  def getZombieSessions(self):
    """ Kill sessions with old states
          SESSION_AUTHED --> kill if it's not reserved sessions and last access > 86400
          SESSION_FAILED --> kill if last access > 15 min ago
          SESSION_PREPARED --> kill if last access > 5 min ago
          SESSION_PROGRESS --> kill if last access > 10 min ago
          SESSION_REDIRECT --> kill if last access > 5 min ago
          SESSION_FINISHING --> kill if last access > 5 min ago

        :return: S_OK(dict)/S_ERROR() -- contain dictionary
    """
    cond = ['(Status NOT IN ("authed", "failed", "prepared", "in progress", "redirect", "finishing"))']
    cond.append(" AND ".join(['(Status IN ("prepared", "redirect", "finishing")',
                              'TIMESTAMPDIFF(SECOND,LastAccess,UTC_TIMESTAMP()) > 300)']))
    cond.append('(Status = "in progress" AND TIMESTAMPDIFF(SECOND,LastAccess,UTC_TIMESTAMP()) > 600)')
    cond.append('(Status = "failed" AND TIMESTAMPDIFF(SECOND,LastAccess,UTC_TIMESTAMP()) > 600)')
    cond.append('(Status = "authed" AND Reserved = "no" AND TIMESTAMPDIFF(SECOND,LastAccess,UTC_TIMESTAMP()) > 86400)')
    result = self.__getFields(['Session', 'Provider'], cond=' OR '.join(cond))
    if not result['OK']:
      return result
    zombies = {}
    sessions = result['Value']
    self.log.info('Found %s old sessions for cleaning' % len(sessions))
    for i in range(0, len(sessions)):
      if 'Session' in sessions[i]:
        session = sessions[i]['Session']
        provider = sessions[i]['Provider']
        if provider not in zombies:
          zombies[provider] = []
        zombies[provider].append(session)
    return S_OK(zombies)

  def killSession(self, session):
    """ Remove session

        :param str session: session id

        :return: S_OK()/S_ERROR()
    """
    self.log.verbose(session, 'session kill..')
    return self.deleteEntries('Sessions', condDict={'Session': session})

  def updateSession(self, session, fieldsToUpdate=None, condDict=None):
    """ Update session record

        :params str session: session id
        :param dict fieldsToUpdate: fields content that need to update
        :param dict condDict: parameters that need add to search filter

        :return: S_OK()/S_ERROR()
    """
    self.log.verbose(session, 'session update..')
    condDict = condDict or {}
    condDict['Session'] = session

    fieldsToUpdate = fieldsToUpdate or {}
    fieldsToUpdate['LastAccess'] = 'UTC_TIMESTAMP()'

    # Convert seconds to datetime
    if 'ExpiresIn' in fieldsToUpdate and isinstance(fieldsToUpdate['ExpiresIn'], int):
      exp = fieldsToUpdate['ExpiresIn']
      self.log.debug(session, 'session, convert access token live time %s seconds to date.' % exp)
      result = self._query("SELECT ADDDATE(UTC_TIMESTAMP(), INTERVAL %s SECOND)" % exp)
      if not result['OK']:
        return result
      exp = result['Value'][0][0] if result['Value'] else 'UTC_TIMESTAMP()'
    return self.updateFields('Sessions', updateDict=fieldsToUpdate, condDict=condDict)

  def __getFields(self, fields=None, cond=None, **kwargs):
    """ Get list of dict of fields that found in DB

        :param list fields: field names
        :param str cond: search filter in records
        :param str `**kwargs`: parameters that need add to search filter

        :return: S_OK(list(dict), dict)/S_ERROR() -- if searching by session dict will return
    """
    fields = fields or self.tableDict['Sessions']['Fields'].keys()
    cmd = 'SELECT %s FROM `Sessions` ' % ', '.join(fields)
    buidCond = self.buildCondition(condDict=kwargs)
    if cond:
      buidCond += ' %s %s' % ('AND' if buidCond else 'WHERE', cond)
    result = self._query(cmd + buidCond)
    if not result['OK']:
      return result

    # Collect result with adding time stamp
    resList = []
    for i in range(0, len(result['Value'])):
      d = {}
      for j, field in list(enumerate(fields)):
        d[field] = result['Value'][i][j]
      if d:
        resList.append(d)
    if not resList and kwargs.get('Session') and len(kwargs) == 1 and not cond:
      return S_ERROR('%s session no found.' % kwargs['Session'])
    return S_OK(resList[0] if kwargs.get('Session') and resList else resList)
