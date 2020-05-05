""" Contains a class used for evaluating policies for accessing jobs/WMS/pilots accounting
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import Properties
from DIRAC.AccountingSystem.private.Policies.FilterExecutor import FilterExecutor


class JobPolicy(object):

  def __init__(self):
    self.__executor = FilterExecutor()
    self.__executor.addGlobalFilter(self.__checkConditions)

  def getListingConditions(self, credDict):
    condDict = {}
    userProps = credDict['properties']

    if Properties.JOB_ADMINISTRATOR in userProps:
      return condDict
    elif Properties.JOB_MONITOR in userProps:
      return condDict
    elif Properties.JOB_SHARING in userProps:
      condDict['UserGroup'] = [credDict['group']]

    return condDict

  def checkRequest(self, iD, credDict, condDict, groupingList):
    return self.__executor.applyFilters(iD, credDict, condDict, groupingList)

  def __checkConditions(self, credDict, condDict, groupingField):
    userProps = credDict['properties']

    if Properties.JOB_ADMINISTRATOR in userProps:
      return S_OK()
    elif Properties.JOB_MONITOR in userProps:
      return S_OK()
    elif Properties.JOB_SHARING in userProps:
      if 'User' in condDict:
        condDict['UserGroup'] = credDict['group']
      if 'User' == groupingField:
        condDict['UserGroup'] = credDict['group']
      if 'UserGroup' in condDict:
        condDict['UserGroup'] = credDict['group']
      if 'UserGroup' == groupingField:
        condDict['UserGroup'] = credDict['group']
    elif Properties.NORMAL_USER in userProps:
      if 'User' in condDict:
        condDict['User'] = credDict['username']
      if 'User' == groupingField:
        condDict['User'] = credDict['username']
      if 'UserGroup' in condDict:
        condDict['User'] = credDict['username']
        condDict['UserGroup'] = credDict['group']
      if 'UserGroup' == groupingField:
        condDict['User'] = credDict['username']
        condDict['UserGroup'] = credDict['group']
      else:
        condDict['User'] = credDict['username']
    else:
      if 'User' in condDict:
        del(condDict['User'])
      if 'UserGroup' in condDict:
        del(condDict['UserGroup'])
      if 'User' == groupingField:
        return S_ERROR("You can't group plots by users!")
      if 'UserGroup' == groupingField:
        return S_ERROR("You can't group plots by user groups!")

    return S_OK()

  def filterListingValues(self, credDict, dataDict):
    userProps = credDict['properties']

    if Properties.JOB_ADMINISTRATOR in userProps:
      return S_OK(dataDict)
    elif Properties.JOB_MONITOR in userProps:
      return S_OK(dataDict)
    elif Properties.JOB_SHARING in userProps:
      return S_OK(dataDict)
    elif Properties.NORMAL_USER in userProps:
      return S_OK(dataDict)

    dataDict['User'] = []
    dataDict['UserGroup'] = []

    return S_OK(dataDict)
