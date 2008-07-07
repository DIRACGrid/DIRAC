
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import Properties
from DIRAC.AccountingSystem.private.Policies.FilterExecutor import FilterExecutor

class JobPolicy:

  def __init__( self ):
    self.__executor = FilterExecutor()
    self.__executor.addGlobalFilter( self.__checkConditions )

  def getListingConditions( self, credDict ):
    condDict = {}
    userProps = credDict[ 'properties' ]

    if Properties.JOB_ADMINISTRATOR in userProps:
      return condDict
    elif Properties.JOB_SHARING in userProps:
      condDict[ 'UserGroup' ] = [ credDict[ 'group' ] ]
    elif Properties.NORMAL_USER in userProps:
      condDict[ 'User' ] = [ credDict[ 'username' ] ]

    return condDict

  def checkPlot( self, id, credDict, condDict, groupingList ):
    return self.__executor.applyFilters( id, credDict, condDict, groupingList )

  def __checkConditions( self, credDict, condDict, groupingList ):
    userProps = credDict[ 'properties' ]

    if Properties.JOB_ADMINISTRATOR in userProps:
      return S_OK()
    elif Properties.JOB_SHARING in userProps:
      if 'UserGroup' in condDict:
        condDict[ 'UserGroup' ] = credDict[ 'group' ]
      if 'UserGroup' in groupingList:
        condDict[ 'UserGroup' ] = credDict[ 'group' ]
    elif Properties.NORMAL_USER in userProps:
      if 'User' in condDict:
        condDict[ 'User' ] = credDict[ 'username' ]
      if 'User' in groupingList:
        condDict[ 'User' ] = credDict[ 'username' ]
      if 'UserGroup' in condDict:
        condDict[ 'User' ] = credDict[ 'username' ]
        condDict[ 'UserGroup' ] = credDict[ 'group' ]
      if 'UserGroup' in groupingList:
        condDict[ 'User' ] = credDict[ 'username' ]
        condDict[ 'UserGroup' ] = credDict[ 'group' ]



    return S_OK()
