from DIRAC import S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.private.DBUtils import DBUtils


class Summaries(DBUtils):

  def __init__(self, db, setup):
    DBUtils.__init__(self, db, setup)

  def generate(self, summaryName, startTime, endTime, argsDict):
    funcName = "_summary%s" % summaryName
    try:
      funcObj = getattr(self, funcName)
    except BaseException:
      return S_ERROR("Summary %s is not defined" % summaryName)
    return funcObj(startTime, endTime, argsDict)

  def summariesList(self):
    sumList = []
    for attr in dir(self):
      if attr.find("_summary") == 0:
        sumList.append(attr.replace("_summary", ""))
    sumList.sort()
    return sumList

  def _summaryDataBySource(self, startTime, endTime, argsDict):
    """
      argsDict: Source -> Summary only sites in source. If not present summary all.
    """
    if 'Source' not in argsDict:
      condDict = {}
    else:
      condDict = {"Source": argsDict['Source']}
    do = DataOperation()
    selectFields = ["Source"]
    selectStringList = ["%s"]
    for fieldTuple in do.definitionAccountingFields:
      selectStringList.append("%s")
      selectFields.append(fieldTuple[0])
    retVal = self._retrieveBucketedData("DataOperation",
                                        startTime,
                                        endTime,
                                        (", ".join(selectStringList), selectFields),
                                        condDict,
                                        ["Source"],
                                        ["Source"])
    if not retVal['OK']:
      return retVal
    return S_OK((selectFields, retVal['Value']))

  def _summaryDataByDestination(self, startTime, endTime, argsDict):
    """
      argsDict: Destination -> Summary only sites in destination. If not present summary all.
    """
    if 'Destination' not in argsDict:
      condDict = {}
    else:
      condDict = {"Destination": argsDict['Destination']}
    do = DataOperation()
    selectFields = ["Destination"]
    selectStringList = ["%s"]
    for fieldTuple in do.definitionAccountingFields:
      selectStringList.append("%s")
      selectFields.append(fieldTuple[0])
    retVal = self._retrieveBucketedData("DataOperation",
                                        startTime,
                                        endTime,
                                        (", ".join(selectStringList), selectFields),
                                        condDict,
                                        ["Destination"],
                                        ["Destination"])
    if not retVal['OK']:
      return retVal
    return S_OK((selectFields, retVal['Value']))

  def _summaryDataBySourceAndDestination(self, startTime, endTime, argsDict):
    """
      argsDict:
        - Source -> Summary only sites in source.
        - Destination -> Summary only sites in destination.
    """
    keyFields = ('Destination', 'Source')
    condDict = {}
    for keyword in keyFields:
      if keyword in argsDict:
        condDict[keyword] = argsDict[keyword]
    do = DataOperation()
    selectFields = list(keyFields)
    selectStringList = ["%s, %s"]
    for fieldTuple in do.definitionAccountingFields:
      selectStringList.append("%s")
      selectFields.append(fieldTuple[0])
    retVal = self._retrieveBucketedData("DataOperation",
                                        startTime,
                                        endTime,
                                        (", ".join(selectStringList), selectFields),
                                        condDict,
                                        keyFields,
                                        keyFields)
    if not retVal['OK']:
      return retVal
    return S_OK((selectFields, retVal['Value']))
