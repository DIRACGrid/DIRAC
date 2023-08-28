# pylint: disable=protected-access
from DIRAC.AccountingSystem.Client.Types.StorageOccupancy import StorageOccupancy


def createStorageOccupancyAccountingRecord():
    accountingDict = {}
    accountingDict["Site"] = "LCG.PIPPO.it"
    accountingDict["Endpoint"] = "somewhere.in.topolinea.it"
    accountingDict["StorageElement"] = "PIPPO-SE"
    accountingDict["SpaceType"] = "Total"
    accountingDict["Space"] = 123456
    oSO = StorageOccupancy()
    oSO.setValuesFromDict(accountingDict)
    return oSO
