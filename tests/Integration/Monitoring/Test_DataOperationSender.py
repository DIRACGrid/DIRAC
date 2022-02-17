import pytest

from DIRAC import gLogger
from DIRAC.MonitoringSystem.Client.DataOperationSender import DataOperationSender

gLogger.setLevel("DEBUG")

dataOpSender = DataOperationSender()

dataOpMonitoringData = [
    {
        "OperationType": "se.getFile",
        "User": "rpozzi",
        "ExecutionSite": "",
        "Source": "CertificationSandboxSE",
        "Destination": "LCG.PIC.es",
        "Protocol": "dips",
        "FinalStatus": "Successful",
        "TransferSize": 3,
        "TransferTime": 1458226213,
        "RegistrationTime": 1458226213,
        "TransferOK": 20,
        "TransferTotal": 50,
        "RegistrationOK": 10,
        "RegistrationTotal": 40,
    },
    {
        "OperationType": "se.getFile",
        "User": "fstagni",
        "ExecutionSite": "",
        "Source": "Failed",
        "Destination": "LCG.PIC.es",
        "Protocol": "dips",
        "FinalStatus": "Failed",
        "TransferSize": 343,
        "TransferTime": 1458226213,
        "RegistrationTime": 1458226213,
        "TransferOK": 6,
        "TransferTotal": 26,
        "RegistrationOK": 3,
        "RegistrationTotal": 35,
    },
    {
        "OperationType": "se.getFile",
        "User": "fstagni",
        "ExecutionSite": "",
        "Source": "Failed",
        "Destination": "LCG.PIC.es",
        "Protocol": "dips",
        "FinalStatus": "Failed",
        "TransferSize": 35555,
        "TransferTime": 1458226213,
        "RegistrationTime": 1458226213,
        "TransferOK": 1345,
        "TransferTotal": 2614,
        "RegistrationOK": 31245,
        "RegistrationTotal": 351255,
    },
    {
        "OperationType": "se.getFile",
        "User": "rpozzi",
        "ExecutionSite": "",
        "Source": "Failed",
        "Destination": "LCG.CNAF.it",
        "Protocol": "dips",
        "FinalStatus": "Failed",
        "TransferSize": 1000,
        "TransferTime": 1458222546,
        "RegistrationTime": 1458226000,
        "TransferOK": 109,
        "TransferTotal": 1204,
        "RegistrationOK": 321,
        "RegistrationTotal": 5000,
    },
]
delayedDataOpData = [
    {
        "OperationType": "se.getFile",
        "User": "fstagni",
        "ExecutionSite": "",
        "Source": "Failed",
        "Destination": "LCG.PIC.es",
        "Protocol": "dips",
        "FinalStatus": "Failed",
        "TransferSize": 3,
        "TransferTime": 1458226213,
        "RegistrationTime": 1458226213,
        "TransferOK": 6,
        "TransferTotal": 26,
        "RegistrationOK": 3,
        "RegistrationTotal": 35,
    },
    {
        "OperationType": "se.getFile",
        "User": "rpozzi",
        "ExecutionSite": "",
        "Source": "Failed",
        "Destination": "LCG.CNAF.it",
        "Protocol": "dips",
        "FinalStatus": "Successfull",
        "TransferSize": 10,
        "TransferTime": 1458226300,
        "RegistrationTime": 1458226300,
        "TransferOK": 23,
        "TransferTotal": 113,
        "RegistrationOK": 11,
        "RegistrationTotal": 403,
    },
    {
        "OperationType": "se.getFile",
        "User": "rpozzi",
        "ExecutionSite": "",
        "Source": "Failed",
        "Destination": "LCG.CNAF.it",
        "Protocol": "dips",
        "FinalStatus": "Successfull",
        "TransferSize": 10,
        "TransferTime": 1458226300,
        "RegistrationTime": 1458226300,
        "TransferOK": 23,
        "TransferTotal": 113,
        "RegistrationOK": 11,
        "RegistrationTotal": 403,
    },
]

# fixture to have before the test methods
@pytest.fixture
def addToRegister():
    # Add the first set
    for record in dataOpMonitoringData:
        add_result = dataOpSender.sendData(record, False, False)
    assert add_result["OK"]
    # Add the second set
    for record in delayedDataOpData:
        add_result = dataOpSender.sendData(record, False, False)
    assert add_result["OK"]
    yield addToRegister


# Test all possible options for the class
@pytest.mark.parametrize(("commitFlag, delayedCommit"), [(False, False), (True, False), (True, True), (False, True)])
def test_DataOperationSender(commitFlag, delayedCommit):
    for record in dataOpMonitoringData:
        result = dataOpSender.sendData(record, commitFlag, delayedCommit)
        if not commitFlag and not delayedCommit:
            dataOpSender.concludeSending()
        assert result["OK"], result["Message"]


def test_delayed_DataOpSender():
    # Try to conclude sending of data added to the register by the fixture method addToRegister
    result = dataOpSender.concludeSending()
    assert result["OK"], result["Message"]
