"""Unit tests for ProductionClient."""
# pylint: disable=protected-access,missing-docstring,invalid-name

from unittest.mock import MagicMock

import pytest

from DIRAC import S_ERROR, S_OK
from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
from DIRAC.ProductionSystem.Client.ProductionStep import ProductionStep


@pytest.fixture
def productionClientAndRPC(monkeypatch):
    productionClient = ProductionClient()
    rpcClient = MagicMock()
    monkeypatch.setattr(productionClient, "_getRPC", lambda: rpcClient)
    return productionClient, rpcClient


def test_addProductionStepIncludesCustomParameters(productionClientAndRPC):
    productionClient, rpcClient = productionClientAndRPC
    rpcClient.addProductionStep.return_value = S_OK(12)
    parentStep = ProductionStep()
    parentStep.Name = "Parent"
    productionStep = ProductionStep()
    productionStep.Name = "Child"
    productionStep.ParentStep = parentStep
    productionStep.CustomOption = "custom value"

    result = productionClient.addProductionStep(productionStep)

    assert result["OK"]
    assert productionStep.Name == "Step1_Child"
    rpcClient.addProductionStep.assert_called_once()
    sentStep = rpcClient.addProductionStep.call_args.args[0]
    assert sentStep["name"] == "Step1_Child"
    assert sentStep["parentStep"] == ["Parent"]
    assert "customParameters" not in sentStep
    assert productionClient.prodDescription == {
        "Step1_Child": {
            "stepID": 12,
            "parentStep": ["Parent"],
            "customParameters": {"CustomOption": "custom value"},
        }
    }

    productionStep.CustomOption = "updated value"
    assert productionClient.prodDescription["Step1_Child"]["customParameters"] == {"CustomOption": "custom value"}


def test_productionStepRejectsTransformationParametersAsCustomParameters():
    productionStep = ProductionStep()

    with pytest.raises(AttributeError, match="Status is a transformation parameter"):
        productionStep.Status = "Active"

    assert "Status" not in productionStep.customParameters


def test_addProductionStepOmitsEmptyCustomParameters(productionClientAndRPC):
    productionClient, rpcClient = productionClientAndRPC
    rpcClient.addProductionStep.return_value = S_OK(12)
    productionStep = ProductionStep()
    productionStep.Name = "Input"

    result = productionClient.addProductionStep(productionStep)

    assert result["OK"]
    assert productionClient.prodDescription == {"Step1_Input": {"stepID": 12, "parentStep": []}}


def test_addProductionStepReturnsStepValidationError(productionClientAndRPC):
    productionClient, rpcClient = productionClientAndRPC
    productionStep = ProductionStep()
    productionStep.Name = "Input"
    productionStep.ParentStep = "invalid parent"

    result = productionClient.addProductionStep(productionStep)

    assert not result["OK"]
    assert result["Message"] == "Invalid Parent Step"
    rpcClient.addProductionStep.assert_not_called()
    assert productionClient.prodDescription == {}


def test_addProductionStepReturnsRPCError(productionClientAndRPC):
    productionClient, rpcClient = productionClientAndRPC
    rpcClient.addProductionStep.return_value = S_ERROR("failed to add step")
    productionStep = ProductionStep()
    productionStep.Name = "Input"

    result = productionClient.addProductionStep(productionStep)

    assert not result["OK"]
    assert result["Message"] == "failed to add step"
    assert productionClient.prodDescription == {}


def test_getProductionsFetchesAllPages(productionClientAndRPC):
    productionClient, rpcClient = productionClientAndRPC
    firstPage = [{"ProductionID": 1}, {"ProductionID": 2}]
    secondPage = [{"ProductionID": 3}]
    rpcClient.getProductions.side_effect = [S_OK(firstPage), S_OK(secondPage)]

    result = productionClient.getProductions(limit=2)

    assert result == S_OK(firstPage + secondPage)
    assert rpcClient.getProductions.call_args_list[0].args == ({}, None, None, "CreationDate", None, 2, 0)
    assert rpcClient.getProductions.call_args_list[1].args == ({}, None, None, "CreationDate", None, 2, 2)
