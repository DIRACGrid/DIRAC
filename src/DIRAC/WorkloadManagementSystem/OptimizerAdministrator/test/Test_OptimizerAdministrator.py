"""Tests for the module OptimizerAdministrator"""

# pylint: disable=missing-function-docstring,invalid-name
from mock import MagicMock

from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.WorkloadManagementSystem.Optimizer.InputDataResolver import InputDataResolver
from DIRAC.WorkloadManagementSystem.Optimizer.SanityChecker import SanityChecker
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.OptimizerAdministrator import OptimizerAdministrator


def test_optimize_normal_case():
    # Arrange
    optimizers = [MagicMock(), MagicMock(), MagicMock()]
    for optimizer in optimizers:
        optimizer.optimize = MagicMock(return_value=S_OK())

    optimizerAdministrator = OptimizerAdministrator(MagicMock())
    optimizerAdministrator.getOptimizers = MagicMock(return_value=optimizers)

    # Act
    result = optimizerAdministrator.optimize()

    # Assert
    assert result["OK"] is True
    for optimizer in optimizers:
        optimizer.optimize.assert_called_once()


def test_optimize_error_case():
    # Arrange
    optimizers = [MagicMock(), MagicMock(), MagicMock()]
    optimizers[0].optimize = MagicMock(return_value=S_OK())
    optimizers[1].optimize = MagicMock(return_value=S_ERROR())
    optimizers[2].optimize = MagicMock(return_value=S_OK())

    optimizerAdministrator = OptimizerAdministrator(MagicMock())
    optimizerAdministrator.getOptimizers = MagicMock(return_value=optimizers)

    # Act
    result = optimizerAdministrator.optimize()

    # Assert
    assert result["OK"] is False
    optimizers[0].optimize.assert_called_once()
    optimizers[1].optimize.assert_called_once()
    optimizers[2].optimize.assert_not_called()


def test_optimize_empty_list_case():
    # Arrange
    optimizerAdministrator = OptimizerAdministrator(MagicMock())
    optimizerAdministrator.getOptimizers = MagicMock(return_value=[])

    # Act
    result = optimizerAdministrator.optimize()

    # Assert
    assert result["OK"] is True


def test_optimize_error_while_setting_status():
    # Arrange
    jobState = MagicMock()
    jobState.setStatus = MagicMock(return_value=S_ERROR())

    optimizerAdministrator = OptimizerAdministrator(jobState)
    optimizerAdministrator.getOptimizers = MagicMock(return_value=[])

    # Act
    result = optimizerAdministrator.optimize()

    # Assert
    assert result["OK"] is False


def test_getOptimizers_normal_case():
    # Arrange
    optimizerAdministrator = OptimizerAdministrator(MagicMock())
    optimizerAdministrator.getJobPath = MagicMock(
        return_value=[SanityChecker.__name__, InputDataResolver.__name__, StagerHander.__name__]
    )

    # Act
    optimizers = optimizerAdministrator.getOptimizers()

    # Assert
    assert optimizers[0].__class__ == SanityChecker
    assert optimizers[1].__class__ == InputDataResolver
    assert optimizers[2].__class__ == StagerHander


def test_getOptimizers_bad_name_case():
    # Arrange
    optimizerAdministrator = OptimizerAdministrator(MagicMock())
    optimizerAdministrator.getJobPath = MagicMock(return_value=["JobSanitye", "InputData"])

    # Act
    optimizers = optimizerAdministrator.getOptimizers()

    # Assert
    assert optimizers[0].__class__ == InputDataResolver


def test_getJobPath_normal_case():
    # Arrange
    jobManifest = MagicMock()
    jobManifest.isOption = MagicMock(return_value=True)
    jobManifest.getOption = MagicMock(return_value="JobSanity,JobScheduling")

    jobState = MagicMock()
    jobState.getManifest = MagicMock(return_value=S_OK(jobManifest))

    # Act
    jobPath = OptimizerAdministrator(jobState).getJobPath()

    # Assert
    assert jobPath == ["JobSanity", "JobScheduling"]


def test_getJobPath_no_jobPath_option():
    # Arrange
    jobManifest = MagicMock()
    jobManifest.isOption = MagicMock(return_value=False)

    jobState = MagicMock()
    jobState.getManifest = MagicMock(return_value=S_OK(jobManifest))

    # Act
    jobPath = OptimizerAdministrator(jobState).getJobPath()

    # Assert
    assert jobPath == ["JobSanity", "InputData", "JobScheduling"]


def test_getJobPath_no_manifest():
    # Arrange
    jobState = MagicMock()
    jobState.getManifest = MagicMock(return_value=S_ERROR())

    # Act
    jobPath = OptimizerAdministrator(jobState).getJobPath()

    # Assert
    assert jobPath == ["JobSanity", "InputData", "JobScheduling"]
