# pylint: disable=missing-docstring, invalid-name

from mock import MagicMock

from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK
from DIRAC.WorkloadManagementSystem.CheckerAdminstrator.CheckerAdmisintrator import CheckerAdministrator


def test_check_normal_case():
    # Arrange
    jobState = MagicMock()
    jobState.setStatus = MagicMock(return_value=S_OK())

    checkers = [MagicMock(), MagicMock(), MagicMock()]
    for checker in checkers:
        checker.check = MagicMock(return_value=S_OK())

    checkerAdministrator = CheckerAdministrator(jobState)
    checkerAdministrator.getCheckers = MagicMock(return_value=checkers)

    # Act
    result = checkerAdministrator.check()

    # Assert
    assert result["OK"] is True
    for checker in checkers:
        checker.check.assert_called_once()


def test_check_empty_list():
    # Arrange
    jobState = MagicMock()
    jobState.setStatus = MagicMock(return_value=S_OK())

    checkerAdministrator = CheckerAdministrator(jobState)
    checkerAdministrator.getCheckers = MagicMock(return_value=[])

    # Act
    result = checkerAdministrator.check()

    # Assert
    assert result["OK"] is True


def test_check_error_while_checking():
    # Arrange
    jobState = MagicMock()
    jobState.setStatus = MagicMock(return_value=S_OK())

    checkers = [MagicMock(), MagicMock(), MagicMock()]
    checkers[0].check = MagicMock(return_value=S_OK())
    checkers[1].check = MagicMock(return_value=S_ERROR())
    checkers[2].check = MagicMock(return_value=S_OK())

    checkerAdministrator = CheckerAdministrator(jobState)
    checkerAdministrator.getCheckers = MagicMock(return_value=checkers)

    # Act
    result = checkerAdministrator.check()

    # Assert
    assert result["OK"] is True
    checkers[0].check.assert_called_once()
    checkers[1].check.assert_called_once()
    checkers[2].check.assert_not_called()


def test_check_error_while_setting_status():
    # Arrange
    jobState = MagicMock()
    jobState.setStatus = MagicMock(return_value=S_ERROR())

    checkers = [MagicMock(), MagicMock(), MagicMock()]
    for checker in checkers:
        checker.check = MagicMock(return_value=S_OK())

    checkerAdministrator = CheckerAdministrator(jobState)
    checkerAdministrator.getCheckers = MagicMock(return_value=checkers)

    # Act
    result = checkerAdministrator.check()

    # Assert
    assert result["OK"] is False
