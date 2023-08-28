""" pytest for TransformationClient
"""
# pylint: disable=protected-access,missing-docstring,invalid-name

import pytest

# sut
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


tc = TransformationClient()


@pytest.mark.parametrize(
    "tsFiles, dictOfNewLFNsStatus, force, expected",
    [
        ({}, {}, False, {}),
        ({}, {"foo": ["status", 2, 1234]}, False, {}),
        ({"foo": ["status", 2, 1234]}, {"foo": "status"}, False, {}),
        ({"foo": ["status", 2, 1234]}, {"foo": "statusA"}, False, {"foo": "statusA"}),
        ({"foo": ["status", 2, 1234], "bar": ["status", 2, 5678]}, {"foo": "status"}, False, {}),
        ({"foo": ["status", 2, 1234], "bar": ["status", 2, 5678]}, {"foo": "statusA"}, False, {"foo": "statusA"}),
        (
            {"foo": ["status", 2, 1234], "bar": ["status", 2, 5678]},
            {"foo": "A", "bar": "B"},
            False,
            {"foo": "A", "bar": "B"},
        ),
        ({"foo": ["status", 2, 1234]}, {"foo": "A", "bar": "B"}, False, {"foo": "A"}),
        ({"foo": ["Assigned", 2, 1234]}, {"foo": "A", "bar": "B"}, False, {"foo": "A"}),
        (
            {"foo": ["Assigned", 2, 1234], "bar": ["Assigned", 2, 5678]},
            {"foo": "Assigned", "bar": "Processed"},
            False,
            {"bar": "Processed"},
        ),
        (
            {"foo": ["Processed", 2, 1234], "bar": ["Unused", 2, 5678]},
            {"foo": "Assigned", "bar": "Processed"},
            False,
            {"bar": "Processed"},
        ),
        (
            {"foo": ["Processed", 2, 1234], "bar": ["Unused", 2, 5678]},
            {"foo": "Assigned", "bar": "Processed"},
            True,
            {"foo": "Assigned", "bar": "Processed"},
        ),
        (
            {"foo": ["MaxReset", 12, 1234], "bar": ["Processed", 22, 5678]},
            {"foo": "Unused", "bar": "Unused"},
            False,
            {},
        ),
        (
            {"foo": ["MaxReset", 12, 1234], "bar": ["Processed", 22, 5678]},
            {"foo": "Unused", "bar": "Unused"},
            True,
            {"foo": "Unused", "bar": "Unused"},
        ),
        (
            {"foo": ["Assigned", 20, 1234], "bar": ["Processed", 2, 5678]},
            {"foo": "Unused", "bar": "Unused"},
            False,
            {"foo": "MaxReset"},
        ),
    ],
)
def test__applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, force, expected):
    res = tc._applyTransformationFilesStateMachine(tsFiles, dictOfNewLFNsStatus, force)
    assert res == expected
