""" pytest of Core.Utilities.Dictionaries
"""
import pytest

# sut
from DIRAC.Core.Utilities.Dictionaries import breakDictionaryIntoChunks


@pytest.mark.parametrize(
    "aDict, chunkSize, expectedSizes",
    [
        ({}, 1, []),
        ({"a": "aa", "b": "bb", "c": "cc", "d": "dd"}, 5, [4]),
        ({"a": "aa", "b": "bb", "c": "cc", "d": "dd"}, 2, [2, 2]),
        ({"a": "aa", "b": "bb", "c": "cc", "d": "dd"}, 1, [1, 1, 1, 1]),
        ({"a": "aa", "b": "bb", "c": "cc", "d": "dd"}, 3, [3, 1]),
    ],
)
def test_breakDictionaryIntoChunks_normal(aDict, chunkSize, expectedSizes):
    """breakDictIntoChunks tests"""
    result = list(breakDictionaryIntoChunks(aDict, chunkSize))
    assert list(map(len, result)) == expectedSizes
    mergedResult = {}
    for partialDict in result:
        mergedResult.update(partialDict)
    assert aDict == mergedResult
