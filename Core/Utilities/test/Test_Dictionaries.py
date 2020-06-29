""" pytest of Core.Utilities.Dictionaries
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest

# sut
from DIRAC.Core.Utilities.Dictionaries import breakDictionaryIntoChunks


@pytest.mark.parametrize("aDict, chunkSize, expected", [
    ({}, 1, []),
    ({'a': 'aa', 'b': 'bb', 'c': 'cc', 'd': 'dd'}, 5, [{'a': 'aa', 'b': 'bb', 'c': 'cc', 'd': 'dd'}]),
    ({'a': 'aa', 'b': 'bb', 'c': 'cc', 'd': 'dd'}, 2, [{'a': 'aa', 'c': 'cc'}, {'b': 'bb', 'd': 'dd'}]),
    ({'a': 'aa', 'b': 'bb', 'c': 'cc', 'd': 'dd'}, 1, [{'a': 'aa'}, {'c': 'cc'}, {'b': 'bb'}, {'d': 'dd'}]),
    ({'a': 'aa', 'b': 'bb', 'c': 'cc', 'd': 'dd'}, 3, [{'a': 'aa', 'b': 'bb', 'c': 'cc'}, {'d': 'dd'}]),
])
def test_breakDictionaryIntoChunks_normal(aDict, chunkSize, expected):
  """ breakDictIntoChunks tests"""
  assert list(breakDictionaryIntoChunks(aDict, chunkSize)) == expected
