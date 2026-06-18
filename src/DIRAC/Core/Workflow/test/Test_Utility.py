"""Regression tests for workflow Utility.substitute.

When a non-string workflow parameter (e.g. a list) contains ``@{var}`` references,
``substitute`` rebuilds the object by evaluating the substituted repr() string.
Such values can legitimately exceed the ``saferEval`` default length cap.

Regression: replacing ``eval`` with ``saferEval`` introduced a hard 2048-byte
limit, so substituting into a large non-string parameter failed with
``ValueError: Object string is too long (>2048 bytes)``.
"""

from DIRAC.Core.Workflow.Utility import substitute


def test_substitute_large_non_string_value():
    # A non-string (list) parameter whose repr() exceeds the 2048-byte default cap.
    param = [f"@{{PREFIX}}_{i:04d}" for i in range(200)]
    assert len(str(param)) > 2048

    result = substitute(param, "PREFIX", "LFN:/lhcb/data/2026")

    assert result == [f"LFN:/lhcb/data/2026_{i:04d}" for i in range(200)]
